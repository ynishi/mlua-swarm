//! # WebSocket Operator Callback IF
//!
//! Path for seating an external HTTP/WS caller as an **Operator role** inside
//! the Engine. One WS connection = one session = three traits co-hosted
//! (`Operator` / `SeniorBridge` / `SpawnHook`); a single sid is registered into
//! all three registries simultaneously.
//!
//! ## Architecture overview
//!
//! ```text
//! ┌─────────────── External Operator (Human / Agent / other process) ────────┐
//! │                  WS /v1/operators/:sid/ws  (Bearer required)             │
//! │     S→C: Ask{req_id,task_id,question}                                   │
//! │     S→C: HookBefore{req_id,task_id,agent,attempt}                       │
//! │     S→C: HookAfter{req_id,task_id,agent,attempt,result}  (fire-and-forget)│
//! │     S→C: Spawn{req_id,task_id,agent,attempt,capability_token}          │
//! │     C→S: Answer{req_id,value}              (SeniorBridge.ask reply)     │
//! │     C→S: HookAck{req_id,ok,reason?}        (SpawnHook.before reply)     │
//! │     C→S: SpawnAck{req_id,value,ok,error?}  (Operator.execute reply)     │
//! └────────────────────────────────┬────────────────────────────────────────┘
//!                                  │ axum WebSocket
//! ┌────────────────────────────────▼────────────────────────────────────────┐
//! │ login.rs                                                                │
//! │   operators_ws_connect  — `GET /v1/operators/:sid/ws` upgrade,          │
//! │                            Bearer token check against minted sid        │
//! │   handle_operator_socket — write task / read task / disconnect path     │
//! └────────────────────────────────┬────────────────────────────────────────┘
//!                                  │
//! ┌────────────────────────────────▼────────────────────────────────────────┐
//! │ session.rs : WSOperatorSession                                          │
//! │   sid + auth_token + tx (Mutex<Option<>>) + pending (Mutex<HashMap>)    │
//! │   impl SeniorBridge { ask → send Ask + wait Answer }                    │
//! │   impl SpawnHook    { before → send HookBefore + wait HookAck /         │
//! │                       after  → send HookAfter fire-and-forget }         │
//! │   impl Operator     { execute → send Spawn + wait SpawnAck,             │
//! │                       thin-forward capability_token to MainAI }         │
//! └────────────────────────────────┬────────────────────────────────────────┘
//!                                  │ same sid → registered into 3 registries at once
//!                                  ▼
//!         engine.senior_bridges / spawn_hooks / operators (SoT)
//!                                  │ dispatch_attempt → resolve_operator_info
//!                                  │ looks up session.bridge_id / hook_id / operator_backend_id
//!                                  ▼
//!         Ctx.operator (= read by SeniorEscalationMiddleware / MainAIMiddleware /
//!                           OperatorDelegateMiddleware)
//!
//! protocol.rs : ServerMsg / ClientMsg / PendingReply (= wire format + internal reply IR)
//! ```
//!
//! ## Thin-control discipline for Spawn (the Spawn thin-control axis)
//!
//! The server sends only `Spawn{capability_token}`; the MainAI (WS Client) forwards the
//! token to the SubAgent, and the SubAgent hits `/v1/worker/prompt` +
//! `/v1/worker/result` itself with `Authorization: Bearer <capability_token>`
//! (= heavy payloads go over HTTP; WS stays purely thin control). See
//! `protocol::ServerMsg::Spawn` and `mlua_swarm::Operator::execute`
//! for details.
//!
//! ## Design rationale (= for future re-constructors)
//!
//! - **3 traits co-hosted**: Holding all 3 faces of the Operator role
//!   (judgment = `SeniorBridge` / observation = `SpawnHook` / execution =
//!   `Operator`) in a single session gives 1 WS connection = 1 Operator that
//!   answers ask/before/after/spawn — the natural shape. Registering the same
//!   sid into three registries preserves "same Operator" semantics on the
//!   Registry axis as well.
//! - **`Mutex<Option<Sender>>` for tx swap-in**: `None` on disconnect,
//!   `Some(new_tx)` on reconnect. The pending `HashMap` persists on the session
//!   side, so a client that held answer/ack values during a disconnect can
//!   reconnect and resend them. (In v1.5, sends during a disconnect fail
//!   immediately — the client is responsible for remembering its own pending.)
//! - **req_id naming**: `<sid>-<ask|hb|ha|spawn>-<uuid>` covers both the trait
//!   axis and uniqueness. Clients can identify the trait from the req_id.
//! - **`parent_req_id` field**: Schema for representing nesting (e.g. a hook
//!   firing inside an ask). In v1.5 the engine-side middleware does not fire
//!   nested calls, so this is always `None`; v2 will re-introduce nesting via
//!   `task_local`.
//!
//! ## Out of scope for v1.5 (carry)
//!
//! - Buffering / replay of ask/spawn/hook_before during a disconnect (= sends
//!   currently just return `Err` on failure).
//! - Automatic session-TTL cleanup (= session leaks after disconnect wait for
//!   the admin `DELETE` endpoint).
//! - True nested ask (= depends on a middleware extension; the `parent_req_id`
//!   schema is already carried).
//! - Multi-Blueprint scope separation (= a single WS Operator currently serves
//!   as the Operator for all tasks).
//! - `CapToken` consistency between the Operator session and the engine attach session.
//!
//! ## REST-like login flow (`login.rs`) — sole Operator session entry point
//!
//! `POST/GET/DELETE /v1/operators` + `WS /v1/operators/:sid/ws` (`login.rs`) is
//! the only Operator session route. The login flow mints the sid server-side,
//! requires Bearer auth (no empty-string default), and enforces a
//! roles-exclusivity 409 at mint time. See the `login` module doc for details.

/// REST-like Operator session resource (`POST/GET/DELETE /v1/operators` + WS upgrade).
pub mod login;
/// Wire format (`ServerMsg` / `ClientMsg`) for `WS /v1/operators/:sid/ws`.
pub mod protocol;
/// `WSOperatorSession`: the 3-trait (`SeniorBridge`/`SpawnHook`/`Operator`) WS session object.
pub mod session;

pub use login::{
    operators_create, operators_delete, operators_info, operators_ws_connect, OperatorSessionEntry,
    OperatorsCreateReq, OperatorsCreateResp, OperatorsInfoResp,
};
pub use protocol::{ClientMsg, ServerMsg};
pub use session::WSOperatorSession;
