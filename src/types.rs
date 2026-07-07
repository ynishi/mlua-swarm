//! Fundamental types: Role / Verb / RoleVerbGate / CapToken / IDs.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ─── ID newtypes ───────────────────────────────────────────────────────────

/// Error returned when an ID string does not carry the expected prefix.
///
/// Produced by the fallible constructors on the ID newtypes
/// ([`StepId::parse`], [`TaskId::parse`], ...) and by their serde
/// `Deserialize` impls (which route through `TryFrom<String>`), so a
/// misrouted or malformed id fails at the boundary instead of deep inside
/// a store lookup.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} id `{got}`: expected `{expected}` prefix")]
pub struct IdParseError {
    /// Human-readable ID kind (`"step"`, `"task"`, ...).
    pub kind: &'static str,
    /// The prefix this ID kind requires, e.g. `ST-`.
    pub expected: &'static str,
    /// The rejected input.
    pub got: String,
}

/// Defines a prefix-validated ID newtype.
///
/// The generated type keeps its inner `String` **private**: the only ways
/// to obtain a value are `new()` (mint) and `parse()` / `TryFrom<String>` /
/// `FromStr` (validated) — which is what makes the prefix check impossible
/// to bypass at call sites. Serde deserialization routes through
/// `TryFrom<String>` (`#[serde(try_from = "String")]`), and serialization
/// stays the plain inner string, so the wire format is byte-for-byte
/// unchanged from the `pub String` era.
macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident, $prefix:literal, $kind:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String")]
        pub struct $name(String);

        impl $name {
            /// The prefix this ID kind carries on the wire.
            pub const PREFIX: &'static str = $prefix;

            /// Mint a fresh id with the kind prefix and a process-unique
            /// nonce (see [`uid_hex`]).
            pub fn new() -> Self {
                Self(format!(concat!($prefix, "{}"), uid_hex(8)))
            }

            /// Parse an externally-supplied string, rejecting values that
            /// do not start with the kind prefix or carry nothing after it.
            pub fn parse(s: impl Into<String>) -> Result<Self, IdParseError> {
                let s = s.into();
                if s.len() > $prefix.len() && s.starts_with($prefix) {
                    Ok(Self(s))
                } else {
                    Err(IdParseError {
                        kind: $kind,
                        expected: $prefix,
                        got: s,
                    })
                }
            }

            /// View the id as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the id and return the inner string.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdParseError;
            fn try_from(s: String) -> Result<Self, IdParseError> {
                Self::parse(s)
            }
        }

        impl std::str::FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, IdParseError> {
                Self::parse(s)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }
    };
}

id_newtype!(
    /// Opaque per-step identifier, e.g. `ST-<hex>`. Newtype over `String` so
    /// step, session, and worker ids can't be swapped by accident at call
    /// sites.
    ///
    /// One `StepId` is minted per dispatched Blueprint step (the engine's
    /// dispatcher "spins up a fresh task per `Step.ref`"). It is scoped to a
    /// single step execution — the whole-kick identity is [`RunId`], and the
    /// work-item identity is [`TaskId`].
    ///
    /// Renamed from `TaskId` (`T-` prefix) in the issue #13 ID-hierarchy
    /// reconciliation: Blueprint → Task → Run → Step → Attempt.
    StepId, "ST-", "step"
);

id_newtype!(
    /// Opaque work-item identifier, e.g. `T-<hex>`. One `TaskId` names one
    /// unit of work ("resolve issue #10" + a Blueprint ref + input ctx),
    /// persisted in the task store. A task can be kicked N times; each kick
    /// is a [`RunId`].
    ///
    /// Not to be confused with [`StepId`] (the per-step id that carried the
    /// `TaskId` name before issue #13).
    TaskId, "T-", "task"
);

id_newtype!(
    /// Opaque run identifier, e.g. `R-<hex>`. One `RunId` names one kick of a
    /// [`TaskId`] — minted server-side when a task is started, propagated
    /// through the engine ctx to every wire frame so steps, workers, and
    /// outputs correlate back to the run.
    ///
    /// The `R-` prefix is reserved for run ids; the engine's resume keys
    /// moved to `RK-` in issue #14 so the two can't shadow each other under
    /// prefix validation.
    RunId, "R-", "run"
);

id_newtype!(
    /// Opaque session identifier, e.g. `S-<hex>`. See [`StepId`] for the
    /// newtype rationale.
    ///
    /// This is the one session-id shape across the system (issue #11): the
    /// engine mints it for attached operator sessions, and the server's
    /// `POST /v1/operators` login path mints the WS operator `sid` in the
    /// same shape (the old `op-<uuid>` sid form is retired). A `SessionId`
    /// is an identifier, not a credential — bearer secrets use `secure_hex`
    /// tokens.
    SessionId, "S-", "session"
);

id_newtype!(
    /// Opaque worker identifier, e.g. `W-<hex>`. See [`StepId`] for the
    /// newtype rationale.
    WorkerId, "W-", "worker"
);

// ─── Role × Verb ───────────────────────────────────────────────────────────

/// The four participant roles in the swarm. Every [`Verb`] a caller wants to
/// invoke must be allow-listed for its role in a [`RoleVerbGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Drives task lifecycle: starts tasks, dispatches attempts, reads
    /// state, manages sessions.
    Operator,
    /// Executes a dispatched attempt: fetches its prompt/data, posts a
    /// result, verifies its own token.
    Worker,
    /// Read-only: subscribes to events and reads trace/state without
    /// mutating anything.
    Observer,
    /// Human/oversight role: answers queries, overrides verdicts, and can
    /// pause/resume the loop or inject a directive.
    Senior,
}

/// Every action a participant can request. Grouped by the [`Role`] that
/// typically performs it (see the `// operator` / `// worker` / ... section
/// comments below); the grouping is documentation only — actual
/// authorization is decided by [`RoleVerbGate::is_allowed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verb {
    // operator
    /// Create a new task.
    StartTask,
    /// Dispatch (or re-dispatch) an attempt for a task.
    DispatchAttempt,
    /// Mint a [`CapToken`] for a worker.
    MintWorkerToken,
    /// Read the current state of a task.
    ReadTaskState,
    /// Cancel a task.
    CancelTask,
    /// Ask a [`Role::Senior`] a question about a task.
    QuerySenior,
    /// Mark a task/attempt as passed.
    MarkPass,
    /// Mark a task/attempt as blocked.
    MarkBlocked,
    /// Attach a session to a task.
    AttachSession,
    /// Detach a session from a task.
    DetachSession,
    /// Emit a liveness heartbeat.
    Heartbeat,
    /// Poll for task progress/completion.
    PollTask,
    // worker
    /// Fetch the rendered prompt for the current attempt.
    FetchPrompt,
    /// Fetch task input data.
    FetchData,
    /// Post the result of an attempt.
    PostResult,
    /// Verify a presented [`CapToken`].
    VerifyToken,
    /// Emit intermediate output for observers.
    EmitOutput,
    // observer
    /// Subscribe to the task's event stream.
    SubscribeEvents,
    /// Read the accumulated trace of a task.
    ReadTrace,
    // senior
    /// Answer a query raised via [`Verb::QuerySenior`].
    AnswerQuery,
    /// Override a previously recorded verdict.
    OverrideVerdict,
    /// Pause the dispatch loop.
    PauseLoop,
    /// Resume a paused dispatch loop.
    ResumeLoop,
    /// Inject a directive into a running task.
    InjectDirective,
}

/// Role × Verb gate table. Const-style storage.
#[derive(Debug, Clone)]
pub struct RoleVerbGate {
    table: HashMap<Role, HashSet<Verb>>,
}

impl RoleVerbGate {
    /// Build an empty gate (nothing allowed until [`Self::allow`] is called).
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    /// Allow-list `verbs` for `role`, merging with any existing entries.
    /// Returns `self` for chained construction (see
    /// [`default_role_verb_table`]).
    pub fn allow(mut self, role: Role, verbs: &[Verb]) -> Self {
        let set = self.table.entry(role).or_default();
        for v in verbs {
            set.insert(*v);
        }
        self
    }

    /// Whether `role` is allow-listed to invoke `verb`.
    pub fn is_allowed(&self, role: Role, verb: Verb) -> bool {
        self.table
            .get(&role)
            .map(|s| s.contains(&verb))
            .unwrap_or(false)
    }
}

impl Default for RoleVerbGate {
    fn default() -> Self {
        default_role_verb_table()
    }
}

// ─── Verb tables (const slices, swap-out points for future Role splits) ──

/// Verbs an Operator may invoke — covers task lifecycle, session, and
/// senior interactions.
pub const OPERATOR_VERBS: &[Verb] = &[
    Verb::StartTask,
    Verb::DispatchAttempt,
    Verb::MintWorkerToken,
    Verb::ReadTaskState,
    Verb::CancelTask,
    Verb::QuerySenior,
    Verb::MarkPass,
    Verb::MarkBlocked,
    Verb::AttachSession,
    Verb::DetachSession,
    Verb::Heartbeat,
    Verb::PollTask,
];

/// The Worker verbs shared across all workers — the minimum a leaf
/// needs, with no sub-task spawning. If we introduce
/// `Role::WorkerLeaf` in the future, that role gets allowed against
/// this slice.
pub const WORKER_LEAF_VERBS: &[Verb] = &[
    Verb::FetchPrompt,
    Verb::FetchData,
    Verb::PostResult,
    Verb::VerifyToken,
    Verb::EmitOutput,
];

/// Worker verbs for recursive swarming: sub-task spawn and
/// observation. When `Role::WorkerSwarm` splits out in the future,
/// that role gets allowed against `WORKER_LEAF_VERBS` plus this
/// slice. The safety valves are `EngineCfg.max_spawn_depth` today,
/// and a task-ownership gate down the line.
pub const WORKER_SWARM_VERBS: &[Verb] = &[
    Verb::StartTask,
    Verb::DispatchAttempt,
    Verb::ReadTaskState,
    Verb::PollTask,
    Verb::CancelTask,
];

/// Verbs an Observer may invoke — strictly read-only (event subscription
/// and trace/state reads, no mutation).
pub const OBSERVER_VERBS: &[Verb] = &[Verb::SubscribeEvents, Verb::ReadTrace, Verb::ReadTaskState];

/// Verbs a Senior may invoke — human/oversight actions: answering
/// queries, overriding verdicts, and pausing/resuming/injecting into the
/// dispatch loop.
pub const SENIOR_VERBS: &[Verb] = &[
    Verb::AnswerQuery,
    Verb::OverrideVerdict,
    Verb::PauseLoop,
    Verb::ResumeLoop,
    Verb::InjectDirective,
];

/// The default Role × Verb table.
///
/// Today `Role::Worker` holds both leaf and swarm capabilities. When
/// we split it into `WorkerLeaf` / `WorkerSwarm` in the future, the
/// only change needed is swapping the `allow(Role::Worker, ...)` line
/// here for two lines — the verb slices themselves stay `const` and
/// get reused as-is.
pub fn default_role_verb_table() -> RoleVerbGate {
    RoleVerbGate::new()
        .allow(Role::Operator, OPERATOR_VERBS)
        .allow(Role::Worker, WORKER_LEAF_VERBS)
        .allow(Role::Worker, WORKER_SWARM_VERBS)
        .allow(Role::Observer, OBSERVER_VERBS)
        .allow(Role::Senior, SENIOR_VERBS)
}

// ─── CapToken ──────────────────────────────────────────────────────────────

/// Capability token. `max_uses` picks between OneTime / Session /
/// Limited.
///
/// The `uses_left` counter is **server-side, on `EngineState`**: the
/// token stays immutable, and the record holds the counter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapToken {
    /// Identifier of the agent this token was minted for.
    pub agent_id: String,
    /// The [`Role`] the bearer is authorized to act as.
    pub role: Role,
    /// Free-form scope strings (interpretation is caller-defined; `"*"`
    /// conventionally means unrestricted).
    pub scopes: Vec<String>,
    /// Unix timestamp (seconds) when the token was minted.
    pub issued_at: u64,
    /// Unix timestamp (seconds) after which the token is expired.
    pub expire_at: u64,
    /// Remaining-use budget: `None` = unlimited (session token), `Some(n)`
    /// = at most `n` uses (one-time when `n == 1`).
    pub max_uses: Option<u32>,
    /// Random per-mint value — **secret material** (it rides inside
    /// `encode()` and the `MSE_TOKEN_NONCE` env). The server-side lookup
    /// key is [`CapToken::fingerprint`] (its SHA-256), never the nonce
    /// itself (issue #14).
    pub nonce: String,
    /// Hex-encoded HMAC-SHA256 signature over [`CapToken::signing_input`].
    pub sig_hex: String,
}

impl CapToken {
    /// Server-side lookup key for this token: hex SHA-256 of the `nonce`.
    ///
    /// The nonce is the token's secret material, so the server never uses
    /// it directly as a map key or prints it in diagnostics — the
    /// fingerprint is the loggable identity (issue #14; the sibling
    /// pattern is the operator login flow's sid / token split). Replaces
    /// the former `id()` accessor, which returned the raw nonce.
    pub fn fingerprint(&self) -> String {
        token_fingerprint(&self.nonce)
    }

    /// Input for the HMAC signature — the concatenation of every field
    /// except `sig` itself.
    pub fn signing_input(&self) -> Vec<u8> {
        let s = format!(
            "{}|{:?}|{}|{}|{}|{:?}|{}",
            self.agent_id,
            self.role,
            self.scopes.join(","),
            self.issued_at,
            self.expire_at,
            self.max_uses,
            self.nonce,
        );
        s.into_bytes()
    }

    /// Whether `now_unix` is at or past [`CapToken::expire_at`].
    pub fn is_expired(&self, now_unix: u64) -> bool {
        now_unix >= self.expire_at
    }

    /// Transport-safe string encoding — URL-safe base64 of the
    /// `serde_json` representation. Used when SubAgents put the token
    /// on the HTTP path via `Authorization: Bearer <encode()>`. The
    /// HMAC signature covers every field, so the server verifies with
    /// `verify_sig` after decoding.
    pub fn encode(&self) -> String {
        use base64::Engine as _;
        let json = serde_json::to_vec(self).expect("CapToken is always JSON-serializable");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    /// The inverse of `encode()`: base64 decode followed by JSON
    /// parse. Either failure returns `CapTokenDecodeError` — this is
    /// the input-validation step when the server receives a `Bearer`
    /// token.
    pub fn decode(s: &str) -> Result<Self, CapTokenDecodeError> {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| CapTokenDecodeError::Base64(e.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|e| CapTokenDecodeError::Json(e.to_string()))
    }
}

/// Response body for `HTTP /v1/worker/prompt` — the shape that lets a
/// SubAgent pull its task input in a single round-trip.
///
/// - `system`: the rendered `AgentDef.profile.system_prompt` (`None`
///   when the profile is absent).
/// - `prompt`: `TaskSpec.initial_directive` rendered to `String` at
///   this boundary (issue #18). The engine stores
///   `initial_directive` as `Value` end-to-end
///   (`EngineState.prompts` / `Engine::fetch_prompt`); the coercion
///   to `String` (strings verbatim, anything else serde-stringified)
///   happens here in `Engine::fetch_worker_payload*` because the
///   `/v1/worker/prompt` HTTP wire format is a plain string.
/// - `agent`: `TaskSpec.agent` — the agent name this dispatch is
///   targeting.
/// - `attempt`: the 1-based attempt number, matching the current
///   `task.attempt`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerPayload {
    /// The task this payload was fetched for. Typed [`StepId`] since issue
    /// #14 — serde keeps the wire shape a plain string.
    pub task_id: StepId,
    /// 1-based attempt number, matching the current `task.attempt`.
    pub attempt: u32,
    /// Name of the agent this dispatch is targeting.
    pub agent: String,
    /// Rendered system prompt, if the agent profile defines one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// The task's initial directive, baked in at dispatch preparation.
    pub prompt: String,
}

/// Error returned when `CapToken::decode` fails.
#[derive(Debug, thiserror::Error)]
pub enum CapTokenDecodeError {
    /// The input was not valid URL-safe base64.
    #[error("base64 decode failed: {0}")]
    Base64(String),
    /// The decoded bytes were not valid `CapToken` JSON.
    #[error("json parse failed: {0}")]
    Json(String),
}

/// Server-side machinery for minting and verifying tokens.
#[derive(Debug, Clone)]
pub struct TokenSigner {
    secret: Vec<u8>,
}

impl TokenSigner {
    /// Build a signer from a raw HMAC secret (any length; HMAC accepts it).
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        Self {
            secret: secret.as_ref().to_vec(),
        }
    }

    /// Mint and sign a [`CapToken`] with an explicit `max_uses` policy.
    /// Prefer [`Self::one_time`] / [`Self::session`] / [`Self::limited`]
    /// for the common cases.
    pub fn mint(
        &self,
        agent_id: impl Into<String>,
        role: Role,
        scopes: Vec<String>,
        ttl: Duration,
        max_uses: Option<u32>,
    ) -> CapToken {
        let now = now_unix();
        let mut token = CapToken {
            agent_id: agent_id.into(),
            role,
            scopes,
            issued_at: now,
            expire_at: now + ttl.as_secs(),
            max_uses,
            nonce: secure_hex(16),
            sig_hex: String::new(),
        };
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).expect("HMAC accepts any key length");
        mac.update(&token.signing_input());
        let sig = mac.finalize().into_bytes();
        token.sig_hex = hex::encode(sig);
        token
    }

    /// HMAC sig verify (constant-time eq for timing side-channel resistance).
    pub fn verify_sig(&self, token: &CapToken) -> bool {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).expect("HMAC accepts any key length");
        mac.update(&token.signing_input());
        let expected = mac.finalize().into_bytes();
        let Ok(provided) = hex::decode(&token.sig_hex) else {
            return false;
        };
        ct_eq(&expected, &provided)
    }

    /// Builder convenience: one-time token.
    pub fn one_time(
        &self,
        agent_id: impl Into<String>,
        role: Role,
        scopes: Vec<String>,
        ttl: Duration,
    ) -> CapToken {
        self.mint(agent_id, role, scopes, ttl, Some(1))
    }

    /// Builder convenience: session token (unlimited uses until expire).
    pub fn session(
        &self,
        agent_id: impl Into<String>,
        role: Role,
        scopes: Vec<String>,
        ttl: Duration,
    ) -> CapToken {
        self.mint(agent_id, role, scopes, ttl, None)
    }

    /// Builder convenience: limited (N uses).
    pub fn limited(
        &self,
        agent_id: impl Into<String>,
        role: Role,
        scopes: Vec<String>,
        ttl: Duration,
        max_uses: u32,
    ) -> CapToken {
        self.mint(agent_id, role, scopes, ttl, Some(max_uses))
    }
}

// ─── helpers ───────────────────────────────────────────────────────────────

pub(crate) fn now_unix() -> u64 {
    // A clock reporting before the epoch means the host clock is broken in a
    // way that would otherwise silently mint `issued_at: 0` / `expire_at: 0`
    // tokens (indistinguishable from "already expired" *and* from "minted at
    // the epoch") — fail loud instead of laundering that into a bogus
    // timestamp.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_secs()
}

/// In-process-unique, restart-decorrelated hex id.
///
/// Combines a monotonic per-process counter (bijective — guarantees no two
/// calls in the same process ever collide) with a random per-process salt
/// drawn once from the OS RNG (decorrelates ids across restarts, so a
/// long-lived id from a previous process run can't be mistaken for one
/// minted by the current process). The high bits of the 128-bit XOR are
/// dominated by the salt (a process fingerprint); the low bits change on
/// every call.
///
/// **Not unguessable.** The counter is a public, low-entropy sequence once
/// the salt leaks (e.g. via any single id from this process) — never use
/// this for bearer credentials, signing nonces, or anything else that must
/// resist an adversary who can observe some ids and guess others. Use
/// [`secure_hex`] for that.
pub fn uid_hex(bytes: usize) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static SALT: OnceLock<u128> = OnceLock::new();
    let salt = *SALT.get_or_init(|| {
        let mut b = [0u8; 16];
        getrandom::fill(&mut b).expect("OS RNG unavailable");
        u128::from_le_bytes(b)
    });
    let c = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    // XOR keeps the counter's in-process uniqueness (bijection) while the
    // per-process random salt decorrelates restarts. High 64 bits are pure
    // salt (a process fingerprint); low bits change every call.
    let v = salt ^ c;
    let raw = format!("{:032x}", v);
    let n = (bytes * 2).min(32);
    raw[32 - n..].to_string()
}

/// OS-RNG hex, safe for bearer credentials.
///
/// Every byte comes from the OS random source (`getrandom`) on every call —
/// unpredictable across calls *and* across process restarts, unlike
/// [`uid_hex`]. Use this whenever the value itself is the secret: the
/// [`CapToken`] nonce (its server-side lookup key and part of the signed
/// material) and worker/session bearer handles.
pub fn secure_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    getrandom::fill(&mut buf).expect("OS RNG unavailable");
    hex::encode(buf)
}

/// Hex SHA-256 of a token nonce / bearer string — the lookup-key shape
/// used by [`CapToken::fingerprint`]. Standalone so callers holding only
/// the bearer string (not a decoded token) can derive the same key.
pub fn token_fingerprint(nonce: &str) -> String {
    use sha2::Digest as _;
    hex::encode(Sha256::digest(nonce.as_bytes()))
}

/// Constant-time byte-slice equality (XOR accumulate). Public so bearer
/// comparisons outside this module (e.g. the operator login token check)
/// can avoid the timing side channel of `==`.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod id_newtype_tests {
    use super::*;

    #[test]
    fn parse_accepts_prefixed_ids() {
        assert_eq!(StepId::parse("ST-abc123").unwrap().as_str(), "ST-abc123");
        assert_eq!(TaskId::parse("T-abc123").unwrap().as_str(), "T-abc123");
        assert_eq!(RunId::parse("R-abc123").unwrap().as_str(), "R-abc123");
        assert_eq!(SessionId::parse("S-abc123").unwrap().as_str(), "S-abc123");
        assert_eq!(WorkerId::parse("W-abc123").unwrap().as_str(), "W-abc123");
    }

    #[test]
    fn parse_rejects_wrong_prefix_and_empty_suffix() {
        // Wrong prefix (including another kind's prefix).
        assert!(StepId::parse("T-abc").is_err());
        assert!(TaskId::parse("ST-abc").is_err());
        assert!(
            RunId::parse("RK-abc").is_err(),
            "resume keys are not run ids"
        );
        assert!(SessionId::parse("W-abc").is_err());
        assert!(WorkerId::parse("S-abc").is_err());
        // Case-sensitive.
        assert!(StepId::parse("st-abc").is_err());
        // Prefix alone (nothing after it).
        assert!(TaskId::parse("T-").is_err());
        // Garbage / empty.
        assert!(RunId::parse("nope").is_err());
        assert!(WorkerId::parse("").is_err());
    }

    #[test]
    fn parse_error_carries_kind_prefix_and_input() {
        let err = TaskId::parse("R-xyz").unwrap_err();
        assert_eq!(err.kind, "task");
        assert_eq!(err.expected, "T-");
        assert_eq!(err.got, "R-xyz");
        assert_eq!(
            err.to_string(),
            "invalid task id `R-xyz`: expected `T-` prefix"
        );
    }

    #[test]
    fn minted_ids_round_trip_through_parse() {
        assert!(StepId::parse(StepId::new().into_string()).is_ok());
        assert!(TaskId::parse(TaskId::new().into_string()).is_ok());
        assert!(RunId::parse(RunId::new().into_string()).is_ok());
        assert!(SessionId::parse(SessionId::new().into_string()).is_ok());
        assert!(WorkerId::parse(WorkerId::new().into_string()).is_ok());
    }

    #[test]
    fn serde_wire_format_is_a_plain_string() {
        let id = TaskId::parse("T-abc").unwrap();
        assert_eq!(
            serde_json::to_value(&id).unwrap(),
            serde_json::json!("T-abc")
        );
        let back: TaskId = serde_json::from_value(serde_json::json!("T-abc")).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn serde_deserialize_validates_prefix() {
        let err = serde_json::from_value::<TaskId>(serde_json::json!("ST-abc"));
        assert!(err.is_err(), "deserialize must route through parse");
    }
}

#[cfg(test)]
mod cap_token_fingerprint_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn fingerprint_is_sha256_of_nonce_and_not_the_nonce() {
        let signer = TokenSigner::new("test-secret");
        let token = signer.session("a", Role::Worker, vec!["*".into()], Duration::from_secs(60));
        let fp = token.fingerprint();
        // 32-byte SHA-256, hex-encoded.
        assert_eq!(fp.len(), 64);
        // The lookup key must never equal (or contain) the secret nonce.
        assert_ne!(fp, token.nonce);
        assert!(!fp.contains(&token.nonce));
        // Standalone helper derives the same key from the bare bearer string.
        assert_eq!(fp, token_fingerprint(&token.nonce));
        // Deterministic per token, distinct across mints.
        assert_eq!(fp, token.fingerprint());
        let other = signer.session("a", Role::Worker, vec!["*".into()], Duration::from_secs(60));
        assert_ne!(fp, other.fingerprint());
    }
}

#[cfg(test)]
mod cap_token_transport_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn encode_decode_round_trips() {
        let signer = TokenSigner::new("test-secret");
        let token = signer.session(
            "worker-of-task-x",
            Role::Worker,
            vec!["*".into()],
            Duration::from_secs(600),
        );
        let s = token.encode();
        // URL-safe base64 should not contain `+` `/` `=`
        assert!(!s.contains('+'));
        assert!(!s.contains('/'));
        assert!(!s.contains('='));

        let decoded = CapToken::decode(&s).expect("decode ok");
        assert_eq!(decoded, token);
        assert!(
            signer.verify_sig(&decoded),
            "HMAC sig still verifies after round-trip"
        );
    }

    #[test]
    fn decode_rejects_garbage() {
        let err = CapToken::decode("not-base64!!!").expect_err("should fail");
        assert!(matches!(err, CapTokenDecodeError::Base64(_)));
    }

    #[test]
    fn decode_rejects_non_token_json() {
        use base64::Engine as _;
        let bogus = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"oops\":1}");
        let err = CapToken::decode(&bogus).expect_err("should fail json shape");
        assert!(matches!(err, CapTokenDecodeError::Json(_)));
    }
}
