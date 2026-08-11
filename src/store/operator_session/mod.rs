//! `OperatorSessionStore` — persistence for Operator login-flow sessions.
//!
//! One row per minted `POST /v1/operators` session: the sid / bearer token /
//! capability manifest / mint time / 記名. This is the record that
//! lets a single-server restart keep every logged-in Operator logged in —
//! the sibling stores (task / run / replay / trace / …) already persist,
//! and `RunRecord.operator_sid` persists a *pointer* into this session
//! space, so leaving the sessions themselves process-volatile stranded
//! every restored run pin on a `404 unknown sid` after restart.
//!
//! Deliberately **not** persisted: the WS adapter state (`tx` sender,
//! `pending` oneshot map). Both are process-lifetime objects with no
//! meaningful serialized form — an empty rebuild on the client's next WS
//! connect (the existing reconnect path) is the correct restoration.
//!
//! Current scope:
//!
//! - [`InMemoryOperatorSessionStore`] — process-volatile default.
//! - [`SqliteOperatorSessionStore`] — file-backed persistence via
//!   `rusqlite-isle` (same shape as [`crate::store::task::SqliteTaskStore`]).

use crate::types::SessionId;
use crate::AgentProviderManifest;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

/// How many [`ObservedAssignment`] entries one session retains.
///
/// **D2** says the observed part is appended to on every `Assign` and has
/// no delete path; it does not say the row grows forever. A session that
/// re-acquires in a loop would otherwise put an unbounded column behind
/// every `GET /v1/operators`, so the log is a ring: over the cap, the
/// **oldest** entry goes. Nothing a reader has is deleted by an API — the
/// oldest fact simply ages out, and
/// [`OperatorSessionRecord::observed_total`] stays monotone so the reader
/// can see that it did.
///
/// The value is set for the thing the log is read for: telling apart the
/// handful of Runs a driver is currently juggling in one repo. 32 is well
/// past that.
///
/// # The size that count multiplies
///
/// A depth is only half of a size, and this ring is rewritten whole to
/// `observed_json` on every `Assign` and returned for up to
/// `OPERATORS_LIST_MAX_LIMIT` sessions per `GET /v1/operators` read. So
/// every field of an entry has a ceiling — [`TASK_METADATA_MAX_BYTES`] for
/// the JSON bag, [`OBSERVED_TEXT_MAX_BYTES`] for each of the three
/// caller-supplied strings — and the product is what a reader is actually
/// promised: at most `32 × (4096 + 3 × 1024)` ≈ 224 KiB of variable
/// content per session, whatever the launch put in them.
///
/// That number is the claim, and it is stated rather than asserted because
/// the alternative was the shape this used to have: a depth with a
/// reassuring adjective ("still small enough to serialize whole") in front
/// of four fields, one of which — `goal` — had no bound at all, so the
/// sentence was true only of launches that happened to be small.
pub const OBSERVED_CAP: usize = 32;

/// Serialized-size ceiling for a recorded [`ObservedAssignment::task_metadata`].
///
/// `task_metadata` is an arbitrary caller-supplied JSON bag, so it is the
/// one observed field with no natural size. Above this it is dropped and
/// [`ObservedAssignment::task_metadata_omitted`] says so — an omission the
/// reader can see beats a session row that inherits someone's payload
/// 32 times over.
pub const TASK_METADATA_MAX_BYTES: usize = 4096;

/// Byte ceiling for each of the three caller-supplied strings on an
/// [`ObservedAssignment`] — `goal`, `project_root` and `work_dir`.
///
/// The same rule [`TASK_METADATA_MAX_BYTES`] applies to the fourth
/// caller-supplied field, differing in what it does when the ceiling is
/// hit: these three are **cut, not dropped**. A JSON bag half-carried is
/// not a JSON bag, but a goal's opening clause and a path's leading
/// components are exactly what the observed part is read for — telling two
/// of a driver's Runs apart — so keeping the prefix keeps the field doing
/// its job. [`ObservedAssignment::text_truncated`] says a cut happened, and
/// the value itself ends in `…`, so a reader is never handed a shortened
/// path that reads like a whole one.
///
/// 1 KiB is generous for both shapes: a goal is a sentence, and a path that
/// long is already past what most filesystems will hand out.
pub const OBSERVED_TEXT_MAX_BYTES: usize = 1024;

/// How long a session may go unaccessed before it expires — the second
/// exit from `Registered` in model §4.1's state diagram
/// (`Registered ── 最終アクセスから 24h ──▶ ╳ 削除`), in seconds.
///
/// # The rule has no predicate number
///
/// It is cited that way throughout this file and the server's, and not as
/// **O1**, which is a different rule: §4.1's `O1` is `join は無認証`, and
/// §6's index confirms the `O1-O8` band is the eight numbered Operator
/// predicates. The 24h horizon appears only in the diagram above them and
/// was never given a number. Citing a number the model does not carry
/// makes every one of these doc comments unfollowable in exactly the way
/// a citation exists to prevent — the reader looks `O1` up and finds a
/// statement about authentication.
///
/// # Enforced at the reads, and on a schedule
///
/// The horizon is first of all a rule about what may be *observed*, and
/// that is where it is enforced: every path that reads a session — the
/// boot restore, the 記名 list, a single-session read, the WS upgrade —
/// drops the expired ones it finds and deletes their rows. A session past
/// the horizon is therefore never returned to anybody, which is the whole
/// of what the state diagram promises to a reader. That shape is shared
/// with the two sibling judgments: **A7** examines a seat at reference
/// time, **O8** cascades at delete time.
///
/// The reads alone leave one thing out, and it is not the row on disk. A
/// teardown also unregisters the session from the engine and the adapter
/// registry, so until it happens a *dispatch* aimed at the dead sid still
/// resolves and parks — and a dispatch is not a read, so nothing about it
/// applies the horizon. "It goes on its own after 24 hours" would
/// therefore hold only on a server somebody happens to be listing. So the
/// server also runs the same judgment on a schedule, as the
/// `operator-session-expiry` job on its periodic-job runner
/// (`mlua_swarm_server::periodic`), which calls the same read-path
/// predicate through the same teardown.
///
/// That is not a reversal of `31fefc1`, which removed a periodic
/// stale-`Run` sweeper. What was wrong there was the *predicate* — "a
/// `Running` row nobody has written to for 3900s has lost its driver" was
/// stated nowhere else and was false of every healthy run it could reach.
/// This horizon is stated by the model, applied by four other call sites,
/// and executed by the teardown a `DELETE` performs; the job contributes
/// the schedule and nothing else. `periodic`'s module doc carries that
/// rule for anything else that wants to be scheduled.
pub const OPERATOR_SESSION_MAX_IDLE_SECS: u64 = 24 * 60 * 60;

pub mod inmemory;
pub mod sqlite;
pub use inmemory::InMemoryOperatorSessionStore;
pub use sqlite::SqliteOperatorSessionStore;

// ──────────────────────────────────────────────────────────────────────────
// OperatorSessionRecord
// ──────────────────────────────────────────────────────────────────────────

/// One persisted Operator login-flow session.
///
/// Field-for-field the durable subset of the server's `LoginSession` —
/// everything except the process-lifetime WS adapter state, which is
/// rebuilt empty on reconnect.
///
/// # The bearer token is never stored
///
/// [`token_digest`](Self::token_digest) holds
/// `hex(SHA-256(bearer))` — the same fingerprint shape the `/v1/sessions`
/// path already keys its store by, for the same reason ("the sid handed to
/// the client is the token nonce itself (a bearer secret), so the server
/// never uses it as a map key"; see `mse_server::SessionStore`). Every
/// consumer of this record only ever *compares* a presented bearer
/// ([`verify_bearer`](Self::verify_bearer)), so nothing downstream needs
/// the plaintext — it exists only inside `POST /v1/operators`, between
/// minting and the mint response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorSessionRecord {
    /// Server-minted session id (`S-<hex>`).
    pub sid: SessionId,
    /// `hex(SHA-256(bearer))` of the auth token required on the WS upgrade
    /// and admin routes. Derive with [`Self::digest_of`]; compare with
    /// [`Self::verify_bearer`]. The plaintext bearer is deliberately absent
    /// (see the type doc).
    pub token_digest: String,
    /// Provider-owned effective capability manifest submitted at join.
    pub capability_manifest: Option<AgentProviderManifest>,
    /// Unix epoch seconds when `POST /v1/operators` minted this session.
    pub joined_at_secs: u64,
    /// Unix epoch seconds when this session was last **accessed** — model
    /// §4.1's `最終アクセス`, the clock the 24h expiry
    /// ([`OPERATOR_SESSION_MAX_IDLE_SECS`]) runs from.
    ///
    /// # Access, not activity
    ///
    /// [`Self::last_activity_secs`] answers "when was this session last
    /// *assigned* something", which is what **D5** sorts the 記名 list by.
    /// This answers "when did the driver behind this session last show
    /// itself", which is a wider set of events: attaching a WebSocket,
    /// reading its own session, being assigned a seat. A driver can be very
    /// much alive and hold no seat for a day, so expiring on activity would
    /// reap live sessions.
    ///
    /// # Why this one is stored and its sibling is derived
    ///
    /// `last_activity_secs` is a maximum over the observed ring, so it
    /// cannot go stale — every value it reads from is already persisted.
    /// An access leaves no such trace: nothing about a WS connect or a
    /// `GET /v1/operators/:sid` is written down anywhere else, so if this
    /// were derived there would be nothing to derive it from. It is
    /// advanced by [`Self::touch`] and written through by the server.
    ///
    /// Additive with `#[serde(default)]`. A row persisted before this field
    /// existed decodes as `0`, which would read as "accessed at the epoch"
    /// and expire it on sight — so every reader goes through
    /// [`Self::last_access_secs`], which folds `0` back onto the join time.
    #[serde(default)]
    pub last_access_secs: u64,
    /// The **confirmed part** of this session's 記名 (model §4.2, **D1**):
    /// roughly 50 characters the joining AI wrote about what it is working
    /// on, fixed at join and never rewritten afterwards.
    ///
    /// It is what the observed part cannot supply. Two drivers in the same
    /// worktree produce the same `project_root` / `work_dir` and can hold
    /// Runs of the same Blueprint; the sentence one of them wrote at join
    /// exists only in that conversation, which is what makes it an
    /// identifier (§4.2: 観測部分だけでは足りない).
    ///
    /// `None` = the session joined without one. Kept as an absence rather
    /// than an empty string so a reader can tell "nothing was written" from
    /// "something was written and it was blank" — `POST /v1/operators` does
    /// not reject a missing `desc` (unlike **A9** on the assignment side,
    /// **D1-D5** name no `400`), so the absence is a real and readable
    /// state.
    ///
    /// **D4**: nothing matches on this. It is read by humans and AIs to
    /// tell sessions apart, never by the server to decide identity.
    ///
    /// Additive with `#[serde(default)]` — rows persisted before the 記名
    /// existed decode as `None`.
    #[serde(default)]
    pub desc: Option<String>,
    /// The **observed part** of this session's 記名 (model §4.2, **D2**):
    /// one entry per seat this session was assigned, appended by the server
    /// at each `Assign` and never removed by any API.
    ///
    /// Oldest first. Bounded by [`OBSERVED_CAP`] and de-duplicated per
    /// `(run_id, slot)` — see [`Self::record_observed`].
    ///
    /// Additive with `#[serde(default)]`.
    #[serde(default)]
    pub observed: Vec<ObservedAssignment>,
    /// How many `Assign`s have been recorded onto [`Self::observed`] over
    /// this session's life, including the ones the ring has since dropped
    /// and the re-assignments folded into an existing entry.
    ///
    /// Monotone. `observed_total > observed.len()` is the visible signal
    /// that the reader is looking at a window rather than the whole
    /// history.
    ///
    /// Additive with `#[serde(default)]`.
    #[serde(default)]
    pub observed_total: u64,
}

/// One `Assign` as the assigned Operator session observed it — the
/// per-entry shape of the 記名's observed part (model §4.2's second row:
/// 担当した Run と goal / `project_root` / `work_dir` / `task_metadata` /
/// 最終活動時刻).
///
/// # Every field is what the server could actually read
///
/// The three path-ish fields come from the Task row's `task_input_spec`
/// (the persisted `TaskInputSpec` the launch was given, which is also what
/// `TaskInputMiddleware` is later built from), and `goal` from the same
/// row. A launch that carried no Task-level input leaves all three `None`,
/// and nothing is substituted for them: an invented `project_root` would
/// be read as a fact about where the work is happening.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservedAssignment {
    /// The Run whose seat was taken.
    pub run_id: String,
    /// Which Blueprint-declared Operator seat (`Run.current`'s key).
    pub slot: String,
    /// The owning Task's human-facing goal, when the Task row could be
    /// read. `None` = the read failed; it is not a claim that the Task has
    /// no goal (the field is not optional on the Task row).
    ///
    /// Cut to [`OBSERVED_TEXT_MAX_BYTES`] — see [`Self::text_truncated`].
    #[serde(default)]
    pub goal: Option<String>,
    /// Task-level project root, from the launch's `TaskInputSpec`. Cut to
    /// [`OBSERVED_TEXT_MAX_BYTES`] — see [`Self::text_truncated`].
    #[serde(default)]
    pub project_root: Option<String>,
    /// Task-level working directory, from the same spec. Cut to
    /// [`OBSERVED_TEXT_MAX_BYTES`] — see [`Self::text_truncated`].
    #[serde(default)]
    pub work_dir: Option<String>,
    /// Task-level metadata bag, from the same spec. Dropped when it
    /// serializes above [`TASK_METADATA_MAX_BYTES`] — see
    /// [`Self::task_metadata_omitted`].
    #[serde(default)]
    pub task_metadata: Option<serde_json::Value>,
    /// `true` when [`Self::task_metadata`] was present but too large to
    /// carry, so a reader does not read the `null` as "the launch supplied
    /// none".
    #[serde(default)]
    pub task_metadata_omitted: bool,
    /// `true` when at least one of [`Self::goal`] / [`Self::project_root`] /
    /// [`Self::work_dir`] was longer than [`OBSERVED_TEXT_MAX_BYTES`] and
    /// was cut to fit.
    ///
    /// One flag for the three because it answers the one question a cut
    /// raises — "is what I am reading the whole value?" — and the cut
    /// values name themselves: each ends in `…`. A flag per field would
    /// only restate that.
    ///
    /// Additive with `#[serde(default)]`: rows written before the ceiling
    /// existed decode as `false`, which is what they were — nothing had
    /// been cut.
    #[serde(default)]
    pub text_truncated: bool,
    /// Unix epoch seconds of the `Assign` this entry records — the session's
    /// last activity when this is its newest entry
    /// ([`OperatorSessionRecord::last_activity_secs`]).
    pub at_secs: u64,
}

impl ObservedAssignment {
    /// Build an entry, applying the [`TASK_METADATA_MAX_BYTES`] bound to
    /// `task_metadata` and the [`OBSERVED_TEXT_MAX_BYTES`] bound to each of
    /// the three caller-supplied strings.
    ///
    /// This is the only constructor callers use, which is what makes the
    /// bounds a property of the type rather than a rule a call site has to
    /// remember: every entry that reaches the ring came through here.
    ///
    /// A metadata value that will not even serialize is treated as an
    /// oversized one (dropped, flagged) rather than as absent — the failure
    /// is about carrying it, not about having it.
    pub fn new(
        run_id: String,
        slot: String,
        goal: Option<String>,
        project_root: Option<String>,
        work_dir: Option<String>,
        task_metadata: Option<serde_json::Value>,
        at_secs: u64,
    ) -> Self {
        let (task_metadata, task_metadata_omitted) = match task_metadata {
            None => (None, false),
            Some(value) => match serde_json::to_string(&value) {
                Ok(text) if text.len() <= TASK_METADATA_MAX_BYTES => (Some(value), false),
                _ => (None, true),
            },
        };
        let mut text_truncated = false;
        let goal = cap_text(goal, &mut text_truncated);
        let project_root = cap_text(project_root, &mut text_truncated);
        let work_dir = cap_text(work_dir, &mut text_truncated);
        Self {
            run_id,
            slot,
            goal,
            project_root,
            work_dir,
            task_metadata,
            task_metadata_omitted,
            text_truncated,
            at_secs,
        }
    }
}

/// Cut `value` to the longest prefix that fits in
/// [`OBSERVED_TEXT_MAX_BYTES`] and mark `truncated`, or hand it back
/// untouched when it already fits.
///
/// The cut lands on a `char` boundary — a byte-sliced `String` would not
/// be one — and the result carries a trailing `…` so the shortening is
/// visible in the value and not only in the flag. That marker is why the
/// output can exceed the ceiling by its own 3 bytes: the bound is on what
/// a caller can put in, not on the notation this adds.
fn cap_text(value: Option<String>, truncated: &mut bool) -> Option<String> {
    let text = value?;
    if text.len() <= OBSERVED_TEXT_MAX_BYTES {
        return Some(text);
    }
    let mut end = OBSERVED_TEXT_MAX_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    *truncated = true;
    let mut cut = String::with_capacity(end + '…'.len_utf8());
    cut.push_str(&text[..end]);
    cut.push('…');
    Some(cut)
}

impl OperatorSessionRecord {
    /// Append one `Assign` to the observed part (**D2**).
    ///
    /// Two shaping rules, both about keeping the log readable rather than
    /// about deleting anything:
    ///
    /// - **One entry per `(run_id, slot)`.** Re-acquiring a seat this
    ///   session already holds is the same fact with a newer timestamp, so
    ///   the existing entry is replaced and moved to the newest position
    ///   instead of accumulating a row per acquire. A driver that
    ///   re-acquires after every reconnect would otherwise fill the whole
    ///   window with one Run.
    /// - **Newest [`OBSERVED_CAP`] kept.** Past the cap the oldest entry is
    ///   dropped.
    ///
    /// [`Self::observed_total`] counts every call regardless, so a reader
    /// can tell that folding or dropping happened.
    pub fn record_observed(&mut self, entry: ObservedAssignment) {
        self.observed_total = self.observed_total.saturating_add(1);
        if let Some(pos) = self
            .observed
            .iter()
            .position(|e| e.run_id == entry.run_id && e.slot == entry.slot)
        {
            self.observed.remove(pos);
        }
        self.observed.push(entry);
        while self.observed.len() > OBSERVED_CAP {
            self.observed.remove(0);
        }
    }

    /// When this session was last seen doing something — the newest
    /// [`ObservedAssignment::at_secs`], or [`Self::joined_at_secs`] for a
    /// session that has never been assigned anything.
    ///
    /// **D5**'s default ordering key. Derived rather than stored: a
    /// separate column would be a second thing to keep in step with the
    /// log, and the ring only ever drops entries older than the newest one,
    /// so the derivation cannot go stale.
    pub fn last_activity_secs(&self) -> u64 {
        self.observed
            .iter()
            .map(|e| e.at_secs)
            .max()
            .unwrap_or(0)
            .max(self.joined_at_secs)
    }

    /// When this session was last accessed, for the 24h expiry clock.
    ///
    /// Reads [`Self::last_access_secs`], with two foldings that make the
    /// value safe to compare against a horizon:
    ///
    /// - a `0` (a row persisted before the field existed, or a session
    ///   never touched since it was minted) reads as the **join time**, so
    ///   a fresh session is never a day old on arrival;
    /// - an assignment counts as an access even if nothing touched the
    ///   field, so [`Self::last_activity_secs`] is folded in as well. A
    ///   session being handed seats is being used, whatever else it does.
    pub fn last_access_secs(&self) -> u64 {
        self.last_access_secs.max(self.last_activity_secs())
    }

    /// Advance [`Self::last_access_secs`] to `now`, never backwards.
    ///
    /// Monotone because the clock is not: a `SystemTime` that steps back
    /// (NTP correction, a suspended laptop) must not make a session look
    /// older than the last time something saw it. Returns whether the value
    /// moved, so a caller can skip a durable write that would change
    /// nothing.
    pub fn touch(&mut self, now: u64) -> bool {
        if now <= self.last_access_secs {
            return false;
        }
        self.last_access_secs = now;
        true
    }

    /// The 24h horizon: has this session gone
    /// [`OPERATOR_SESSION_MAX_IDLE_SECS`] without being accessed, as of
    /// `now`?
    ///
    /// A pure predicate over the record. What it *cannot* see is whether a
    /// socket is attached right now, which is why the server's expiry
    /// checks pair it with a connectivity read — a driver holding an idle
    /// WebSocket open is present, and reaping it would be the reaper
    /// causing the outage it exists to prevent. See
    /// `mse_server::operator_ws::login`'s expiry note.
    pub fn is_expired_at(&self, now: u64) -> bool {
        now.saturating_sub(self.last_access_secs()) >= OPERATOR_SESSION_MAX_IDLE_SECS
    }

    /// Digest a plaintext bearer into the at-rest shape
    /// ([`Self::token_digest`]).
    ///
    /// Callers mint a bearer with
    /// [`operator_bearer_token`](crate::types::operator_bearer_token) and
    /// keep the plaintext only long enough to answer the mint request.
    pub fn digest_of(bearer: &str) -> String {
        crate::types::token_fingerprint(bearer)
    }

    /// Constant-time check of a presented bearer against
    /// [`Self::token_digest`].
    ///
    /// The comparison runs over the two digests (fixed-width hex), so it
    /// carries no timing signal about the bearer itself.
    pub fn verify_bearer(&self, bearer: &str) -> bool {
        crate::types::ct_eq(
            self.token_digest.as_bytes(),
            Self::digest_of(bearer).as_bytes(),
        )
    }
}

/// Errors surfaced by an [`OperatorSessionStore`] implementation.
#[derive(Debug, Error)]
pub enum OperatorSessionStoreError {
    /// No session exists for the given sid.
    #[error("operator session not found: {0}")]
    NotFound(SessionId),

    /// Backend-specific failure not covered by the other variants.
    #[error("other: {0}")]
    Other(String),
}

// ──────────────────────────────────────────────────────────────────────────
// OperatorSessionStore trait
// ──────────────────────────────────────────────────────────────────────────

/// Persistence interface for Operator login-flow sessions.
///
/// Write-through contract on the server side: `POST /v1/operators` calls
/// [`put`](Self::put) before answering the mint, teardown (`DELETE
/// /v1/operators/:sid`) calls [`delete`](Self::delete), and a fresh boot
/// calls [`list`](Self::list) once to rehydrate its in-memory session map.
#[async_trait]
pub trait OperatorSessionStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Insert or replace the row for `record.sid`. Upsert semantics: sids
    /// are freshly minted so a same-sid overwrite only happens on a
    /// deliberate re-put of the same session.
    async fn put(&self, record: OperatorSessionRecord) -> Result<(), OperatorSessionStoreError>;

    /// Delete the row for `sid`. `NotFound` when no such row exists.
    async fn delete(&self, sid: &SessionId) -> Result<(), OperatorSessionStoreError>;

    /// The row stored under `sid`, **exactly as stored** — `Ok(None)` when
    /// there is none.
    ///
    /// # This one does not apply the horizon, and that is the point
    ///
    /// [`list`](Self::list) both filters and deletes, which leaves it
    /// unable to answer the question its own contract is written around:
    /// *was the expired row deleted, or merely withheld?* Both produce the
    /// same `list`. Three assertions elsewhere claimed to check the
    /// deletion and read it through `list`, so all three would have passed
    /// on a filter-only backend — the load-bearing half of the contract
    /// ("Filtering without deleting would hide them from the reader while
    /// leaving the file growing") was untestable through the trait,
    /// because the trait exposed no unfiltered read.
    ///
    /// This is that read. It reports the backing store's contents and
    /// applies no judgment of its own, so a caller can tell a deleted row
    /// from a hidden one.
    ///
    /// # It is not a session-resolution path
    ///
    /// Nothing in the server resolves a live session through here: a
    /// running process answers about sessions out of its in-memory map,
    /// and the durable rows are read exactly once, at boot, by `list`.
    /// Handing an expired row back is therefore not a way to revive one —
    /// the row goes to a test or a diagnostic, both of which want the
    /// truth about the file rather than the truth about who may be served.
    async fn get(
        &self,
        sid: &SessionId,
    ) -> Result<Option<OperatorSessionRecord>, OperatorSessionStoreError>;

    /// List the sessions this store can decode **and that have not
    /// expired**, ascending by `joined_at_secs` (mint order, stable for
    /// deterministic rehydration).
    ///
    /// # Contract: an expired row is dropped *and deleted*
    ///
    /// A row whose last access is [`OPERATOR_SESSION_MAX_IDLE_SECS`] or
    /// more in the past is model §4.1's second exit from `Registered`:
    /// `Registered ── 最終アクセスから 24h ──▶ ╳ 削除` (unnumbered — see
    /// [`OPERATOR_SESSION_MAX_IDLE_SECS`]). Implementations must omit it from
    /// the returned vector and remove it from the backing store, reporting
    /// each removal with a `tracing::info!`.
    ///
    /// A `list` that deletes is unusual enough to say why it is here rather
    /// than in a reaper. The sole caller is boot-time rehydration, which is
    /// also the only moment a persisted session is read from disk at all —
    /// so this is where an expired row would otherwise be resurrected, once
    /// per restart, forever (the row's own driver crashed and lost the
    /// bearer `DELETE /v1/operators/:sid` wants, so nothing else can ever
    /// remove it). Filtering without deleting would hide them from the
    /// reader while leaving the file growing.
    ///
    /// The running server sweeps expired sessions on a schedule as well
    /// (see [`OPERATOR_SESSION_MAX_IDLE_SECS`]), but that job walks the
    /// live session map — which, at this moment, is the empty one this
    /// call is about to fill. Boot is the one point where a row exists and
    /// no session does, so this contract is the sweep's counterpart across
    /// a restart, not a duplicate of it.
    ///
    /// Deleting is safe precisely because the row is expired: no live
    /// process holds it (it was not in memory — this call is what would
    /// have put it there), and nothing else refers to it. A `Run.current`
    /// naming it is repaired by an `acquire` (**A8**), the same repair a
    /// crashed driver's seat already needs.
    ///
    /// # Contract: per row, not all-or-nothing
    ///
    /// A backend that decodes at-rest bytes back into
    /// [`OperatorSessionRecord`] **must not** let one undecodable row fail
    /// the whole call. Such a row is skipped and reported with a
    /// `tracing::warn!` naming the row and the field that failed; the
    /// intact rows are still returned. An `Err` from this method therefore
    /// means the *backend* failed (the file is unreadable, the connection
    /// is gone) — never that one stored session went bad.
    ///
    /// This matters because the sole caller is boot-time rehydration, and
    /// its own error path is fatal: an `Err` here takes `mse serve` down
    /// and every healthy session with it. Undecodable rows are reachable
    /// in practice — an older build could persist shapes a newer one
    /// rejects (`sid: "op-<uuid>"` predates the `S-<hex>` shape) — so
    /// all-or-nothing decoding means one stale row bricks the boot.
    ///
    /// Skipping the row rather than defaulting the field is deliberate: a
    /// session restored minus a field it was minted with would come back
    /// claiming something other than what it is, and would fail later,
    /// elsewhere, and quietly. Dropping it is the observable choice.
    ///
    /// # Backends that never decode
    ///
    /// [`InMemoryOperatorSessionStore`] holds live
    /// [`OperatorSessionRecord`]s, so no row of its can be undecodable and
    /// it never skips anything. That is consistent with the contract, not
    /// an exemption from it: "the sessions this store can decode" is every
    /// session it holds.
    async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError>;
}

/// The wall clock the expiry horizon is measured against.
///
/// A clock that cannot answer yields `0`, which makes `now.saturating_sub`
/// zero for every record and expires nothing. That is the right way to
/// fail: an unreadable clock is not evidence that a session is stale, and
/// this is a deleting path.
pub(crate) fn expiry_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Split a freshly read set of records into the ones a caller may see and
/// the sids the 24h horizon has expired, logging one line per expiry.
///
/// Shared by both backends so the horizon, the predicate and the wording
/// are decided once — a backend that drifted on any of the three would
/// give the same server two different session lifetimes depending on how
/// it was configured.
pub(crate) fn partition_expired(
    records: Vec<OperatorSessionRecord>,
    now: u64,
    backend: &str,
) -> (Vec<OperatorSessionRecord>, Vec<SessionId>) {
    let mut live = Vec::with_capacity(records.len());
    let mut expired = Vec::new();
    for record in records {
        if record.is_expired_at(now) {
            tracing::info!(
                sid = %record.sid,
                backend,
                last_access_secs = record.last_access_secs(),
                idle_secs = now.saturating_sub(record.last_access_secs()),
                desc = record.desc.as_deref().unwrap_or("<none>"),
                "operator session expired (24h since last access); dropping the row \
                 instead of restoring it"
            );
            expired.push(record.sid);
        } else {
            live.push(record);
        }
    }
    (live, expired)
}

// ──────────────────────────────────────────────────────────────────────────
// Shared inner state used by the InMemory backend.
// ──────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct Inner {
    /// Insertion order — used as a stable tie-break under `list()`.
    pub(crate) order: Vec<SessionId>,
    pub(crate) records: HashMap<SessionId, OperatorSessionRecord>,
}

pub(crate) type SharedInner = Mutex<Inner>;

// ──────────────────────────────────────────────────────────────────────────
// tests — the 記名 shaping rules (D1 / D2 / D5)
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod record_tests {
    use super::*;
    use serde_json::json;

    fn record() -> OperatorSessionRecord {
        OperatorSessionRecord {
            sid: SessionId::parse("S-1").expect("a well-formed sid"),
            token_digest: OperatorSessionRecord::digest_of("bearer"),
            capability_manifest: None,
            joined_at_secs: 100,
            last_access_secs: 100,
            desc: None,
            observed: Vec::new(),
            observed_total: 0,
        }
    }

    fn entry(run: &str, slot: &str, at_secs: u64) -> ObservedAssignment {
        ObservedAssignment::new(
            run.to_string(),
            slot.to_string(),
            Some("resolve issue #10".to_string()),
            Some("/repo".to_string()),
            Some("/repo/.worktrees/topic".to_string()),
            Some(json!({"issue": 10})),
            at_secs,
        )
    }

    /// Re-taking a seat this session already holds refreshes the one entry
    /// instead of adding a second, and moves it to the newest position.
    #[test]
    fn re_assigning_the_same_seat_folds_into_one_entry() {
        let mut r = record();
        r.record_observed(entry("R-a", "phase-a-op", 110));
        r.record_observed(entry("R-b", "phase-a-op", 120));
        r.record_observed(entry("R-a", "phase-a-op", 130));

        let seen: Vec<(&str, u64)> = r
            .observed
            .iter()
            .map(|e| (e.run_id.as_str(), e.at_secs))
            .collect();
        assert_eq!(seen, vec![("R-b", 120), ("R-a", 130)]);
        assert_eq!(
            r.observed_total, 3,
            "the fold is not a deletion: the count still says three Assigns happened"
        );
    }

    /// The same Run in a different seat is a different fact.
    #[test]
    fn the_same_run_in_another_seat_is_its_own_entry() {
        let mut r = record();
        r.record_observed(entry("R-a", "phase-a-op", 110));
        r.record_observed(entry("R-a", "phase-b-op", 111));
        assert_eq!(r.observed.len(), 2);
    }

    /// Past the cap the oldest entry ages out; the counter keeps saying how
    /// many there really were.
    #[test]
    fn the_log_is_a_ring_bounded_by_the_cap() {
        let mut r = record();
        for i in 0..(OBSERVED_CAP + 5) {
            r.record_observed(entry(&format!("R-{i}"), "phase-a-op", 200 + i as u64));
        }
        assert_eq!(r.observed.len(), OBSERVED_CAP);
        assert_eq!(r.observed[0].run_id, "R-5", "the oldest five aged out");
        assert_eq!(r.observed_total, (OBSERVED_CAP + 5) as u64);
    }

    /// D5's ordering key: the newest activity, falling back to the join.
    #[test]
    fn last_activity_falls_back_to_the_join_time() {
        let mut r = record();
        assert_eq!(r.last_activity_secs(), 100);
        r.record_observed(entry("R-a", "phase-a-op", 140));
        assert_eq!(r.last_activity_secs(), 140);
    }

    /// An oversized metadata bag is dropped *and flagged*, so the `null` is
    /// not read as "the launch supplied none".
    #[test]
    fn oversized_task_metadata_is_dropped_and_flagged() {
        let big = json!({ "blob": "x".repeat(TASK_METADATA_MAX_BYTES) });
        let e = ObservedAssignment::new(
            "R-a".to_string(),
            "phase-a-op".to_string(),
            None,
            None,
            None,
            Some(big),
            1,
        );
        assert!(e.task_metadata.is_none());
        assert!(e.task_metadata_omitted);

        let small = ObservedAssignment::new(
            "R-a".to_string(),
            "phase-a-op".to_string(),
            None,
            None,
            None,
            None,
            1,
        );
        assert!(!small.task_metadata_omitted, "absent is not omitted");
    }

    /// The bound [`OBSERVED_CAP`]'s doc multiplies by 32 has to exist for
    /// every field, not only for the JSON bag. `goal` is the one a caller
    /// controls with no natural size, and it used to be copied verbatim.
    #[test]
    fn an_oversized_goal_is_cut_and_flagged() {
        let e = ObservedAssignment::new(
            "R-a".to_string(),
            "phase-a-op".to_string(),
            Some("g".repeat(OBSERVED_TEXT_MAX_BYTES * 4)),
            Some("/repo".to_string()),
            None,
            None,
            1,
        );
        let goal = e.goal.as_deref().expect("the prefix is kept, not dropped");
        assert!(
            goal.len() <= OBSERVED_TEXT_MAX_BYTES + '…'.len_utf8(),
            "a goal must not enter the ring longer than the ceiling (+ the marker), got {}",
            goal.len()
        );
        assert!(goal.ends_with('…'), "the cut names itself in the value");
        assert!(e.text_truncated, "and in the flag");
        assert_eq!(
            e.project_root.as_deref(),
            Some("/repo"),
            "a field that fits is untouched"
        );
    }

    /// The cut lands on a `char` boundary — a multi-byte goal must not be
    /// sliced through the middle of one.
    #[test]
    fn the_cut_lands_on_a_char_boundary() {
        // 3 bytes each, so the ceiling falls inside a character.
        let text = "あ".repeat(OBSERVED_TEXT_MAX_BYTES);
        let e = ObservedAssignment::new(
            "R-a".to_string(),
            "phase-a-op".to_string(),
            Some(text),
            None,
            None,
            None,
            1,
        );
        let goal = e.goal.as_deref().expect("kept");
        assert!(e.text_truncated);
        assert!(
            goal.trim_end_matches('…').chars().all(|c| c == 'あ'),
            "the prefix is whole characters"
        );
    }

    /// Nothing is flagged when nothing was cut — the flag is a report, not
    /// a default.
    #[test]
    fn a_short_entry_is_not_flagged() {
        let e = entry("R-a", "phase-a-op", 1);
        assert!(!e.text_truncated);
        assert_eq!(e.goal.as_deref(), Some("resolve issue #10"));
    }
}
