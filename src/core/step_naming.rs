//! `StepNaming` — GH #23: the Blueprint-declared step-projection naming
//! table.
//!
//! Before this module, a dispatched Step was addressable under two
//! independent, occasionally-colliding names: the flow.ir data-plane
//! producer name (`Step.ref` / `AgentDef.name`) and the `result_ref`
//! ctx-path key (`Step.out`'s top-level path segment). Consumers
//! (`ContextPolicy.steps` filter / `StepPointer.name` / the REST
//! `:step` resolver / `FileProjectionAdapter`'s file stem) resolved the
//! union of both, data-plane winning on collision — see
//! `crates/mlua-swarm-server/src/projection.rs`'s `enumerate_steps` for
//! the pre-GH-#23 runtime union rule this table statically replaces.
//!
//! [`StepNaming`] collapses that union into a single addressing space,
//! built ONCE per Blueprint at
//! [`blueprint::compiler::Compiler::compile`](crate::blueprint::compiler::Compiler::compile)
//! time (the sole construction site — see [`StepNaming::from_blueprint`]),
//! then threaded read-only from there: `EngineDispatcher` stashes an
//! `Arc<StepNaming>` per dispatched task
//! (`EngineState.step_namings`, keyed by `StepId`), and
//! `Engine::step_naming_for` is the accessor later consumers pull from.
//!
//! GH #23 subtask-2/3 completed the 5-consumer switch-over this module's
//! table backs — `Engine::submit_output`/`materialize_final_submission`
//! (data-plane write + file stem), `ContextPolicy.allows_step`
//! (`crates/mlua-swarm-server/src/worker.rs`'s `allows_step_canonical`
//! seam), `StepPointer`/`StepSummary` assembly, and the REST `:step`
//! resolver all resolve through [`StepNaming::canonical_of_producer`] /
//! [`StepNaming::resolve`] instead of re-deriving the pre-GH-#23 union
//! rule at read time. `crate::store::output::OutputStore::get_latest_by_name_in_run`
//! (Layer 2) closed the cross-Run same-name race this table's
//! canonicalization alone could not: a declared or undeclared name is
//! now resolved Run-scoped regardless. An undeclared step's `canonical`
//! stays its raw `Step.ref` and its `aliases` still include the
//! `result_ref` top-level segment, so the pre-GH-#23 union's observable
//! behavior is unchanged for any Blueprint that never declares
//! `AgentMeta.projection_name`.

use std::collections::{BTreeMap, BTreeSet};

use mlua_flow_ir::{Expr, Node};

use crate::blueprint::Blueprint;

/// One step's resolved canonical projection name plus every alias name
/// consumers may still address it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepNameEntry {
    /// The name every consumer converges on: the `AgentMeta.projection_name`
    /// declared for the step's Blueprint agent, or (when undeclared) the
    /// flow.ir `Step.ref` (the data-plane producer name) unchanged.
    pub canonical: String,
    /// Every name this step should ALSO resolve under: always includes the
    /// `Step.ref`, plus — when the Step's `out` is a `Path` expr — that
    /// path's top-level segment (the pre-GH-#23 `result_ref`-derived
    /// name), subject to the strong/weak claim ladder in
    /// [`StepNaming::from_blueprint`]'s doc: a top segment claimed from a
    /// NESTED `out` (`$.r.a`) is a WEAK claim and is absent here whenever
    /// another step claims the same name. A bare `Step.ref` that happens
    /// to equal its own `out` top segment collapses to a single-element
    /// set; this is not a collision.
    pub aliases: BTreeSet<String>,
}

/// Non-fatal collision detected while building a [`StepNaming`] table:
/// two UNDECLARED steps' canonical/alias name sets intersect.
/// Registration still proceeds — the pre-GH-#23 union rule's
/// "data-plane wins" tie-break applies (see
/// [`StepNaming::from_blueprint`]) — but the caller is expected to
/// surface this via `tracing::warn!`. This type carries no logging side
/// effect itself, matching the crate's existing convention
/// (`blueprint::compiler`'s static-walk helpers) of returning data and
/// letting the caller decide how to report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepNamingWarning {
    /// The contested name.
    pub name: String,
    /// The step (`Step.ref`) that claimed `name` first.
    pub first_step_ref: String,
    /// The step (`Step.ref`) whose claim collided with the first.
    pub second_step_ref: String,
}

/// Fatal collision: at least one side of the clash declared `name` via
/// `AgentMeta.projection_name`. Rejected at registration time — the same
/// "Blueprint validation error" family as
/// `blueprint::compiler::CompileError`'s existing fail-fast checks
/// (`DuplicateAgent` / `UnresolvedMetaRef` / …).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "StepNaming collision: name '{name}' is claimed by both step '{first_step_ref}' and step \
     '{second_step_ref}' ({reason})"
)]
pub struct StepNamingError {
    /// The contested name.
    pub name: String,
    /// The step (`Step.ref`) that claimed `name` first.
    pub first_step_ref: String,
    /// The step (`Step.ref`) whose claim collided with the first.
    pub second_step_ref: String,
    /// Human-readable reason (which side(s) declared `projection_name`).
    pub reason: String,
}

/// GH #23 — the single addressing-space table for one Blueprint's
/// dispatched steps. See the module doc for the construction site and
/// storage/accessor threading; this doc covers the resolution rules.
///
/// # Canonical / alias resolution
///
/// For every distinct `Step.ref` appearing anywhere in the flow (`Seq` /
/// `Branch` / `Fanout` / `Loop` / `Try` nesting all walked — see
/// [`Self::from_blueprint`]):
///
/// - `canonical` = the dispatching agent's `AgentMeta.projection_name`
///   when declared, else the `Step.ref` itself (byte-identical to
///   pre-GH-#23 behavior for undeclared Blueprints).
/// - `aliases` = `{Step.ref}` ∪ every `out` Path expr's top-level segment
///   seen across every occurrence of that `ref` in the flow (`"$.plan"`
///   → `"plan"`, `"$.a.b"` → `"a"`; a non-`Path` `out` contributes
///   nothing — best-effort, mirroring `blueprint::compiler`'s existing
///   static-walk convention of skipping what can't be inspected
///   structurally), minus any WEAK claim another step contests (below).
///
/// # Strong and weak claims
///
/// Not every name a step could claim is claimed with the same strength:
///
/// - **strong claim** — the `Step.ref`, the declared `projection_name`,
///   and an `out` that is exactly `$.T` (depth 1, so the step OWNS the
///   whole `T` subtree).
/// - **weak claim** — the top segment `T` of an `out` writing UNDER it
///   (`$.T.x…`, depth ≥ 2). The step owns a lane inside `T`, not `T`.
///
/// A weak claim registers only when no other step claims that name at
/// all. The moment a second step claims it — strongly or weakly — every
/// weak claim on the name is dropped (silently, at `tracing::debug!`
/// level: a shared nesting root is ordinary Blueprint shape, not a
/// defect). The name then resolves to whichever step claims it strongly,
/// or to nothing when the contest was weak-vs-weak. That deliberate miss
/// replaces the pre-existing behavior of handing out one arbitrary lane's
/// output under the shared root's name.
///
/// Every strong claim (`canonical` + every strongly-claimed alias) is
/// checked for cross-step collisions. A clash where either side declared
/// `projection_name` is a hard [`StepNamingError`] (registration is
/// rejected outright). A clash between two undeclared steps is a soft
/// [`StepNamingWarning`]: the pre-GH-#23 union rule's "data-plane wins"
/// precedence is preserved by letting the step whose OWN `ref` equals the
/// contested name own it in [`Self::resolve`] — an alias derived merely
/// from another step's `out` segment never displaces it.
///
/// The resulting boundaries:
///
/// | Blueprint shape | result |
/// |---|---|
/// | `$.r.a` / `$.r.b` / … written by different steps (a shared nesting root) | `"r"` is nobody's alias; no warning. Each lane is addressed by its own ref / `projection_name` |
/// | two steps whose `out` is the identical `$.r` | soft warning + data-plane priority (a genuine ambiguity) |
/// | `ref: "r"` on one step, `$.r.x` on another | the weak claim yields: no warning, `resolve("r")` is the `"r"` step |
/// | `projection_name: "r"` on one step, `$.r.x` on another | the weak claim yields: compiles (no hard error) |
/// | `$.r.a` written by exactly one step | `"r"` stays that step's alias (unchanged) |
#[derive(Debug, Clone, Default)]
pub struct StepNaming {
    by_ref: BTreeMap<String, String>,
    by_name: BTreeMap<String, String>,
    entries: BTreeMap<String, StepNameEntry>,
}

impl StepNaming {
    /// Resolve `name` (canonical or alias) to its canonical name.
    pub fn resolve(&self, name: &str) -> Option<&str> {
        self.by_name.get(name).map(String::as_str)
    }

    /// Resolve a Step's data-plane producer name (`Step.ref` /
    /// `AgentDef.name`) to its canonical name.
    pub fn canonical_of_producer(&self, ref_name: &str) -> Option<&str> {
        self.by_ref.get(ref_name).map(String::as_str)
    }

    /// Every canonical name this table declares (subtask-2/3 enumeration
    /// consumers, e.g. `McpQueryAdapter::enumerate_steps`).
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// Every full [`StepNameEntry`] (canonical + aliases) this table
    /// holds.
    pub fn entries(&self) -> impl Iterator<Item = &StepNameEntry> {
        self.entries.values()
    }

    /// Build the table from a Blueprint's `flow` + `agents` — the sole
    /// construction site (see the module + struct docs). Returns the
    /// table plus any soft [`StepNamingWarning`]s (the caller decides
    /// how to log them, typically via `tracing::warn!`); a hard
    /// collision returns [`StepNamingError`] instead.
    pub fn from_blueprint(
        bp: &Blueprint,
    ) -> Result<(StepNaming, Vec<StepNamingWarning>), StepNamingError> {
        // 1. Static walk: collect every Step occurrence's
        //    (ref, out-top-segment + whether that `out` was NESTED under
        //    the segment).
        let mut occurrences: Vec<(String, Option<(String, bool)>)> = Vec::new();
        collect_steps(&bp.flow, &mut occurrences);

        // 2. Group by ref — a `Step.ref` may recur (e.g. inside a Loop
        //    body, or a flow author simply dispatching the same agent
        //    twice); the same agent always resolves to the same
        //    canonical name, so all of its occurrences fold into one
        //    entry, and every `out`-top segment seen across occurrences
        //    is unioned into its claim set. Contests are therefore only
        //    ever evaluated BETWEEN distinct refs.
        let mut order: Vec<String> = Vec::new();
        let mut out_tops: BTreeMap<String, BTreeSet<(String, bool)>> = BTreeMap::new();
        for (ref_, top) in occurrences {
            let tops = out_tops.entry(ref_.clone()).or_default();
            if let Some(top) = top {
                tops.insert(top);
            }
            if !order.contains(&ref_) {
                order.push(ref_);
            }
        }

        // 3. `AgentDef.name -> AgentMeta.projection_name` (declared-only).
        let declared: BTreeMap<&str, &str> = bp
            .agents
            .iter()
            .filter_map(|ad| {
                let name = ad.meta.as_ref()?.projection_name.as_deref()?;
                Some((ad.name.as_str(), name))
            })
            .collect();

        // 4. Split each ref's claims into the strong set (its `ref`, its
        //    canonical name, and every `out` that is exactly `$.T`) and
        //    the weak set (top segments of NESTED `out`s, `$.T.x…` — the
        //    step owns a lane inside `T`, not `T` itself). See the struct
        //    doc's "Strong and weak claims".
        let mut plans: Vec<StepClaims> = Vec::with_capacity(order.len());
        for ref_ in &order {
            let is_declared = declared.contains_key(ref_.as_str());
            let canonical = declared
                .get(ref_.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| ref_.clone());
            let mut strong_aliases: BTreeSet<String> = BTreeSet::new();
            strong_aliases.insert(ref_.clone());
            let mut weak_aliases: BTreeSet<String> = BTreeSet::new();
            for (top, nested) in out_tops.remove(ref_).unwrap_or_default() {
                if nested {
                    weak_aliases.insert(top);
                } else {
                    strong_aliases.insert(top);
                }
            }
            // A name this ref ALSO claims strongly is not weak for it.
            weak_aliases.retain(|n| n != &canonical && !strong_aliases.contains(n));
            plans.push(StepClaims {
                ref_: ref_.clone(),
                canonical,
                is_declared,
                strong_aliases,
                weak_aliases,
            });
        }

        // 5. Count claimants per name (each ref counted at most once per
        //    name) so a weak claim can tell "nobody else wants this" from
        //    "contested".
        let mut claimants: BTreeMap<&str, usize> = BTreeMap::new();
        for plan in &plans {
            for name in plan.claimed_strong().chain(plan.weak_aliases.iter()) {
                *claimants.entry(name.as_str()).or_default() += 1;
            }
        }

        let mut naming = StepNaming::default();
        let mut warnings = Vec::new();
        // name -> (owning ref, declared?) — tracks current ownership so a
        // later occurrence can detect + (for soft clashes) re-arbitrate.
        let mut claims: BTreeMap<String, (String, bool)> = BTreeMap::new();

        for plan in &plans {
            let StepClaims {
                ref_,
                canonical,
                is_declared,
                strong_aliases,
                weak_aliases,
            } = plan;
            let (is_declared, canonical) = (*is_declared, canonical.clone());

            // Weak claims survive only uncontested; a contested one is
            // dropped without a warning (a shared nesting root is
            // ordinary Blueprint shape). `debug!` keeps it observable.
            let mut aliases: BTreeSet<String> = strong_aliases.clone();
            for name in weak_aliases {
                if claimants.get(name.as_str()).copied().unwrap_or(0) > 1 {
                    tracing::debug!(
                        name = %name,
                        step_ref = %ref_,
                        "StepNaming: dropping a contested weak (nesting-root) alias claim; \
                         address this step by its ref or projection_name instead"
                    );
                    continue;
                }
                aliases.insert(name.clone());
            }

            let mut claimed: BTreeSet<String> = aliases.clone();
            claimed.insert(canonical.clone());

            for name in &claimed {
                match claims.get(name).cloned() {
                    None => {
                        claims.insert(name.clone(), (ref_.clone(), is_declared));
                        naming.by_name.insert(name.clone(), canonical.clone());
                    }
                    Some((other_ref, other_declared)) => {
                        if is_declared || other_declared {
                            return Err(StepNamingError {
                                name: name.clone(),
                                first_step_ref: other_ref,
                                second_step_ref: ref_.clone(),
                                reason: collision_reason(other_declared, is_declared),
                            });
                        }
                        warnings.push(StepNamingWarning {
                            name: name.clone(),
                            first_step_ref: other_ref.clone(),
                            second_step_ref: ref_.clone(),
                        });
                        // Soft clash between two undeclared steps: the
                        // pre-GH-#23 union rule's data-plane-first
                        // precedence — whichever step's OWN `ref` equals
                        // the contested name owns it. If neither (or
                        // both, which cannot happen since refs are
                        // unique) side's ref matches, the first-seen
                        // owner is kept (deterministic tie-break).
                        if ref_ == name && &other_ref != name {
                            claims.insert(name.clone(), (ref_.clone(), false));
                            naming.by_name.insert(name.clone(), canonical.clone());
                        }
                    }
                }
            }

            naming.by_ref.insert(ref_.clone(), canonical.clone());
            naming
                .entries
                .insert(canonical.clone(), StepNameEntry { canonical, aliases });
        }

        Ok((naming, warnings))
    }
}

/// One distinct `Step.ref`'s claim ladder, as folded from every
/// occurrence of that ref in the flow — the intermediate
/// [`StepNaming::from_blueprint`] builds before arbitrating contests (see
/// the [`StepNaming`] struct doc's "Strong and weak claims").
struct StepClaims {
    ref_: String,
    canonical: String,
    is_declared: bool,
    /// `{Step.ref}` ∪ every depth-1 `out` top segment (`$.T`).
    strong_aliases: BTreeSet<String>,
    /// Top segments of nested `out`s (`$.T.x…`), minus anything this ref
    /// already claims strongly.
    weak_aliases: BTreeSet<String>,
}

impl StepClaims {
    /// Every name this ref claims strongly, each yielded once
    /// (`canonical` frequently IS the ref, i.e. already in
    /// `strong_aliases`).
    fn claimed_strong(&self) -> impl Iterator<Item = &String> {
        self.strong_aliases.iter().chain(
            std::iter::once(&self.canonical).filter(|c| !self.strong_aliases.contains(c.as_str())),
        )
    }
}

fn collision_reason(other_declared: bool, is_declared: bool) -> String {
    match (other_declared, is_declared) {
        (true, true) => "both sides declare projection_name".to_string(),
        (true, false) => "the first step declares projection_name".to_string(),
        (false, true) => "the second step declares projection_name".to_string(),
        (false, false) => {
            unreachable!("hard StepNamingError requires at least one declared side")
        }
    }
}

/// Walk the flow `Node` (same recursion shape as
/// `blueprint::compiler::collect_refs` / `collect_step_meta_refs`) and
/// collect every `Step`'s `(ref, out-alias)`.
fn collect_steps(node: &Node, out: &mut Vec<(String, Option<(String, bool)>)>) {
    match node {
        Node::Step {
            ref_,
            out: out_expr,
            ..
        } => {
            out.push((ref_.clone(), out_alias(out_expr)));
        }
        Node::Seq { children } => {
            for child in children {
                collect_steps(child, out);
            }
        }
        Node::Branch { then_, else_, .. } => {
            collect_steps(then_, out);
            collect_steps(else_, out);
        }
        Node::Fanout { body, .. } => collect_steps(body, out),
        Node::Loop { body, .. } => collect_steps(body, out),
        Node::Try { body, catch, .. } => {
            collect_steps(body, out);
            collect_steps(catch, out);
        }
        Node::Assign { .. } => {} // The Assign node carries no ref.
    }
}

/// Extract the top-level segment of a `Step.out` `Path` expr together
/// with whether the path writes UNDER that segment rather than to it:
/// `"$.plan"` → `("plan", false)` (a strong claim on `plan`), `"$.a.b"` →
/// `("a", true)` (a weak claim on `a` — the step owns the `b` lane, not
/// `a`). Any other `Expr` shape (or an empty path) contributes no alias —
/// best-effort, mirroring `blueprint::compiler`'s existing static-walk
/// convention of skipping what can't be inspected structurally (flow.ir's
/// own `write_path` requires `Step.out` to be a `Path` expr at eval time
/// regardless, so a non-`Path` `out` is already a runtime error there —
/// this walk just never invents an alias for it statically).
fn out_alias(expr: &Expr) -> Option<(String, bool)> {
    let Expr::Path { at } = expr else {
        return None;
    };
    let rendered = at.to_string();
    let trimmed = rendered
        .strip_prefix("$.")
        .or_else(|| rendered.strip_prefix('$'))?;
    let mut segments = trimmed.split('.').filter(|s| !s.is_empty());
    let top = segments.next()?.to_string();
    Some((top, segments.next().is_some()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::{
        current_schema_version, AgentDef, AgentKind, AgentMeta, BlueprintMetadata, CompilerHints,
        CompilerStrategy,
    };
    use mlua_flow_ir::JoinMode;
    use serde_json::json;

    fn path(s: &str) -> Expr {
        Expr::Path {
            at: s.parse().expect("literal test path"),
        }
    }

    fn step(ref_: &str, out: &str) -> Node {
        Node::Step {
            ref_: ref_.to_string(),
            in_: path("$.in"),
            out: path(out),
        }
    }

    fn agent(name: &str, projection_name: Option<&str>) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": name }),
            profile: None,
            meta: Some(AgentMeta {
                projection_name: projection_name.map(str::to_string),
                ..Default::default()
            }),
            runner: None,
            runner_ref: None,
            verdict: None,
            lints: None,
        }
    }

    fn bp(flow: Node, agents: Vec<AgentDef>) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "step-naming-ut".into(),
            flow,
            agents,
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    #[test]
    fn declared_step_canonical_is_projection_name_aliases_are_ref_and_out_top() {
        let flow = step("planner", "$.plan");
        let bp = bp(flow, vec![agent("planner", Some("plan-out"))]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(warnings.is_empty());
        assert_eq!(naming.canonical_of_producer("planner"), Some("plan-out"));
        let entry = naming
            .entries()
            .find(|e| e.canonical == "plan-out")
            .expect("entry present");
        assert_eq!(
            entry.aliases,
            BTreeSet::from(["planner".to_string(), "plan".to_string()])
        );
    }

    #[test]
    fn undeclared_step_canonical_is_ref_aliases_are_ref_and_out_top() {
        let flow = step("worker", "$.result");
        let bp = bp(flow, vec![agent("worker", None)]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(warnings.is_empty());
        assert_eq!(naming.canonical_of_producer("worker"), Some("worker"));
        let entry = naming
            .entries()
            .find(|e| e.canonical == "worker")
            .expect("entry present");
        assert_eq!(
            entry.aliases,
            BTreeSet::from(["worker".to_string(), "result".to_string()])
        );
    }

    #[test]
    fn ref_equal_to_out_top_collapses_to_single_alias_and_is_not_a_collision() {
        let flow = step("scout", "$.scout");
        let bp = bp(flow, vec![agent("scout", None)]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(warnings.is_empty());
        let entry = naming
            .entries()
            .find(|e| e.canonical == "scout")
            .expect("entry present");
        assert_eq!(entry.aliases, BTreeSet::from(["scout".to_string()]));
    }

    #[test]
    fn declared_name_colliding_with_another_steps_ref_is_a_hard_error() {
        // Step "a" declares projection_name "b"; step "b" is undeclared
        // (its own ref IS "b") — the two claim the same canonical name.
        let flow = Node::Seq {
            children: vec![step("a", "$.a_out"), step("b", "$.b_out")],
        };
        let bp = bp(flow, vec![agent("a", Some("b")), agent("b", None)]);
        let err = StepNaming::from_blueprint(&bp).expect_err("declared collision must reject");
        assert_eq!(err.name, "b");
        assert!(
            err.reason.contains("declare"),
            "reason should explain which side declared: {}",
            err.reason
        );
    }

    #[test]
    fn undeclared_collision_is_ok_with_a_warning_and_data_plane_priority() {
        // Step "foo" (undeclared) has out "$.bar" — alias "bar".
        // Step "bar" (undeclared) has its own ref "bar" — canonical "bar".
        // Both claim the name "bar"; neither declares projection_name, so
        // this is a soft warning, and the data-plane owner ("bar"'s own
        // ref) must win `resolve("bar")`.
        let flow = Node::Seq {
            children: vec![step("foo", "$.bar"), step("bar", "$.baz")],
        };
        let bp = bp(flow, vec![agent("foo", None), agent("bar", None)]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("soft collision is Ok");
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].name, "bar");
        assert_eq!(naming.resolve("bar"), Some("bar"));
    }

    #[test]
    fn walk_covers_seq_branch_fanout_loop_and_try_nesting() {
        let flow = Node::Seq {
            children: vec![
                step("in-seq", "$.a"),
                Node::Branch {
                    cond: Expr::Lit { value: json!(true) },
                    then_: Box::new(step("in-then", "$.b")),
                    else_: Box::new(step("in-else", "$.c")),
                },
                Node::Fanout {
                    items: path("$.items"),
                    bind: path("$.item"),
                    body: Box::new(step("in-fanout", "$.d")),
                    join: JoinMode::All,
                    out: path("$.results"),
                },
                Node::Loop {
                    counter: path("$.n"),
                    cond: Expr::Lit { value: json!(true) },
                    body: Box::new(step("in-loop", "$.e")),
                    max: 3,
                },
                Node::Try {
                    body: Box::new(step("in-try", "$.f")),
                    catch: Box::new(step("in-catch", "$.g")),
                    err_at: None,
                },
                Node::Assign {
                    at: path("$.h"),
                    value: Expr::Lit { value: json!(1) },
                },
            ],
        };
        let agents = vec![
            "in-seq",
            "in-then",
            "in-else",
            "in-fanout",
            "in-loop",
            "in-try",
            "in-catch",
        ]
        .into_iter()
        .map(|n| agent(n, None))
        .collect();
        let bp = bp(flow, agents);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(warnings.is_empty());
        let mut names: Vec<&str> = naming.names().collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![
                "in-catch",
                "in-else",
                "in-fanout",
                "in-loop",
                "in-seq",
                "in-then",
                "in-try",
            ]
        );
    }

    #[test]
    fn resolve_returns_canonical_for_alias_lookup() {
        let flow = step("planner", "$.plan");
        let bp = bp(flow, vec![agent("planner", Some("plan-out"))]);
        let (naming, _) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert_eq!(naming.resolve("plan-out"), Some("plan-out"));
        assert_eq!(naming.resolve("planner"), Some("plan-out"));
        assert_eq!(naming.resolve("plan"), Some("plan-out"));
        assert_eq!(naming.resolve("does-not-exist"), None);
    }

    /// The shape this claim ladder exists for: several lanes writing
    /// under one shared nesting root (`$.r.a` / `$.r.b` / `$.r.c`). Every
    /// claim on `"r"` is weak and contested, so `"r"` becomes nobody's
    /// alias — no warning is emitted (this is ordinary Blueprint shape),
    /// `resolve("r")` misses explicitly instead of handing out one
    /// arbitrary lane, and each lane stays addressable by its own ref.
    #[test]
    fn shared_nesting_root_is_claimed_by_nobody_and_warns_nowhere() {
        let flow = Node::Seq {
            children: vec![
                step("lane-a", "$.r.a"),
                step("lane-b", "$.r.b"),
                step("lane-c", "$.r.c"),
            ],
        };
        let bp = bp(
            flow,
            vec![
                agent("lane-a", None),
                agent("lane-b", None),
                agent("lane-c", None),
            ],
        );
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(
            warnings.is_empty(),
            "a shared nesting root is not an ambiguity: {warnings:?}"
        );
        assert_eq!(naming.resolve("r"), None);
        for ref_ in ["lane-a", "lane-b", "lane-c"] {
            let entry = naming
                .entries()
                .find(|e| e.canonical == ref_)
                .expect("entry present");
            assert_eq!(
                entry.aliases,
                BTreeSet::from([ref_.to_string()]),
                "{ref_} must keep its own ref and drop the contested root"
            );
            assert_eq!(naming.resolve(ref_), Some(ref_));
        }
    }

    /// The genuine ambiguity the weak-claim rule must NOT swallow: two
    /// steps writing the identical depth-1 path both claim `"r"` strongly,
    /// so the pre-existing soft warning + data-plane priority stands.
    #[test]
    fn two_steps_writing_the_identical_root_still_warn() {
        let flow = Node::Seq {
            children: vec![step("first", "$.r"), step("second", "$.r")],
        };
        let bp = bp(flow, vec![agent("first", None), agent("second", None)]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("soft collision is Ok");
        assert_eq!(warnings.len(), 1, "warnings: {warnings:?}");
        assert_eq!(warnings[0].name, "r");
        // Neither step's own ref is "r", so the first-seen owner is kept.
        assert_eq!(naming.resolve("r"), Some("first"));
    }

    /// A step whose ref IS the nesting root claims it strongly; the
    /// nesting sibling's weak claim yields, so no warning fires and
    /// `resolve` keeps pointing at the `ref: "r"` step.
    #[test]
    fn strong_ref_claim_beats_a_nesting_sibling_without_warning() {
        let flow = Node::Seq {
            children: vec![step("r", "$.r_out"), step("lane", "$.r.x")],
        };
        let bp = bp(flow, vec![agent("r", None), agent("lane", None)]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(warnings.is_empty(), "warnings: {warnings:?}");
        assert_eq!(naming.resolve("r"), Some("r"));
        let lane = naming
            .entries()
            .find(|e| e.canonical == "lane")
            .expect("entry present");
        assert_eq!(lane.aliases, BTreeSet::from(["lane".to_string()]));
    }

    /// The hard-error case the ladder dissolves: a DECLARED
    /// `projection_name` against a nesting sibling's weak claim used to
    /// reject the compile outright. The weak claim now yields instead.
    #[test]
    fn declared_name_against_a_nesting_sibling_compiles() {
        let flow = Node::Seq {
            children: vec![step("declarer", "$.declarer_out"), step("lane", "$.r.x")],
        };
        let bp = bp(
            flow,
            vec![agent("declarer", Some("r")), agent("lane", None)],
        );
        let (naming, warnings) =
            StepNaming::from_blueprint(&bp).expect("a yielding weak claim must not hard-error");
        assert!(warnings.is_empty(), "warnings: {warnings:?}");
        assert_eq!(naming.resolve("r"), Some("r"));
        assert_eq!(naming.canonical_of_producer("declarer"), Some("r"));
    }

    /// Backward compat: an uncontested nesting root is still that step's
    /// alias — dropping weak claims is a contest rule, not a blanket
    /// demotion of nested `out` paths.
    #[test]
    fn uncontested_nesting_root_stays_an_alias() {
        let flow = step("only", "$.r.a");
        let bp = bp(flow, vec![agent("only", None)]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(warnings.is_empty());
        let entry = naming
            .entries()
            .find(|e| e.canonical == "only")
            .expect("entry present");
        assert_eq!(
            entry.aliases,
            BTreeSet::from(["only".to_string(), "r".to_string()])
        );
        assert_eq!(naming.resolve("r"), Some("only"));
    }

    #[test]
    fn same_ref_dispatched_twice_unions_out_top_aliases_without_self_collision() {
        let flow = Node::Seq {
            children: vec![step("worker", "$.first"), step("worker", "$.second")],
        };
        let bp = bp(flow, vec![agent("worker", None)]);
        let (naming, warnings) = StepNaming::from_blueprint(&bp).expect("no collision");
        assert!(warnings.is_empty());
        let entry = naming
            .entries()
            .find(|e| e.canonical == "worker")
            .expect("entry present");
        assert_eq!(
            entry.aliases,
            BTreeSet::from([
                "worker".to_string(),
                "first".to_string(),
                "second".to_string()
            ])
        );
    }
}
