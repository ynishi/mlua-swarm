-- verifier_router: selects a verifier axis via $.axis, runs it, and returns
-- { axis, outcome }.
--
-- The old host.verify bridge has been removed; all 4 verifiers (des /
-- canonical / noop / agent-ref) are implemented inline in pure Lua. The only
-- host primitive used is the canonical_yaml + content_hash recomputation for
-- the canonical axis (to stay consistent with the Rust-side ContentHash, blake3
-- is not reimplemented in pure Lua and goes through the host bridge instead).
--
-- Ported from the Rust 4-VerifierAdapter implementation.

local ctx = _CTX
local axis = ctx.axis
local applied = ctx.applied or {}

-- VerifyOutcome externally-tagged serde form (matches the Rust enum
-- VerifyOutcome { Pass {evidence}, Deny {reason} }).
local function pass(evidence)
  return { Pass = { evidence = evidence } }
end
local function deny(reason)
  return { Deny = { reason = reason } }
end

-- Walks a flow.ir Node visiting each Step.ref; same logic as the Rust
-- walk_step_refs.
local function walk_step_refs(node, fn)
  if type(node) ~= "table" then return end
  local kind = node.kind
  if kind == "step" then
    fn(node["ref"])
  elseif kind == "seq" then
    for _, child in ipairs(node.children or {}) do
      walk_step_refs(child, fn)
    end
  elseif kind == "branch" then
    walk_step_refs(node["then"], fn)
    walk_step_refs(node["else"], fn)
  elseif kind == "fanout" then
    walk_step_refs(node.body, fn)
  elseif kind == "loop" then
    walk_step_refs(node.body, fn)
  elseif kind == "try" then
    walk_step_refs(node.body, fn)
    walk_step_refs(node.catch, fn)
  end
end

-- ──────────────────────────────────────────────────────────────────────────
-- 4 verifier inline impl
-- ──────────────────────────────────────────────────────────────────────────

local function verify_des(applied)
  -- Axis (a) Deserialize consistency: can new_bp_yaml be re-parsed into the
  -- Blueprint shape? patch_applier already succeeded at host.canonical_yaml,
  -- meaning the Blueprint round-trip succeeded, so this passes in that case.
  -- Still, do a lightweight shape check in pure Lua (required keys: id / flow /
  -- agents).
  local bp = applied.new_bp_json
  if type(bp) ~= "table" then
    return deny("new_bp_json missing")
  end
  if type(bp.id) ~= "string" or bp.id == "" then
    return deny("blueprint.id missing or not string")
  end
  if type(bp.flow) ~= "table" then
    return deny("blueprint.flow missing or not table")
  end
  if type(bp.agents) ~= "table" then
    return deny("blueprint.agents missing or not array")
  end
  return pass("blueprint shape ok (id/flow/agents present)")
end

local function verify_canonical(applied)
  -- Axis (b) Canonical form match: does re-running canonical_yaml +
  -- content_hash on new_bp_json match the new_hash computed by patch_applier
  -- (i.e. confirms round-trip stability)?
  local canon_yaml = host.canonical_yaml(applied.new_bp_json)
  local canon_hash = host.content_hash(canon_yaml)
  if canon_hash == applied.new_hash then
    return pass("canonical hash matches input hash")
  else
    return deny(string.format(
      "canonical hash mismatch: input=%s canonical=%s",
      tostring(applied.new_hash), tostring(canon_hash)
    ))
  end
end

local function verify_noop(applied)
  -- Axis (c) No-op detection: if new_hash == prev_hash, the patch didn't
  -- change anything -> Deny.
  if applied.new_hash == applied.prev_hash then
    return deny("patch is no-op (new_hash == prev_hash)")
  else
    return pass("patch produces new hash")
  end
end

local function verify_agent_ref(applied)
  -- Axis (d) Agent ref consistency: does every Step.ref in the Blueprint exist
  -- among agents/_.name?
  local bp = applied.new_bp_json
  if type(bp) ~= "table" or type(bp.agents) ~= "table" or type(bp.flow) ~= "table" then
    return deny("blueprint shape invalid for agent-ref verify")
  end

  -- build the agent-name set
  local agent_names = {}
  local agent_count = 0
  for _, a in ipairs(bp.agents) do
    if type(a) == "table" and type(a.name) == "string" then
      agent_names[a.name] = true
      agent_count = agent_count + 1
    end
  end

  -- walk the Step.ref list
  local unresolved = {}
  walk_step_refs(bp.flow, function(r)
    if not agent_names[r] then
      table.insert(unresolved, r)
    end
  end)

  if #unresolved == 0 then
    return pass(string.format("all %d step refs resolved", agent_count))
  else
    return deny("unresolved step refs: " .. table.concat(unresolved, ", "))
  end
end

-- ──────────────────────────────────────────────────────────────────────────
-- main dispatch
-- ──────────────────────────────────────────────────────────────────────────

local outcome
if axis == "des" then
  outcome = verify_des(applied)
elseif axis == "canonical" then
  outcome = verify_canonical(applied)
elseif axis == "noop" then
  outcome = verify_noop(applied)
elseif axis == "agent-ref" then
  outcome = verify_agent_ref(applied)
else
  outcome = deny("unknown verifier axis: " .. tostring(axis))
end

return { axis = axis, outcome = outcome }
