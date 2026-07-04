-- committer: walks the verdicts, tallies them in pure Lua, and returns via the
-- Ok path in both the Applied and Rejected cases.
--
-- ## Responsibilities
--
-- 1. Walk $.verdicts (the verifier result array from Fanout JoinMode::All) and
--    tally Deny outcomes
-- 2. 0 Deny -> committed=true, carrying new_bp_yaml / new_bp_json / new_hash /
--    bump_label from ctx.applied (assembles the material dispatch_one needs
--    for bp_store.write_new and enhance_log_store.append into ctx.commit)
-- 3. 1+ Deny -> committed=false, carrying deny_reasons (assembles the material
--    dispatch_one needs to build the Rejected status and record reasons in
--    the log entry)
--
-- ## The old ok=false return path has been removed
--
-- The old design returned ok=false on a Deny case, bubbling a "blocked" Err up
-- through the dispatcher. That has been changed to reach final_ctx.commit
-- through the Ok path, carrying `{committed: false, reasons: [...]}` instead;
-- dispatch_one resolves this via the extract_status::Rejected mapping.
--
-- ## Return-value shape (this is what gets written into ctx.commit)
--
-- committed=true:
--   { committed = true, new_version = "<hex>", new_bp_yaml = "<yaml>", new_bp_json = {...},
--     bump = "patch"|"minor"|"major", rationale = "<from patch>", verdicts_summary = [...] }
--
-- committed=false:
--   { committed = false, reasons = ["axis1: ...", "axis2: ..."],
--     verdicts_summary = [...], rationale = "<from patch, if any>" }
--
-- Always ok=true; strict (a missing required field fires an Err via error(),
-- 1-value defaulting is disallowed).

local ctx = _CTX

-- ── strict input validation (surfaces via Err instead of 1-value defaulting) ──
if type(ctx) ~= "table" then
  error("committer: _CTX must be a table")
end
local verdicts = ctx.verdicts
if type(verdicts) ~= "table" then
  error("committer: ctx.verdicts must be an array (Fanout join=all output)")
end

-- ── verdicts walk + summary ──────────────────────────────────────────────
local deny_reasons = {}
local verdicts_summary = {}
for _, branch_ctx in ipairs(verdicts) do
  if type(branch_ctx) ~= "table" or type(branch_ctx.verdict) ~= "table" then
    error("committer: verdict branch shape invalid (expected {verdict={axis,outcome}})")
  end
  local v = branch_ctx.verdict
  local axis = tostring(v.axis or error("committer: verdict.axis missing"))
  local outcome = v.outcome
  if type(outcome) ~= "table" then
    error("committer: verdict.outcome missing or not table (axis=" .. axis .. ")")
  end
  if outcome.Deny ~= nil then
    local reason = tostring(outcome.Deny.reason or error("committer: Deny.reason missing"))
    table.insert(deny_reasons, axis .. ": " .. reason)
    table.insert(verdicts_summary, { axis = axis, status = "deny", reason = reason })
  elseif outcome.Pass ~= nil then
    local evidence = tostring(outcome.Pass.evidence or "")
    table.insert(verdicts_summary, { axis = axis, status = "pass", evidence = evidence })
  else
    error("committer: verdict.outcome must be Pass or Deny (axis=" .. axis .. ")")
  end
end

-- ── patch / applied carry (loads the persistable material into ctx.commit) ──
local patch = ctx.patch
if type(patch) ~= "table" then
  error("committer: ctx.patch must be a table (carried from patch-spawner step)")
end
local rationale = tostring(patch.rationale or error("committer: patch.rationale missing"))
local bump = tostring(patch.bump or error("committer: patch.bump missing"))

if #deny_reasons == 0 then
  -- Applied: carry all persistable material from the applied struct
  local applied = ctx.applied
  if type(applied) ~= "table" then
    error("committer: ctx.applied must be a table for committed=true case")
  end
  local new_hash = tostring(applied.new_hash or error("committer: applied.new_hash missing"))
  local new_bp_yaml = tostring(applied.new_bp_yaml or error("committer: applied.new_bp_yaml missing"))
  local new_bp_json = applied.new_bp_json
  if type(new_bp_json) ~= "table" then
    error("committer: applied.new_bp_json must be a table")
  end
  return {
    value = {
      committed = true,
      new_version = new_hash,
      new_bp_yaml = new_bp_yaml,
      new_bp_json = new_bp_json,
      bump = bump,
      rationale = rationale,
      verdicts_summary = verdicts_summary,
    },
    ok = true,
  }
else
  -- Rejected: carry committed=false through the Ok path (so the dispatcher
  -- doesn't treat this as blocked)
  return {
    value = {
      committed = false,
      reasons = deny_reasons,
      rationale = rationale,
      verdicts_summary = verdicts_summary,
    },
    ok = true,
  }
end
