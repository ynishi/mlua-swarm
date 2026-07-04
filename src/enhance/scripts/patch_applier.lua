-- patch_applier: takes $.prev_bp_yaml + $.patch + $.prev_hash + $.epoch_id,
-- applies RFC 6902 ops in pure Lua, bumps semver, canonicalizes the result,
-- computes the content hash, and returns a VerifyContext (consumed by
-- verifier_router.lua / committer.lua).
--
-- The old host.dry_run bridge has been removed; only 3 host primitives are
-- used now (yaml_to_json / canonical_yaml / content_hash).
--
-- Ported from the Rust PatchApplier::dry_run implementation.

local ctx = _CTX

-- ──────────────────────────────────────────────────────────────────────────
-- Pure Lua RFC 6902 (JSON Patch) impl
-- ──────────────────────────────────────────────────────────────────────────

-- JSON Pointer parsing: "/metadata/description" -> {"metadata", "description"}.
-- "~0" -> "~", "~1" -> "/" (RFC 6901 escape). Order matters: unescape ~1 first,
-- then ~0.
local function parse_pointer(pointer)
  if pointer == nil or pointer == "" then return {} end
  if pointer:sub(1, 1) ~= "/" then
    error("invalid JSON Pointer (must start with '/' or be empty): " .. tostring(pointer))
  end
  local parts = {}
  for part in (pointer .. "/"):sub(2):gmatch("([^/]*)/") do
    -- ~1 -> /, ~0 -> ~ (RFC 6901 §4 order: unescape ~1 first)
    part = part:gsub("~1", "/"):gsub("~0", "~")
    table.insert(parts, part)
  end
  return parts
end

-- Determine whether a Lua table is an array (sequential, 1-indexed) vs a
-- string-keyed table. mlua's from_value turns a JSON array into a sequential
-- Lua table, so we treat #t > 0 with consecutive integer keys as an array.
-- Note that the empty-array vs empty-object distinction is fixed to `{}` or
-- `[]` on the serde_json::Value side; a Lua table's rawget alone can't tell
-- them apart, so that ambiguity is the caller's responsibility.
local function is_array(t)
  if type(t) ~= "table" then return false end
  local n = #t
  if n == 0 then
    -- An empty table can't be classified as array or object here (if the
    -- caller expects an array, it will be treated as one)
    return false
  end
  -- if every key is an integer in 1..n, it's an array
  for k, _ in pairs(t) do
    if type(k) ~= "number" or k < 1 or k > n or math.floor(k) ~= k then
      return false
    end
  end
  return true
end

-- Returns the parent node + final key for a path. The final key is a string;
-- numeric-index interpretation is left to the caller.
local function navigate_parent(root, parts)
  if #parts == 0 then return nil, nil end
  local current = root
  for i = 1, #parts - 1 do
    local key = parts[i]
    if type(current) ~= "table" then
      error("path traverses non-table at part " .. i .. ": " .. tostring(key))
    end
    -- candidate numeric key (array index, 0-based RFC 6901 -> 1-based Lua)
    local num_key = tonumber(key)
    if num_key ~= nil and is_array(current) then
      current = current[num_key + 1]
    else
      current = current[key]
    end
    if current == nil then
      error("path not found at part " .. i .. ": " .. tostring(key))
    end
  end
  return current, parts[#parts]
end

-- Applies a single RFC 6902 op (supports add / remove / replace / move / copy / test).
local function apply_op(root, op)
  local op_name = op.op
  local parts = parse_pointer(op.path)

  if op_name == "replace" then
    if #parts == 0 then return op.value end
    local parent, last_key = navigate_parent(root, parts)
    local num_key = tonumber(last_key)
    if num_key ~= nil and is_array(parent) then
      parent[num_key + 1] = op.value
    else
      parent[last_key] = op.value
    end
    return root

  elseif op_name == "add" then
    if #parts == 0 then return op.value end
    local parent, last_key = navigate_parent(root, parts)
    if last_key == "-" then
      -- append to the end of the array (RFC 6901 §4)
      table.insert(parent, op.value)
    else
      local num_key = tonumber(last_key)
      if num_key ~= nil and is_array(parent) then
        table.insert(parent, num_key + 1, op.value)
      else
        parent[last_key] = op.value
      end
    end
    return root

  elseif op_name == "remove" then
    if #parts == 0 then return nil end
    local parent, last_key = navigate_parent(root, parts)
    local num_key = tonumber(last_key)
    if num_key ~= nil and is_array(parent) then
      table.remove(parent, num_key + 1)
    else
      parent[last_key] = nil
    end
    return root

  elseif op_name == "move" or op_name == "copy" then
    local from_parts = parse_pointer(op.from or "")
    if #from_parts == 0 then
      error(op_name .. ": 'from' required")
    end
    local from_parent, from_key = navigate_parent(root, from_parts)
    local val
    local num_from = tonumber(from_key)
    if num_from ~= nil and is_array(from_parent) then
      val = from_parent[num_from + 1]
    else
      val = from_parent[from_key]
    end
    -- copy: leave the source value in place; move: remove it from the source
    if op_name == "move" then
      if num_from ~= nil and is_array(from_parent) then
        table.remove(from_parent, num_from + 1)
      else
        from_parent[from_key] = nil
      end
    end
    -- add at the destination path
    return apply_op(root, { op = "add", path = op.path, value = val })

  elseif op_name == "test" then
    -- expected value = op.value; error if it doesn't match the value at path
    local parent, last_key = navigate_parent(root, parts)
    local actual
    local num_key = tonumber(last_key)
    if num_key ~= nil and is_array(parent) then
      actual = parent[num_key + 1]
    else
      actual = parent[last_key]
    end
    -- simplified equality check (full pure-Lua deep equal is skipped; this is a
    -- scalar-only POC comparison)
    if actual ~= op.value then
      error("test failed at " .. tostring(op.path))
    end
    return root

  else
    error("unsupported RFC 6902 op: " .. tostring(op_name))
  end
end

local function apply_ops(root, ops)
  for _, op in ipairs(ops or {}) do
    root = apply_op(root, op)
  end
  return root
end

-- ──────────────────────────────────────────────────────────────────────────
-- Pure Lua deep_equal (used for no-op detection + verifiers)
-- ──────────────────────────────────────────────────────────────────────────

local function deep_equal(a, b)
  if a == b then return true end
  if type(a) ~= "table" or type(b) ~= "table" then return false end
  for k, v in pairs(a) do
    if not deep_equal(v, b[k]) then return false end
  end
  for k, _ in pairs(b) do
    if a[k] == nil then return false end
  end
  return true
end

-- ──────────────────────────────────────────────────────────────────────────
-- semver bump (pure Lua, same logic as PatchApplier::dry_run)
-- ──────────────────────────────────────────────────────────────────────────

local function parse_semver(s)
  if type(s) ~= "string" then return 0, 0, 0 end
  local maj, min, pat = s:match("^(%d+)%.(%d+)%.(%d+)")
  if maj == nil then return 0, 0, 0 end
  return tonumber(maj), tonumber(min), tonumber(pat)
end

local function bump_semver(prev_label, bump)
  local maj, min, pat = parse_semver(prev_label)
  if bump == "major" then
    return string.format("%d.0.0", maj + 1)
  elseif bump == "minor" then
    return string.format("%d.%d.0", maj, min + 1)
  else
    -- default = patch
    return string.format("%d.%d.%d", maj, min, pat + 1)
  end
end

-- ──────────────────────────────────────────────────────────────────────────
-- main flow: prev_bp_yaml → JSON → apply ops → bump → canonical yaml → hash
-- ──────────────────────────────────────────────────────────────────────────

local prev_bp_yaml = ctx.prev_bp_yaml
if prev_bp_yaml == nil then error("patch_applier: ctx.prev_bp_yaml missing") end

local prev_bp_json = host.yaml_to_json(prev_bp_yaml)
if type(prev_bp_json) ~= "table" then
  error("patch_applier: yaml_to_json returned non-table")
end

-- Without a deepcopy of the original BP, the post-apply comparison would always
-- be true (shared reference).
-- Cheap deepcopy via a host round-trip: call yaml_to_json(prev_bp_yaml) again
-- (produces an equal but distinct instance).
local before_json = host.yaml_to_json(prev_bp_yaml)

local patch = ctx.patch or {}
local ops = patch.ops or {}
local new_bp_json = apply_ops(prev_bp_json, ops)

-- ──────────────────────────────────────────────────────────────────────────
-- Post-hook: detect a replace on /agents/N/profile/system_prompt and recompute
-- version_hash (hash-consistency concern, see the enhance-agent-integration
-- design notes §5).
--
-- When an Enhance Patch rewrites an agent body, this automatically updates
-- that agent's profile.version_hash to the blake3 hex of the new body,
-- structurally preventing a stale hash from being committed (relied on by the
-- Verifier axis / AgentStore cache).
--
-- Path form: "/agents/<index>/profile/system_prompt" (RFC 6901 0-based index)
-- Only applies to "replace" | "add" ops (a "remove" is equivalent to deleting
-- the agent, so no hash update is needed)
-- ──────────────────────────────────────────────────────────────────────────

local function extract_agent_index(path)
  -- "/agents/12/profile/system_prompt" -> 12 (Lua number)
  local idx = path:match("^/agents/(%d+)/profile/system_prompt$")
  return idx and tonumber(idx) or nil
end

local touched_indices = {}
for _, op in ipairs(ops) do
  if op.op == "replace" or op.op == "add" then
    local idx = extract_agent_index(op.path or "")
    if idx ~= nil then
      touched_indices[idx] = true
    end
  end
end

if next(touched_indices) ~= nil then
  local agents = new_bp_json.agents
  if type(agents) == "table" then
    for idx, _ in pairs(touched_indices) do
      -- RFC 6901 0-based → Lua 1-based
      local agent = agents[idx + 1]
      if type(agent) == "table" and type(agent.profile) == "table" then
        local body = agent.profile.system_prompt
        if type(body) == "string" then
          agent.profile.version_hash = host.content_hash(body)
        end
      end
    end
  end
end

-- Diff check: bump semver if there's a diff, otherwise no bump (so the
-- NoOpVerifier correctly rejects it)
if not deep_equal(new_bp_json, before_json) then
  local metadata = new_bp_json.metadata
  if metadata == nil then
    new_bp_json.metadata = {}
    metadata = new_bp_json.metadata
  end
  local prev_label = metadata.version_label
  local new_label = bump_semver(prev_label, patch.bump or "patch")
  metadata.version_label = new_label
end

-- canonical_yaml = round-trip through the Blueprint type (same format as the
-- Rust-side PatchApplier::dry_run)
local new_bp_yaml = host.canonical_yaml(new_bp_json)
if type(new_bp_yaml) ~= "string" then
  error("patch_applier: canonical_yaml returned non-string")
end

-- new_hash = blake3 32-byte hex of canonical YAML bytes
local new_hash = host.content_hash(new_bp_yaml)

-- VerifyContext shape (consumed by verifier_router.lua / committer.lua):
-- kept backward compatible with the old Rust VerifyContext, plus new_bp_json
-- added so verifiers don't need to re-parse it.
return {
  prev_bp_yaml = prev_bp_yaml,
  prev_bp_json = before_json,
  prev_hash    = ctx.prev_hash,
  patch        = patch,
  new_bp_yaml  = new_bp_yaml,
  new_bp_json  = new_bp_json,
  new_hash     = new_hash,
  epoch_id     = ctx.epoch_id,
}
