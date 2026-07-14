--- flow_dsl.lua — pure-Lua internal DSL for the flow.ir wire vocabulary.
---
--- `F = require("flow_dsl")`. Wraps Expr / Node AST construction behind a
--- small, chainable Lua API; every builder emits a plain Lua table shaped
--- exactly like the flow.ir wire format (see the field-name source of
--- truth: `mse://guides/blueprint-authoring` § "Expr ops" / "Flow node
--- kinds"). flow_dsl has zero dependencies — it does not `require`
--- `bp_dsl` (bp_dsl depends on flow_dsl, never the reverse).
---
--- ## Expr wrapper
---
--- `F.p"$.x"` / `F.lit(v)` return an Expr wrapper with a method-chain of
--- comparison / boolean helpers (`E:eq(v)`, `E:And(other)`, ...) plus
--- arithmetic operator overloads (`+ - * / %`). Comparison operators
--- (`< <= == ...`) are deliberately NOT overloaded: the Lua VM coerces
--- their result to a VM-level boolean before any metamethod return value
--- could survive as an AST table, so `:lt()` / `:eq()` / ... are the only
--- way to build a comparison Expr.
---
--- Anywhere an Expr is expected, a raw Lua value (auto-`lit`) or another
--- Expr wrapper is also accepted — see `F.unwrap`.
---
--- ## Node builders
---
--- `F.step` / `F.seq` / `F.branch` / `F.loop_` / `F.assign` / `F.try_`
--- return plain Lua tables (no metatable) shaped like flow.ir `Node`s.
--- `loop` / `try` / `in` are Lua reserved words, hence the `loop_` / `try_`
--- suffixes on the Node builders and the `input` rename on `F.step`;
--- `then` / `else` (Branch) are reserved too, hence `on_true` / `on_false`.

local M = {}

local Expr = {}
Expr.__index = Expr

local function is_expr(v)
  return type(v) == "table" and getmetatable(v) == Expr
end

local function wrap(ast)
  return setmetatable({ ast = ast }, Expr)
end

--- `F.raw(t)` — treat an arbitrary raw AST table as an Expr wrapper
--- passthrough (no validation; `t` is emitted verbatim as this Expr's
--- AST). This is the escape hatch for ops with no dedicated builder
--- (e.g. `call_extern`).
function M.raw(t)
  return wrap(t)
end

--- `F.p(path_str)` — a `path` Expr, e.g. `F.p"$.x"`.
function M.p(path_str)
  return wrap({ op = "path", at = path_str })
end

--- `F.lit(v)` — a `lit` Expr wrapping a literal JSON-serializable value.
function M.lit(v)
  return wrap({ op = "lit", value = v })
end

--- `F.unwrap(x)` — Expr wrapper -> its raw AST table; any other raw Lua
--- value -> a `lit` AST (auto-lit). Every builder that accepts "Expr or
--- raw value" funnels its argument through this function; it is exposed
--- directly for DSL authors who build `F.raw()` tables by hand.
function M.unwrap(x)
  if is_expr(x) then
    return x.ast
  end
  return M.lit(x).ast
end

local function binop(op)
  return function(self, other)
    return wrap({ op = op, lhs = self.ast, rhs = M.unwrap(other) })
  end
end

Expr.eq = binop("eq")
Expr.ne = binop("ne")
Expr.lt = binop("lt")
Expr.lte = binop("lte")
Expr.gt = binop("gt")
Expr.gte = binop("gte")

--- `self:And(other)` — variadic `and` Expr over exactly `{self, other}`.
--- For 3+ operands in a single node use `F.all{...}` instead (this method
--- always emits a 2-element `args`, matching the pairwise chain form
--- `a:And(b):And(c)` produces nested `and` nodes, not a flattened one).
function Expr:And(other)
  return wrap({ op = "and", args = { self.ast, M.unwrap(other) } })
end

--- `self:Or(other)` — see `Expr:And`'s note (same variadic-but-pairwise
--- shape; use `F.any{...}` for a single flat N-ary node).
function Expr:Or(other)
  return wrap({ op = "or", args = { self.ast, M.unwrap(other) } })
end

function Expr:Not()
  return wrap({ op = "not", arg = self.ast })
end

function Expr:exists()
  return wrap({ op = "exists", arg = self.ast })
end

function Expr:len()
  return wrap({ op = "len", arg = self.ast })
end

--- `self:contains(needle)` — `in` Expr: true iff `needle` is a member of
--- `self` (the haystack, expected to evaluate to a JSON array).
function Expr:contains(needle)
  return wrap({ op = "in", needle = M.unwrap(needle), haystack = self.ast })
end

-- Arithmetic metamethods. `a`/`b` may be an Expr wrapper OR a raw value
-- (Lua invokes a metamethod as soon as either operand has one, so the
-- other operand is passed through as-is and must go through M.unwrap too).
local function arith(op)
  return function(a, b)
    return wrap({ op = op, lhs = M.unwrap(a), rhs = M.unwrap(b) })
  end
end

Expr.__add = arith("add")
Expr.__sub = arith("sub")
Expr.__mul = arith("mul")
Expr.__div = arith("div")
Expr.__mod = arith("mod")

--- `F.all{e1, e2, ...}` — a single, N-ary `and` Expr (flow-ir's `And.args`
--- is natively variadic, so this is one AST node regardless of list
--- length, not a left-fold of binary nodes).
function M.all(list)
  local args = {}
  for i, v in ipairs(list) do
    args[i] = M.unwrap(v)
  end
  return wrap({ op = "and", args = args })
end

--- `F.any{e1, e2, ...}` — a single, N-ary `or` Expr (see `F.all`'s note).
function M.any(list)
  local args = {}
  for i, v in ipairs(list) do
    args[i] = M.unwrap(v)
  end
  return wrap({ op = "or", args = args })
end

-- ── Node builders (plain tables, no metatable) ──────────────────────────

--- `F.step{id=, agent=, input=, out=}` — a `step` Node.
---
--- - `id` is an optional author-facing label (NOT part of the flow.ir
---   wire format; discarded — flow.ir Steps have no identity of their own,
---   only structural position within a `seq`).
--- - `agent` -> wire `ref` (the dispatch key).
--- - `input` -> wire `in` (`in` is a Lua reserved word, hence the
---   DSL-level rename); accepts an Expr or a raw value (auto-`lit`).
--- - `out` -> wire `out`; must resolve to a `path` Expr (a write target)
---   — flow_dsl does not enforce this, it is the author's responsibility.
function M.step(t)
  return {
    kind = "step",
    ref = t.agent,
    ["in"] = M.unwrap(t.input),
    out = M.unwrap(t.out),
  }
end

--- `F.seq{node1, node2, ...}` — a `seq` Node (children evaluated in
--- order, threading ctx through each).
function M.seq(list)
  local children = {}
  for i, node in ipairs(list) do
    children[i] = node
  end
  return { kind = "seq", children = children }
end

--- `F.branch{cond=, on_true=, on_false=}` — a `branch` Node. `then` /
--- `else` are Lua reserved words, hence `on_true` / `on_false`.
function M.branch(t)
  return {
    kind = "branch",
    cond = M.unwrap(t.cond),
    ["then"] = t.on_true,
    ["else"] = t.on_false,
  }
end

--- `F.loop_{counter=, cond=, max=, body=}` — a `loop` Node. `loop` is a
--- Lua reserved word, hence the trailing underscore.
function M.loop_(t)
  return {
    kind = "loop",
    counter = M.unwrap(t.counter),
    cond = M.unwrap(t.cond),
    body = t.body,
    max = t.max,
  }
end

--- `F.assign{at=, value=}` — an `assign` Node (pure ctx transform, no
--- agent dispatch).
function M.assign(t)
  return {
    kind = "assign",
    at = M.unwrap(t.at),
    value = M.unwrap(t.value),
  }
end

--- `F.try_{body=, catch=, err_at=}` — a `try` Node. `try` is a Lua
--- reserved word, hence the trailing underscore. `err_at` is optional
--- (omitted entirely from the emitted table when not given, matching the
--- wire schema's `#[serde(default)]` field).
function M.try_(t)
  local node = { kind = "try", body = t.body, catch = t.catch }
  if t.err_at ~= nil then
    node.err_at = M.unwrap(t.err_at)
  end
  return node
end

return M
