"""A small, safe boolean-expression evaluator for transition guards.

Guards are strings like ``"principal > 0 and ccy == 'USD'"`` evaluated against
the candidate document. Only a restricted AST is allowed — comparisons, boolean
and/or/not, names (resolved from the document), and literals — so untrusted
contract text cannot call functions, access attributes, or import anything.
"""

from __future__ import annotations

import ast
import operator
from functools import lru_cache
from typing import Any

from phronexus.errors import GuardError

_CMP = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}


@lru_cache(maxsize=2048)
def _parse(expr: str) -> ast.Expression:
    """Parse (and cache) an expression AST — the same guard/DQ string is evaluated
    on every document, so parsing once per distinct expression is a big win on the
    hot write path."""
    try:
        return ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise GuardError(f"invalid guard expression: {expr!r}") from exc


def safe_eval(expr: str, context: dict[str, Any]) -> bool:
    return bool(_eval(_parse(expr).body, context))


def _eval(node: ast.AST, ctx: dict[str, Any]) -> Any:
    if isinstance(node, ast.BoolOp):
        vals = [_eval(v, ctx) for v in node.values]
        return all(vals) if isinstance(node.op, ast.And) else any(vals)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return not _eval(node.operand, ctx)
    if isinstance(node, ast.Compare):
        left = _eval(node.left, ctx)
        for op, comparator in zip(node.ops, node.comparators):
            fn = _CMP.get(type(op))
            if fn is None:
                raise GuardError(f"unsupported comparison: {type(op).__name__}")
            right = _eval(comparator, ctx)
            if not fn(left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.Name):
        return ctx.get(node.id)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple)):
        return [_eval(e, ctx) for e in node.elts]
    raise GuardError(f"disallowed expression element: {type(node).__name__}")
