"""Admin CLI: ``phronexus <command>`` (or ``python -m phronexus.cli``).

A thin wrapper over the SDK for operators: publish/load contracts, do CRUD and
queries, run backfills, and sweep orphans. Backend and auth come from the
environment (``PHRONEXUS_*`` / ``.env``), same as every other component.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from phronexus import Phronexus, Settings
from phronexus.admin.backfill import BackfillJob


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _build(args) -> Phronexus:
    px = Phronexus(Settings())
    if getattr(args, "contracts_dir", None):
        px.load_contract_dir(args.contracts_dir)
    return px


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="phronexus", description="Phronexus Core admin CLI")
    parser.add_argument("--contracts-dir", help="load contracts from this directory first")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("publish-contract", help="publish a contract file")
    p.add_argument("path")
    p.add_argument("--no-activate", action="store_true")

    p = sub.add_parser("put", help="write a document")
    p.add_argument("entity")
    p.add_argument("json", help="document as JSON")

    p = sub.add_parser("get", help="read a document by id")
    p.add_argument("entity")
    p.add_argument("doc_id")

    p = sub.add_parser("delete", help="delete a document")
    p.add_argument("entity")
    p.add_argument("doc_id")

    p = sub.add_parser("query", help="run a JSON query")
    p.add_argument("entity")
    p.add_argument("where", help="predicate list as JSON")
    p.add_argument("--view")
    p.add_argument("--limit", type=int, default=100)

    p = sub.add_parser("backfill", help="re-project documents under the active contract")
    p.add_argument("entity")
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("reap", help="sweep orphan projections")
    p.add_argument("entities", nargs="+")

    args = parser.parse_args(argv)
    px = _build(args)
    try:
        if args.cmd == "publish-contract":
            px.load_contract_file(args.path, activate=not args.no_activate)
            _print({"published": args.path, "activated": not args.no_activate})
        elif args.cmd == "put":
            _print({"doc_id": px.put(args.entity, json.loads(args.json))})
        elif args.cmd == "get":
            _print(px.get(args.entity, args.doc_id))
        elif args.cmd == "delete":
            _print({"deleted": px.delete(args.entity, args.doc_id)})
        elif args.cmd == "query":
            q = {"entity": args.entity, "where": json.loads(args.where), "limit": args.limit}
            docs = px.query_view(args.entity, args.view, q) if args.view else px.query(q)
            _print({"count": len(docs), "documents": docs})
        elif args.cmd == "backfill":
            n = BackfillJob(px).run(args.entity, dry_run=args.dry_run)
            _print({"backfilled": n, "dry_run": args.dry_run})
        elif args.cmd == "reap":
            _print({"removed": px.reaper.sweep(args.entities)})
        return 0
    finally:
        px.close()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
