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

    p = sub.add_parser("publish-contract", help="publish a single contract file")
    p.add_argument("path")
    p.add_argument("--no-activate", action="store_true")

    p = sub.add_parser("ingest", help="publish a directory of contracts into the store")
    p.add_argument("dir", help="directory of *.yaml/*.json contracts")
    p.add_argument("--no-activate", action="store_true")

    sub.add_parser("list-contracts", help="list contracts stored in the source of truth")

    p = sub.add_parser("get-contract", help="dump one stored contract by identity")
    p.add_argument("identity", help="e.g. storage:trade:v1")

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

    sub.add_parser("doctor", help="check the durability / no-loss posture of this deployment")

    p = sub.add_parser("reap", help="sweep orphan projections")
    p.add_argument("entities", nargs="+")

    args = parser.parse_args(argv)
    px = _build(args)
    try:
        if args.cmd == "publish-contract":
            px.load_contract_file(args.path, activate=not args.no_activate)
            _print({"published": args.path, "activated": not args.no_activate})
        elif args.cmd == "ingest":
            from phronexus.contracts.loader import load_dir

            contracts = load_dir(args.dir)
            for c in contracts:
                px.publish_contract(c, activate=not args.no_activate)
            _print({"ingested": [c.identity() for c in contracts],
                    "count": len(contracts), "activated": not args.no_activate})
        elif args.cmd == "list-contracts":
            _print(px.list_contracts())
        elif args.cmd == "get-contract":
            _print(px.get_contract(args.identity))
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
        elif args.cmd == "doctor":
            rep = px.durability_report()
            mark = {"pass": "✓", "warn": "!", "fail": "✗", "unknown": "?"}
            print(f"durability: {rep['summary']}  (no_loss={rep['no_loss']})")
            for c in rep["checks"]:
                print(f"  {mark.get(c['status'], '?')} {c['link']:32} {c['detail']}")
            if rep.get("unverifiable"):
                print("  not auto-verified:")
                for u in rep["unverifiable"]:
                    print(f"    - {u}")
            return 0 if rep["no_loss"] else 1   # non-zero so CI/deploy gates can fail
        return 0
    finally:
        px.close()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
