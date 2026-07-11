"""Simple load/throughput harness (in-memory backend).

    python scripts/loadtest.py --docs 20000 --queries 5000

Reports write/read/query throughput and lands the same data through the
retention worker so the whole pipeline is exercised end to end.
"""

from __future__ import annotations

import argparse
import random
import time

from phronexus import Phronexus, Settings
from phronexus.retention import InMemoryWarehouse, MemoryEventSource, RetentionWorker

CPTYS = ["GS", "JPM", "MS", "BofA", "Citi", "DB", "BARC", "UBS"]
CCYS = ["USD", "EUR", "GBP", "JPY"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=20000)
    ap.add_argument("--queries", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    rnd = random.Random(args.seed)

    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    px = Phronexus(s)
    px.load_contract_dir("contracts_examples")

    # writes
    t0 = time.perf_counter()
    for i in range(args.docs):
        px.put("trade", {
            "trade_id": f"T{i}",
            "counterparty": rnd.choice(CPTYS),
            "notional": round(rnd.uniform(1e3, 1e7), 2),
            "ccy": rnd.choice(CCYS),
            "trade_date": 20250101 + rnd.randint(0, 200),
            "book": f"BOOK-{rnd.randint(1, 20)}",
        })
    tw = time.perf_counter() - t0

    # point reads
    t0 = time.perf_counter()
    for _ in range(args.queries):
        px.get("trade", f"T{rnd.randint(0, args.docs - 1)}")
    tr = time.perf_counter() - t0

    # searches
    t0 = time.perf_counter()
    hits = 0
    for _ in range(args.queries):
        res = px.query({"entity": "trade", "where": [
            {"field": "counterparty", "op": "eq", "value": rnd.choice(CPTYS)},
            {"field": "trade_date", "op": "gte", "value": 20250150},
        ], "limit": 50})
        hits += len(res)
    tq = time.perf_counter() - t0

    # retention pipeline
    t0 = time.perf_counter()
    worker = RetentionWorker(px.registry, InMemoryWarehouse())
    stats = worker.run(MemoryEventSource(px.sink), batch_size=1000)
    tret = time.perf_counter() - t0

    def rate(n, t):
        return f"{n / t:,.0f}/s" if t else "n/a"

    print(f"writes : {args.docs:>8,} in {tw:6.2f}s  {rate(args.docs, tw)}")
    print(f"reads  : {args.queries:>8,} in {tr:6.2f}s  {rate(args.queries, tr)}")
    print(f"queries: {args.queries:>8,} in {tq:6.2f}s  {rate(args.queries, tq)}  (avg {hits/max(args.queries,1):.1f} hits)")
    print(f"retain : {stats['upserts']:>8,} appends in {tret:6.2f}s  {rate(stats['upserts'], tret)}")
    px.close()


if __name__ == "__main__":
    main()
