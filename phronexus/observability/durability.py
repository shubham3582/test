"""Durability / no-loss preflight — confirm the *configuration posture*.

Answers "can this deployment lose a durably-accepted message?" by checking the
links that make the effectively-once pipeline no-loss (see
docs/ccr-reference.md → "Durability & delivery guarantees"). It confirms the
config supports no-loss; it cannot prove a specific message survives a specific
failure, and it cannot see third-party producers writing to the input topics.

Status per check: ``pass`` (no-loss) · ``warn`` (weaker guarantee / possible loss)
· ``fail`` (loss on a normal failure, e.g. an in-memory store) · ``unknown``.
"""

from __future__ import annotations

from typing import Any


def _parse_info(text: str) -> dict[str, str]:
    """Aerospike info responses are ``k=v;k=v;...``."""
    out: dict[str, str] = {}
    for part in text.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _namespace_info(px, ns: str) -> dict[str, str]:
    try:
        client = px.store.native_client()
        resp = client.info_all(f"namespace/{ns}")  # {node: (err, response)} or {node: response}
        for val in resp.values():
            text = val[1] if isinstance(val, (tuple, list)) else val
            if text:
                return _parse_info(text)
    except Exception:  # noqa: BLE001 - best effort; unknown if we can't introspect
        pass
    return {}


def durability_report(px) -> dict[str, Any]:
    s = px.settings
    checks: list[dict[str, str]] = []

    def add(link: str, status: str, detail: str) -> None:
        checks.append({"link": link, "status": status, "detail": detail})

    # 1) Consumer — offsets committed only after durable processing (framework-fixed).
    add("consumer.commit_after_process", "pass",
        "manual offset commit after durable processing (enable.auto.commit=false)")

    # 2) Producer — acks=all + idempotence (pinned in make_producer).
    if s.kafka.enabled:
        conf = s.kafka.client_config()
        acks = str(conf.get("acks", "all"))
        idem = bool(conf.get("enable.idempotence", True))
        ok = acks in ("all", "-1") and idem
        add("producer.durable", "pass" if ok else "warn",
            f"acks={acks}, enable.idempotence={idem}")
    else:
        add("producer.durable", "warn",
            "kafka disabled — change feed is an in-memory buffer (dev only), not durable")

    # 3) Rejects — a DLQ so poison messages aren't silently dropped.
    dlq = s.statemachine.dlq_topic
    add("reject.dlq", "pass" if dlq else "warn",
        f"dlq_topic={dlq!r}" if dlq else "no dlq_topic — rejected/poison messages are acked and dropped")

    # 3b) Atomic multi-record writes — the manifest write and the state-machine
    #     "state + outputs + dedup" commit are only all-or-nothing (crash-atomic /
    #     exactly-once) with atomic transactions. Without them a crash can tear a
    #     write; the SM cannot be exactly-once (see statemachine.require_atomic).
    try:
        atomic = px.store.supports_atomic_txn()
    except Exception:  # noqa: BLE001
        atomic = None
    if atomic is True:
        add("store.atomic_writes", "pass", "atomic multi-record transactions available")
    elif atomic is False:
        add("store.atomic_writes", "warn",
            "native transactions off — multi-record writes are not atomic; "
            "state-machine exactly-once is not guaranteed (best-effort at-least-once)")
    else:
        add("store.atomic_writes", "unknown", "could not determine transaction atomicity")

    # 4) Store durability.
    if s.backend == "memory":
        add("store.durable", "fail", "in-memory backend — all data lost on restart (dev/test only)")
    elif s.backend == "aerospike":
        info = _namespace_info(px, s.aerospike.namespace)
        if not info:
            add("store.durable", "unknown", "could not introspect the Aerospike namespace")
        else:
            se = info.get("storage-engine", info.get("type", "?"))
            persistent = se not in ("memory", "?") or "device" in se or "file" in se
            add("store.persistent", "pass" if persistent else "fail",
                f"storage-engine={se}" + ("" if persistent else " — memory namespace loses data on restart"))
            sc = info.get("strong-consistency", "?")
            add("store.strong_consistency",
                "pass" if sc == "true" else ("warn" if sc == "false" else "unknown"),
                f"strong-consistency={sc}" + ("" if sc == "true" else " — non-SC can drop an acked write on failover"))
            rf = info.get("effective_replication_factor", info.get("replication-factor", "?"))
            rf_ok = rf.isdigit() and int(rf) >= 2
            add("store.replication", "pass" if rf_ok else "warn", f"replication-factor={rf}")

    fails = [c for c in checks if c["status"] == "fail"]
    warns = [c for c in checks if c["status"] == "warn"]
    unknowns = [c for c in checks if c["status"] == "unknown"]
    return {
        "no_loss": not fails and not warns,
        "summary": ("no-loss configuration" if not fails and not warns
                    else "LOSS POSSIBLE — see failing checks" if fails
                    else "at risk — see warnings"),
        "checks": checks,
        "unverifiable": [
            "third-party producers writing to the input topics (use acks=all upstream)",
            "broker-side min.insync.replicas / retention / unclean.leader.election "
            "(confirm on the Kafka/MSK cluster)",
        ] + (["Aerospike namespace could not be read"] if unknowns else []),
    }
