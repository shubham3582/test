"""G7 — concurrent contract publish/activate is serializable.

Before F5, publish did two un-CAS'd puts (body then pointer): two different
contracts at the same version silently overwrote each other, and a concurrent
pointer flip could be lost. Now body+pointer commit atomically under generation
CAS, and republishing a divergent body at an existing identity is rejected.
"""

from __future__ import annotations

import threading

import pytest

from phronexus.errors import ContractValidationError
from phronexus.kv.memory import InMemoryKV
from tests.harness import phronexus_with


def _storage(entity: str, version: int, fields: list[str]) -> dict:
    return {
        "kind": "storage", "entity": entity, "version": version,
        "primary_key": ["id"], "manifest_set": f"{entity}_manifest",
        "projections": [
            {"name": "main", "set": f"{entity}_main", "key": "{id}",
             "fields": fields, "canonical": True}
        ],
    }


def test_divergent_same_version_publish_is_rejected():
    px = phronexus_with(InMemoryKV(), contracts=None)
    px.publish_contract(_storage("wid", 1, ["*"]))
    # Idempotent: republishing byte-identical content is a no-op, not an error.
    px.publish_contract(_storage("wid", 1, ["*"]))
    # A different contract at the SAME identity must not silently overwrite.
    with pytest.raises(ContractValidationError):
        px.publish_contract(_storage("wid", 1, ["id"]))
    # the original body is intact
    assert px.get_contract("storage:wid:v1")["projections"][0]["fields"] == ["*"]
    px.close()


@pytest.mark.chaos
def test_concurrent_publishes_are_serializable():
    px = phronexus_with(InMemoryKV(), contracts=None)
    errors: list[Exception] = []

    def publish(v: int) -> None:
        try:
            px.publish_contract(_storage("cx", v, ["*"]))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=publish, args=(v,)) for v in range(1, 6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"no publisher should be starved/lost: {errors}"
    lc = px.list_contracts()
    # every body survived (no lost write) ...
    for v in range(1, 6):
        assert f"storage:cx:v{v}" in lc["contracts"]
    # ... and the active pointer is exactly one real published version (not torn).
    assert lc["active"]["active:storage:cx"] in {f"storage:cx:v{v}" for v in range(1, 6)}
    px.close()
