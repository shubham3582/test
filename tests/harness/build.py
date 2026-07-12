"""Build a real :class:`Phronexus` on top of a supplied (e.g. fault-injecting)
KV backend, by intercepting the ``build_store`` / ``build_sink`` factories that
``phronexus.core`` calls — zero production change."""

from __future__ import annotations

from typing import Optional

import phronexus.core as _core
from phronexus import Phronexus, Settings
from phronexus.events.base import EventSink
from phronexus.kv.base import KVStore


def phronexus_with(
    store: KVStore,
    *,
    sink: Optional[EventSink] = None,
    contracts: Optional[str] = "contracts_examples",
    settings: Optional[Settings] = None,
) -> Phronexus:
    """Construct a Phronexus whose KV backend is ``store``.

    Contracts are loaded into that same store, so **arm faults after this
    returns** (otherwise contract ingestion trips them). ``sink`` optionally
    replaces the change-feed sink.
    """
    settings = settings or Settings(backend="memory")
    settings.observability.log_level = "WARNING"
    orig_store, orig_sink = _core.build_store, _core.build_sink
    _core.build_store = lambda s, t=None: store
    if sink is not None:
        _core.build_sink = lambda s: sink
    try:
        px = Phronexus(settings)
    finally:
        _core.build_store, _core.build_sink = orig_store, orig_sink
    if contracts:
        px.load_contract_dir(contracts)
    return px
