"""Governance service: contract change requests, N-of-M approval, governed rollback.

Sits above the contract registry: the registry keeps the low-level
``publish``/``activate`` primitives; this service wraps them with an approval
workflow, an immutable hash-chained audit log, and (later) promotion. A change
moves draft → submitted → approved → published, and can be rejected or withdrawn.

Every mutation is a read-modify-write under generation CAS, so concurrent
approvals are serialised (distinct-approver and N-of-M counts stay correct), and
every action is appended to the :class:`GovernanceLog`.
"""

from __future__ import annotations

import time
import uuid
from typing import Callable, Optional

import structlog

from phronexus.config import GovernanceSettings
from phronexus.contracts.loader import parse_contract
from phronexus.contracts.models import StorageContract
from phronexus.contracts.registry import ContractRegistry, _active_key
from phronexus.errors import GenerationConflict, PhronexusError
from phronexus.governance.log import GovernanceLog
from phronexus.kv.base import KVStore

log = structlog.get_logger("phronexus.governance")

DRAFT, SUBMITTED, APPROVED, PUBLISHED, REJECTED, WITHDRAWN = (
    "draft", "submitted", "approved", "published", "rejected", "withdrawn")


class GovernanceError(PhronexusError):
    """A governance-workflow violation (wrong state, self-approval, etc.)."""

    http_status = 409


class GovernanceService:
    def __init__(self, store: KVStore, registry: ContractRegistry,
                 settings: GovernanceSettings, telemetry=None, *, secret: str = "phronexus"):
        self._store = store
        self._registry = registry
        self._cfg = settings
        self._set = settings.changes_set
        self._secret = settings.bundle_secret or secret
        self.log = GovernanceLog(store, settings.log_set)

    # --- storage helpers ------------------------------------------------

    def get_change(self, change_id: str) -> dict:
        rec = self._store.get(self._set, change_id)
        if rec is None:
            raise GovernanceError(f"change {change_id!r} not found")
        return dict(rec.bins["cr"])

    def list_changes(self) -> list[dict]:
        out = [dict(rec.bins["cr"]) for _k, rec in self._store.scan(self._set)]
        out.sort(key=lambda c: c.get("created_ts", 0), reverse=True)
        return out

    def _update(self, change_id: str, mutator: Callable[[dict], None], retries: int = 6) -> dict:
        """Atomic read-modify-write of a change record under generation CAS."""
        for attempt in range(retries + 1):
            rec = self._store.get(self._set, change_id)
            if rec is None:
                raise GovernanceError(f"change {change_id!r} not found")
            cr = dict(rec.bins["cr"])
            mutator(cr)  # may raise GovernanceError
            cr["updated_ts"] = time.time()
            try:
                self._store.put(self._set, change_id, {"cr": cr},
                                expected_generation=rec.generation)
                return cr
            except GenerationConflict:
                if attempt >= retries:
                    raise
        raise RuntimeError("unreachable")

    def _required_approvals(self, environment: str, kind: str) -> int:
        policy = self._cfg.approval_policy or {}
        env_map = policy.get(environment) or policy.get("*") or {}
        return int(env_map.get(kind, env_map.get("*", 1)))

    # --- workflow -------------------------------------------------------

    def draft(self, actor: str, contract: dict, *, environment: Optional[str] = None) -> dict:
        c = parse_contract(contract)  # validate shape up front
        env = environment or self._cfg.environment
        now = time.time()
        cr = {
            "id": uuid.uuid4().hex, "kind": "publish",
            "contract_kind": c.kind.value, "entity": c.entity, "identity": c.identity(),
            "contract": contract, "environment": env, "status": DRAFT, "author": actor,
            "created_ts": now, "updated_ts": now,
            "approvals": [], "required_approvals": None, "compat": None, "reason": None,
        }
        self._store.put(self._set, cr["id"], {"cr": cr}, expected_generation=0)
        self.log.append("change.drafted", actor, target=cr["identity"],
                        detail={"change_id": cr["id"], "environment": env})
        return cr

    def submit(self, actor: str, change_id: str) -> dict:
        compat_holder: dict = {}

        def _m(cr: dict) -> None:
            if cr["status"] != DRAFT:
                raise GovernanceError(f"cannot submit a change in status {cr['status']!r}")
            c = parse_contract(cr["contract"])
            if isinstance(c, StorageContract):
                cr["compat"] = self._registry.compat_report(c)
            cr["required_approvals"] = self._required_approvals(cr["environment"], cr["contract_kind"])
            cr["status"] = SUBMITTED
            compat_holder["required"] = cr["required_approvals"]

        cr = self._update(change_id, _m)
        self.log.append("change.submitted", actor, target=cr["identity"],
                        detail={"change_id": change_id, "required_approvals": compat_holder.get("required")})
        return cr

    def approve(self, actor: str, change_id: str) -> dict:
        def _m(cr: dict) -> None:
            if cr["status"] != SUBMITTED:
                raise GovernanceError(f"cannot approve a change in status {cr['status']!r}")
            if actor == cr["author"] and not self._cfg.allow_self_approve:
                raise GovernanceError(
                    "author cannot approve their own change (separation of duties)")
            if any(a["approver"] == actor for a in cr["approvals"]):
                raise GovernanceError(f"{actor!r} has already approved this change")
            cr["approvals"].append({"approver": actor, "ts": time.time()})
            if len(cr["approvals"]) >= (cr["required_approvals"] or 1):
                cr["status"] = APPROVED

        cr = self._update(change_id, _m)
        self.log.append("change.approved", actor, target=cr["identity"],
                        detail={"change_id": change_id, "approvals": len(cr["approvals"]),
                                "required": cr["required_approvals"], "status": cr["status"]})
        return cr

    def reject(self, actor: str, change_id: str, reason: str = "") -> dict:
        def _m(cr: dict) -> None:
            if cr["status"] not in (SUBMITTED, DRAFT):
                raise GovernanceError(f"cannot reject a change in status {cr['status']!r}")
            cr["status"] = REJECTED
            cr["reason"] = reason

        cr = self._update(change_id, _m)
        self.log.append("change.rejected", actor, target=cr["identity"],
                        detail={"change_id": change_id, "reason": reason})
        return cr

    def withdraw(self, actor: str, change_id: str) -> dict:
        def _m(cr: dict) -> None:
            if cr["status"] == PUBLISHED:
                raise GovernanceError("cannot withdraw a published change")
            cr["status"] = WITHDRAWN

        cr = self._update(change_id, _m)
        self.log.append("change.withdrawn", actor, target=cr["identity"],
                        detail={"change_id": change_id})
        return cr

    def publish(self, actor: str, change_id: str, *, force: bool = False) -> dict:
        pre = self.get_change(change_id)
        if pre["status"] != APPROVED:
            raise GovernanceError(
                f"change must be approved before publish (status={pre['status']!r})")
        # Publish the exact bytes that were approved.
        self._registry.publish(parse_contract(pre["contract"]), activate=True, force=force)

        def _m(cr: dict) -> None:
            if cr["status"] != APPROVED:
                raise GovernanceError(f"change no longer approved (status={cr['status']!r})")
            cr["status"] = PUBLISHED

        cr = self._update(change_id, _m)
        self.log.append("contract.published", actor, target=cr["identity"],
                        detail={"change_id": change_id, "environment": cr["environment"],
                                "approvals": [a["approver"] for a in cr["approvals"]]})
        return cr

    # --- governed rollback (GOV6) --------------------------------------

    def rollback(self, actor: str, identity: str, *, reason: str = "") -> dict:
        """Flip the active pointer to a prior (already-published) version. Audited,
        reversible, and non-destructive — version history is never mutated."""
        c = self._registry.get_version(identity)  # raises if unknown
        akey = _active_key(c.kind.value, c.entity, getattr(c, "view", None))
        from_id = self._registry.list_contracts()["active"].get(akey)
        self._registry.activate(identity)
        self.log.append("contract.rolledback", actor, target=identity,
                        detail={"entity": c.entity, "from": from_id, "to": identity,
                                "reason": reason})
        return {"entity": c.entity, "from": from_id, "to": identity, "reason": reason}

    # --- environment promotion (GOV4) ----------------------------------

    def export_bundle(self, actor: str, identity: str) -> dict:
        """Produce a signed, provenance-stamped promotion bundle for a stored
        contract version. The target environment imports it with `import_bundle`."""
        from phronexus.governance.promote import content_hash, sign

        c = self._registry.get_version(identity)  # raises if unknown
        doc = c.model_dump(mode="json")
        bundle = {
            "identity": identity, "contract": doc, "source_env": self._cfg.environment,
            "content_hash": content_hash(doc), "exported_ts": time.time(),
            "exported_by": actor, "gov_log_head": self.log.head(),
        }
        signature = sign(self._secret, bundle)
        self.log.append("contract.promotion_exported", actor, target=identity,
                        detail={"source_env": self._cfg.environment,
                                "content_hash": bundle["content_hash"]})
        return {"bundle": bundle, "signature": signature}

    def import_bundle(self, actor: str, bundle: dict, signature: str) -> dict:
        """Verify a promotion bundle and open an approved-by-promotion change in
        THIS environment (subject to this environment's approval policy)."""
        from phronexus.governance.promote import content_hash, verify

        if not verify(self._secret, bundle, signature):
            raise GovernanceError("promotion bundle signature is invalid")
        if content_hash(bundle.get("contract")) != bundle.get("content_hash"):
            raise GovernanceError("promotion bundle content hash mismatch (tampered)")
        cr = self.draft(actor, bundle["contract"], environment=self._cfg.environment)
        self.submit(actor, cr["id"])

        def _m(c: dict) -> None:
            c["provenance"] = {"promoted_from": bundle.get("source_env"),
                               "identity": bundle.get("identity"),
                               "content_hash": bundle.get("content_hash")}

        cr = self._update(cr["id"], _m)
        self.log.append("contract.promotion_imported", actor, target=bundle.get("identity"),
                        detail={"source_env": bundle.get("source_env"),
                                "target_env": self._cfg.environment, "change_id": cr["id"]})
        return cr

    def promote_direct(self, actor: str, identity: str, target: "GovernanceService") -> dict:
        """Direct cross-store promotion: export from self, import into a reachable
        target control plane in one call."""
        exported = self.export_bundle(actor, identity)
        return target.import_bundle(actor, exported["bundle"], exported["signature"])

    # --- COB / processing date (GOV5 default valid-time) ----------------

    def get_cob(self, environment: Optional[str] = None):
        env = environment or self._cfg.environment
        rec = self._store.get(self._cfg.env_set, env)
        return rec.bins.get("cob") if rec else None

    def set_cob(self, actor: str, cob: int, environment: Optional[str] = None) -> dict:
        """Set the environment's close-of-business / processing date (int YYYYMMDD)."""
        env = environment or self._cfg.environment
        prev = self.get_cob(env)
        self._store.put(self._cfg.env_set, env,
                        {"cob": int(cob), "updated_ts": time.time(), "updated_by": actor})
        self.log.append("cob.set", actor, target=env, detail={"from": prev, "to": int(cob)})
        return {"environment": env, "cob": int(cob), "previous": prev}

    def advance_cob(self, actor: str, environment: Optional[str] = None) -> dict:
        """Advance the processing date by one calendar day."""
        import datetime as _dt

        env = environment or self._cfg.environment
        cur = self.get_cob(env)
        if cur is None:
            nxt = int(_dt.date.today().strftime("%Y%m%d"))
        else:
            d = _dt.datetime.strptime(str(cur), "%Y%m%d").date() + _dt.timedelta(days=1)
            nxt = int(d.strftime("%Y%m%d"))
        return self.set_cob(actor, nxt, env)

    # --- fleet view (GOV10) --------------------------------------------

    def fleet(self) -> dict:
        lc = self._registry.list_contracts()
        return {
            "environment": self._cfg.environment,
            "tier": self._cfg.tier,
            "is_production": self._cfg.is_production,
            "promotion_source": self._cfg.promotion_source,
            "cob": self.get_cob(),
            "cache_age_seconds": round(self._registry.cache_age_seconds, 1),
            "active": lc["active"],           # "active:<kind>:<subject>" -> identity
            "contracts": lc["contracts"],
            "backfills": self.backfill_status(),
            "history": self.log.verify(),     # {ok, count} — chain integrity at a glance
        }

    # --- backfill status + intervention (GOV7) -------------------------

    _BF_FIELDS = ("state", "cursor", "count", "done", "requested_action", "error", "updated_ts")

    def backfill_status(self, entity: Optional[str] = None) -> dict:
        out: dict[str, dict] = {}
        for key, rec in self._store.scan(self._cfg.backfill_state_set):
            if entity is not None and key != entity:
                continue
            out[key] = {k: rec.bins.get(k) for k in self._BF_FIELDS}
        return out

    def control_backfill(self, actor: str, entity: str, action: str) -> dict:
        if action not in ("pause", "resume", "cancel", "reset"):
            raise GovernanceError(f"unknown backfill action {action!r}")
        rec = self._store.get(self._cfg.backfill_state_set, entity)
        cur = dict(rec.bins) if rec else {}
        if action == "reset":
            cur.update(cursor=None, done=False, state="reset", requested_action=None)
        elif action == "resume":
            cur.update(requested_action=None, state="running")
        else:  # pause | cancel — the running job picks it up cooperatively
            cur["requested_action"] = action
        cur["updated_ts"] = time.time()
        self._store.put(self._cfg.backfill_state_set, entity, cur)
        self.log.append(f"backfill.{action}", actor, target=entity)
        return {"entity": entity, "action": action}
