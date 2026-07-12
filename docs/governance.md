# Governance control plane

The control plane governs **who may change a contract, how a change is reviewed and
approved, how it is promoted across environments, how the fleet's active versions are
seen, and how every action produces tamper-evident audit evidence**. It sits above the
contract registry: the registry keeps the low-level `publish`/`activate` primitives; the
`GovernanceService` (`px.governance`) wraps them with RBAC, an approval workflow, an
immutable history, promotion, and evidence.

Everything here is available two ways — as a **library** (`px.governance.*`) and over the
**REST API** (`/governance/*`, permission-gated). New KV sets back it: `_gov_changes`
(change requests), `_gov_log` (hash-chained history), `_gov_env` (per-environment COB).

---

## 1. RBAC — config-driven roles → permissions

Authentication yields a `Principal` carrying `roles` (from JWT/OIDC claims or the local
user map). Authorization is **pure config**: `api.auth.roles` maps each role to a list of
permission strings; a principal's effective permissions are the union over its roles.

```yaml
# config/api.yaml  (api.auth)
roles:
  admin:    ["*"]                                  # wildcard = everything
  viewer:   ["governance:read"]
  author:   ["governance:read", "contract:draft", "contract:submit"]
  approver: ["governance:read", "contract:approve", "contract:publish",
             "contract:rollback", "evidence:export"]
  operator: ["governance:read", "backfill:run", "backfill:control",
             "cob:set", "contract:promote"]
```

Wildcards: `*` grants everything; `contract:*` grants every `contract:` permission.

**Permission taxonomy**

| Permission | Gates |
|---|---|
| `governance:read` | list changes, fleet, history, diff, backfill status, lineage |
| `contract:draft` / `contract:submit` | create / submit a change request |
| `contract:approve` | approve or reject a submitted change |
| `contract:publish` | publish an approved change |
| `contract:rollback` | governed rollback of the active pointer |
| `contract:promote` | export/import promotion bundles |
| `backfill:run` / `backfill:control` | run / pause-resume-cancel a backfill |
| `cob:set` | set/advance the environment COB |
| `evidence:export` | export an audit-evidence bundle |

The gate is `require_permission("…")` on each endpoint (403 on failure). In the library
you call the service directly; the service enforces workflow rules (below), and the API
layer enforces the permission. The management console reads the caller's permissions from
`/auth/me` and hides/disables controls the caller lacks (`hasPermission()`), but the server
is always the enforcer.

> Note: the role-less auth schemes (`api_key` / `bearer` / `mtls`) carry no roles and, unless
> `admin_principals` is set, are treated as admin. Use `jwt`/`oidc` (which carry roles) for
> a governed deployment.

---

## 2. Change requests + N-of-M approval

A contract change moves through states — nothing publishes without the policy's approvals:

```
draft ──submit──▶ submitted ──approve×N──▶ approved ──publish──▶ published
                     │                                            
                     └──reject──▶ rejected     (author) ──withdraw──▶ withdrawn
```

**N-of-M policy** — `governance.approval_policy` is `environment → contract-kind → required
distinct approvals`, with `*` fallbacks; `allow_self_approve` defaults **false** (separation
of duties):

```yaml
# config/governance.yaml
approval_policy:
  prod: { storage: 2, "*": 1 }   # two-person rule for storage changes in prod
  "*":  { "*": 1 }
allow_self_approve: false
```

Enforcement: `submit` computes `required_approvals` and runs the compatibility check;
`approve` records **distinct** approvers (a repeat approval is rejected) and blocks the
author when `allow_self_approve` is false; `publish` refuses until
`len(distinct approvals) ≥ required`, then publishes the **exact approved bytes** via the
registry.

**Library**
```python
cr = px.governance.draft("alice", contract_dict)      # -> {id, status:"draft", ...}
px.governance.submit("alice", cr["id"])               # compat report + required_approvals
px.governance.approve("bob", cr["id"])                # a DIFFERENT approver
px.governance.publish("bob", cr["id"])                # only when approved
```
**REST** — `POST /governance/changes`, then `/{id}/{submit|approve|reject|withdraw|publish}`,
`GET /governance/changes[/{id}]`.

---

## 3. Compatibility explanations + diff

Before publish, the change carries a structured compatibility report (also available
standalone). A change is **breaking** when it would strand existing documents (primary key,
manifest set, or canonical location); projection additions are non-breaking (“run a
backfill”).

```python
px.registry.compat_report(storage_contract)
# {compatible: false, changes: [{field:"manifest_set", from, to, breaking:true, reason}]}
px.registry.diff("storage:trade:v1", "storage:trade:v2")   # field-level diff
```
**REST** — `POST /governance/compat`, `GET /governance/diff?a=…&b=…`.

---

## 4. Immutable, hash-chained history

Every governance action is appended to `_gov_log` as one immutable record linked to the
previous by a SHA-256 chain, so any edit/reorder/delete is detectable.

```python
px.governance.log.entries()          # ordered; action, actor, ts, target, detail, hash
px.governance.log.verify()           # {"ok": true, "count": N} — re-walks the chain
```
**REST** — `GET /governance/log?action=&target=` (returns entries **and** a `verify` result).
The Fleet view and console History tab surface the verify status as a badge.

---

## 5. Governed rollback

Rollback flips the active pointer to a prior, already-published version. It is
permission-gated, logged, reversible, and **never mutates version history** (older versions
stay stored and interpretable — documents pinned to them still read correctly).

```python
px.governance.rollback("carol", "storage:trade:v1", reason="regression")
# {entity, from:"storage:trade:v2", to:"storage:trade:v1", reason}
```
**REST** — `POST /governance/rollback {identity, reason}` (`contract:rollback`).

---

## 6. Environment promotion

Environments (dev/uat/prod) are separate deployments. A version is promoted two ways:

- **Signed bundle** (air-gapped / cross-cluster): `export_bundle` produces the contract bytes
  + provenance `{source_env, identity, content_hash}`, HMAC-signed. The target verifies the
  signature and content hash and opens an *approved-by-promotion* change (subject to the
  target's own approval policy). A tampered bundle or wrong secret is rejected.
- **Direct** (`promote_direct`): when the target control plane is reachable in-process, export
  from source and import into target in one call.

```python
b = src.governance.export_bundle("ops", "storage:trade:v1")   # {bundle, signature}
tgt.governance.import_bundle("ops", b["bundle"], b["signature"])  # -> a submitted change
```
**REST** — `POST /governance/promote/{identity}/bundle`, `POST /governance/import`
(`contract:promote`). The signing key is `governance.bundle_secret` (falls back to
`api.auth.jwt_secret`).

---

## 7. COB / processing date

Each environment has a close-of-business / processing date in `_gov_env`. It is the default
valid-time for bitemporal reads (see [bitemporal.md](bitemporal.md)) and is audited.

```python
px.governance.get_cob()                     # e.g. 20260712  (int YYYYMMDD)
px.governance.set_cob("ops", 20260712)      # audited
px.governance.advance_cob("ops")            # +1 calendar day
```
**REST** — `GET /governance/cob`, `POST /governance/cob {cob | advance:true}` (`cob:set`).

---

## 8. Backfill status + intervention

When a contract evolves, `BackfillJob` re-projects existing documents under the new active
contract. It is restartable (a checkpoint cursor resumes an interrupted run) and
**controllable**: the control plane requests pause/resume/cancel and the job honors it
cooperatively between documents, persisting a fleet-visible `state`.

```python
px.governance.backfill_status()                       # {entity: {state, cursor, count, ...}}
px.governance.control_backfill("ops", "trade", "pause")   # pause|resume|cancel|reset
```
**REST** — `GET /governance/backfill`, `POST /governance/backfill/{entity}/{action}`
(`backfill:control`). Run the job itself via `python -m phronexus.cli backfill <entity>`.

---

## 9. Fleet visibility

One call summarises the deployment: active version per entity, registry cache age (drift),
COB, backfill states, and the history chain-integrity check.

```python
px.governance.fleet()
# {environment, is_production, cob, cache_age_seconds, active:{...}, backfills:{...}, history:{ok,count}}
```
**REST** — `GET /governance/fleet` (`governance:read`).

---

## 10. Audit evidence export

Assemble a windowed, tamper-evident bundle from the per-document audit trail (`_audit`), the
request/response interactions (`_interactions`), and the governance log — with a content
hash and the governance-log head hash. The export is itself recorded in the log.

```python
from phronexus.governance.evidence import build_evidence
bundle = build_evidence(px, "auditor", entity="trade")     # {manifest:{content_hash,...}, records:{...}}
```
**REST** — `POST /governance/evidence {entity?, doc_id?, start_ts?, end_ts?, sources?}`
(`evidence:export`).

---

## Configuration summary

```yaml
# config/api.yaml → auth.roles                 (RBAC role→permission map)
# config/governance.yaml → phronexus.config.GovernanceSettings
environment: prod
is_production: true
approval_policy: { "*": { "*": 1 } }
allow_self_approve: false
bundle_secret: "${GOV_BUNDLE_SECRET}"          # promotion/evidence signing (else jwt_secret)
```

Everything above is exercised end-to-end by the CCR reference
([ccr-reference.md](ccr-reference.md)) and the Stage-2 governance test suite
(`pytest -m governance`).
