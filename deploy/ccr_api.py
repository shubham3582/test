"""Serve the Phronexus REST API + management UI against an EXISTING backend.

Unlike ``ccr_demo`` (which seeds in-memory data), this reads whatever contracts
and documents are already persisted in the configured backend — e.g. the live
Aerospike stack after ``examples/ccr/run_ccr.py`` has run — so you can browse the
real, persisted CCR data (trace / find / lineage) in the browser.

    docker run --network phronexus_default -p 8088:8080 \
      -e PHRONEXUS_BACKEND=aerospike \
      -e PHRONEXUS_AEROSPIKE__HOSTS=aerospike:3000 \
      -e PHRONEXUS_AEROSPIKE__NAMESPACE=phronexus \
      -v "$PWD/deploy:/app/deploy" phronexus-app python -m deploy.ccr_api

Login: admin/admin (full), or author/approver/viewer (RBAC) — see ccr_demo.py.
"""

from __future__ import annotations

from phronexus import Settings
from phronexus.api.app import create_app


def _settings() -> Settings:
    s = Settings()  # backend + aerospike/kafka connection come from the environment
    # Aerospike CE has no native multi-record transactions: run best-effort so the
    # API can start and write (reads are unaffected).
    s.aerospike.use_native_txn = False
    s.statemachine.require_atomic = False
    # UI auth: local password login -> JWT, config-driven RBAC (mirrors ccr_demo).
    a = s.api.auth
    a.schemes = ["jwt"]
    a.provider = "local"
    a.jwt_secret = "phronexus-ccr-demo-secret"
    a.users = {
        "admin": {"password": "admin", "roles": ["admin"]},
        "author": {"password": "author", "roles": ["author"]},
        "approver": {"password": "approver", "roles": ["approver"]},
        "viewer": {"password": "viewer", "roles": ["viewer"]},
    }
    a.roles = {
        "admin": ["*"],
        "author": ["governance:read", "contract:draft", "contract:submit"],
        "approver": ["governance:read", "contract:approve", "contract:publish", "contract:rollback"],
        "viewer": ["governance:read"],
    }
    return s


app = create_app(settings=_settings())


def main() -> None:
    import uvicorn
    print("[ccr-api] UI at http://localhost:8088/ui (login admin/admin) — backed by Aerospike", flush=True)
    print("[ccr-api] trace/find this id -> CCR-T-1 (entity 'ccr_trade')", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
