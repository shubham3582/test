"""Uvicorn entrypoint: ``python -m phronexus.api.server``.

Honours server-side TLS/mTLS from :class:`ApiSettings` — set ``tls_ca_certs``
and ``require_client_cert`` to require client certificates.
"""

from __future__ import annotations

import ssl

from phronexus.api.app import create_app
from phronexus.config import Settings


def main() -> None:
    import uvicorn

    settings = Settings()
    app = create_app(settings=settings)
    api = settings.api

    kwargs: dict = {"host": api.host, "port": api.port}
    if api.tls_certfile and api.tls_keyfile:
        kwargs.update(ssl_certfile=api.tls_certfile, ssl_keyfile=api.tls_keyfile)
        if api.tls_ca_certs:
            kwargs["ssl_ca_certs"] = api.tls_ca_certs
            kwargs["ssl_cert_reqs"] = (
                ssl.CERT_REQUIRED if api.require_client_cert else ssl.CERT_OPTIONAL
            )
    uvicorn.run(app, **kwargs)


if __name__ == "__main__":  # pragma: no cover
    main()
