from __future__ import annotations

import json
import time

import httpx
import jwt as pyjwt  # PyJWT
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from phronexus import Phronexus, Settings
from phronexus.api import create_app
from phronexus.api import jwt as sessionjwt
from phronexus.api.providers import OidcProvider

ISSUER = "https://login.microsoftonline.com/tenant/v2.0"
CLIENT_ID = "phx-client"
KID = "test-key-1"


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwks(rsa_key):
    pub_jwk = json.loads(pyjwt.algorithms.RSAAlgorithm.to_jwk(rsa_key.public_key()))
    pub_jwk.update({"kid": KID, "use": "sig", "alg": "RS256"})
    return {"keys": [pub_jwk]}


def _id_token(rsa_key, *, roles, nonce, sub="jane@corp.com"):
    claims = {
        "iss": ISSUER, "aud": CLIENT_ID, "sub": "abc-123",
        "preferred_username": sub, "roles": roles, "nonce": nonce,
        "exp": int(time.time()) + 300, "iat": int(time.time()),
    }
    return pyjwt.encode(claims, rsa_key, algorithm="RS256", headers={"kid": KID})


def _mock_http(rsa_key, id_token):
    disc = {
        "issuer": ISSUER,
        "authorization_endpoint": ISSUER + "/authorize",
        "token_endpoint": ISSUER + "/token",
        "jwks_uri": ISSUER + "/keys",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/.well-known/openid-configuration"):
            return httpx.Response(200, json=disc)
        if url.endswith("/keys"):
            return httpx.Response(200, json=_jwks(rsa_key))
        if url.endswith("/token"):
            return httpx.Response(200, json={"id_token": id_token, "token_type": "Bearer"})
        return httpx.Response(404)

    return httpx.Client(transport=httpx.MockTransport(handler))


def _provider(rsa_key, id_token):
    s = Settings(backend="memory")
    s.api.auth.oidc.issuer = ISSUER
    s.api.auth.oidc.client_id = CLIENT_ID
    s.api.auth.oidc.redirect_uri = "http://localhost:8080/auth/oidc/callback"
    p = OidcProvider(s.api.auth.oidc)
    p._http_client = _mock_http(rsa_key, id_token)   # inject mocked IdP
    return p


def test_authorization_url_has_pkce_and_client(rsa_key):
    p = _provider(rsa_key, _id_token(rsa_key, roles=["admin"], nonce="n"))
    url = p.authorization_url(state="ST", code_challenge="CH", nonce="n")
    assert url.startswith(ISSUER + "/authorize?")
    for frag in ["client_id=phx-client", "code_challenge=CH", "code_challenge_method=S256", "state=ST"]:
        assert frag in url


def test_exchange_and_verify_returns_claims_and_roles(rsa_key):
    token = _id_token(rsa_key, roles=["admin", "trader"], nonce="nonce-1")
    p = _provider(rsa_key, token)
    claims = p.exchange_and_verify("the-code", "verifier", nonce="nonce-1")
    assert claims["preferred_username"] == "jane@corp.com"
    assert p.roles_from_claims(claims) == ["admin", "trader"]


def test_nonce_mismatch_rejected(rsa_key):
    token = _id_token(rsa_key, roles=["admin"], nonce="expected")
    p = _provider(rsa_key, token)
    from phronexus.errors import ConfigError
    with pytest.raises(ConfigError):
        p.exchange_and_verify("code", "verifier", nonce="different")


def test_tampered_token_fails_signature(rsa_key):
    token = _id_token(rsa_key, roles=["admin"], nonce="n") + "x"
    p = _provider(rsa_key, token)
    with pytest.raises(Exception):
        p.exchange_and_verify("code", "verifier", nonce="n")


# --- endpoint flow (stubbed provider) ------------------------------------

class _StubProvider:
    name = "oidc"

    def authorization_url(self, *, state, code_challenge, nonce):
        return f"https://idp.example/authorize?state={state}&nonce={nonce}"

    def exchange_and_verify(self, code, verifier, *, nonce=None):
        return {"preferred_username": "jane@corp.com", "roles": ["admin"], "nonce": nonce}

    def roles_from_claims(self, claims):
        return claims.get("roles", [])

    def authenticate(self, u, p):  # pragma: no cover
        return None


def _oidc_client():
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.api.auth.schemes = ["jwt"]
    s.api.auth.provider = "oidc"
    s.api.auth.jwt_secret = "sekret"
    s.api.auth.oidc.enabled = True
    s.api.auth.oidc.issuer = ISSUER
    s.api.auth.oidc.client_id = CLIENT_ID
    px = Phronexus(s)
    app = create_app(px=px)
    app.state.auth_provider = _StubProvider()  # avoid network in the endpoint test
    return TestClient(app), s


def test_oidc_login_redirects_to_idp():
    c, _ = _oidc_client()
    r = c.get("/auth/oidc/login", follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"].startswith("https://idp.example/authorize")


def test_oidc_callback_issues_session_token():
    c, s = _oidc_client()
    state = sessionjwt.encode({"v": "verifier", "n": "nonce", "exp": int(time.time()) + 300}, s.api.auth.jwt_secret)
    r = c.get(f"/auth/oidc/callback?code=abc&state={state}", follow_redirects=False)
    assert r.status_code == 307
    loc = r.headers["location"]
    assert loc.startswith("/ui/#token=")
    token = loc.split("#token=", 1)[1]
    claims = sessionjwt.decode(token, s.api.auth.jwt_secret)
    assert claims["sub"] == "jane@corp.com" and claims["roles"] == ["admin"]


def test_oidc_login_disabled_when_provider_local():
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    px = Phronexus(s)
    c = TestClient(create_app(px=px))
    assert c.get("/auth/oidc/login", follow_redirects=False).status_code == 403


def test_config_reports_oidc():
    c, _ = _oidc_client()
    body = c.get("/auth/config").json()
    assert body["provider"] == "oidc" and body["oidc_enabled"] is True
