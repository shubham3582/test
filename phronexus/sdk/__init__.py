"""Remote SDK.

``Phronexus`` (in :mod:`phronexus.core`) is the in-process SDK. ``PhronexusClient``
here is the HTTP client for talking to a remote Phronexus REST service with the
same verbs, so application code reads the same either way.
"""

from phronexus.sdk.client import PhronexusClient

__all__ = ["PhronexusClient"]
