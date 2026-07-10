"""Phronexus Core — contract-driven data projection framework.

Public entrypoints:
    from phronexus import Phronexus, Settings
"""

from phronexus.config import Settings
from phronexus.core import Phronexus

__all__ = ["Phronexus", "Settings"]
__version__ = "0.1.0"
