"""Config-driven search: an inverted index plus a JSON/YAML query engine."""

from phronexus.query.engine import QueryEngine
from phronexus.query.inverted import InvertedIndex
from phronexus.query.models import QueryDoc

__all__ = ["QueryEngine", "InvertedIndex", "QueryDoc"]
