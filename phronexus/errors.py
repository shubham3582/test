"""Typed error hierarchy for Phronexus.

Every error carries a stable ``code`` so the REST layer can map it to an HTTP
status and clients can branch on it without string matching.
"""

from __future__ import annotations


class PhronexusError(Exception):
    """Base class for all Phronexus errors."""

    code = "phronexus_error"
    http_status = 500


class ConfigError(PhronexusError):
    code = "config_error"
    http_status = 500


# --- Contracts -----------------------------------------------------------

class ContractError(PhronexusError):
    code = "contract_error"
    http_status = 400


class ContractNotFound(ContractError):
    code = "contract_not_found"
    http_status = 404


class ContractValidationError(ContractError):
    code = "contract_validation_error"
    http_status = 422


# --- Storage / KV --------------------------------------------------------

class StorageError(PhronexusError):
    code = "storage_error"
    http_status = 500


class GenerationConflict(StorageError):
    """Optimistic-concurrency (CAS) check failed on a record generation."""

    code = "generation_conflict"
    http_status = 409


# --- Write path ----------------------------------------------------------

class WriteError(PhronexusError):
    code = "write_error"
    http_status = 400


class DocumentAlreadyExists(WriteError):
    """Raised under an ``insert_only`` update policy when the doc is present."""

    code = "document_already_exists"
    http_status = 409


class DocumentNotFound(PhronexusError):
    code = "document_not_found"
    http_status = 404


class ValidationError(PhronexusError):
    code = "validation_error"
    http_status = 422


# --- Query / views -------------------------------------------------------

class QueryError(PhronexusError):
    code = "query_error"
    http_status = 400


# --- state machine -------------------------------------------------------

class TransitionRejected(PhronexusError):
    """No valid transition for the (event_type, current_state) pair, or a guard failed."""

    code = "transition_rejected"
    http_status = 409


class GuardError(PhronexusError):
    code = "guard_error"
    http_status = 400


class ViewError(PhronexusError):
    code = "view_error"
    http_status = 400
