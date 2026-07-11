"""Decoupled, per-document audit trail (the trace / debug view).

Populated by a change-feed consumer (:class:`~phronexus.audit.worker.AuditWorker`)
that runs standalone in production and inline for the no-services dev mode; read
back via :meth:`~phronexus.audit.log.AuditLog.trace`.
"""

from phronexus.audit.log import AuditLog
from phronexus.audit.worker import AuditingSink, AuditWorker

__all__ = ["AuditLog", "AuditWorker", "AuditingSink"]
