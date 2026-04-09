"""Cache Invalidator — Flush service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CacheDatabase:
    """Business-logic service for Flush operations in Cache Invalidator."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("CacheDatabase started")

    def tag(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the tag workflow for a new Flush."""
        if "key" not in payload:
            raise ValueError("Missing required field: key")
        record = self._repo.insert(
            payload["key"], payload.get("invalidated_at"),
            **{k: v for k, v in payload.items()
              if k not in ("key", "invalidated_at")}
        )
        if self._events:
            self._events.emit("flush.tagd", record)
        return record

    def purge(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Flush and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Flush {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("flush.purged", updated)
        return updated

    def refresh(self, rec_id: str) -> None:
        """Remove a Flush and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Flush {rec_id!r} not found")
        if self._events:
            self._events.emit("flush.refreshd", {"id": rec_id})

    def search(
        self,
        key: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search flushs by *key* and/or *status*."""
        filters: Dict[str, Any] = {}
        if key is not None:
            filters["key"] = key
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search flushs: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Flush counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
