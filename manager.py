"""Cache Invalidator — Entry service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CacheManager:
    """Business-logic service for Entry operations in Cache Invalidator."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("CacheManager started")

    def purge(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the purge workflow for a new Entry."""
        if "scope" not in payload:
            raise ValueError("Missing required field: scope")
        record = self._repo.insert(
            payload["scope"], payload.get("ttl_s"),
            **{k: v for k, v in payload.items()
              if k not in ("scope", "ttl_s")}
        )
        if self._events:
            self._events.emit("entry.purged", record)
        return record

    def tag(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Entry and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Entry {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("entry.tagd", updated)
        return updated

    def refresh(self, rec_id: str) -> None:
        """Remove a Entry and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Entry {rec_id!r} not found")
        if self._events:
            self._events.emit("entry.refreshd", {"id": rec_id})

    def search(
        self,
        scope: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search entrys by *scope* and/or *status*."""
        filters: Dict[str, Any] = {}
        if scope is not None:
            filters["scope"] = scope
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search entrys: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Entry counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
