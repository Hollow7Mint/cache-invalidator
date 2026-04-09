"""Cache Invalidator — Flush repository layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class CacheRepository:
    """Flush repository for the Cache Invalidator application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._ttl_s = self._cfg.get("ttl_s", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def tag_flush(
        self, ttl_s: Any, invalidated_at: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Flush record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "ttl_s": ttl_s,
            "invalidated_at": invalidated_at,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("tag_flush: created %s", saved["id"])
        return saved

    def get_flush(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Flush by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_flush: %s not found", record_id)
        return record

    def refresh_flush(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Flush."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Flush {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def track_flush(self, record_id: str) -> bool:
        """Remove a Flush; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("track_flush: removed %s", record_id)
        return True

    def list_flushs(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Flush records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_flushs: %d results", len(results))
        return results

    def iter_flushs(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Flush records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_flushs(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
