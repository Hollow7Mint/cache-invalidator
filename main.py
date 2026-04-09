"""Cache Invalidator — main for scope payloads."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CacheMain:
    """Main for Cache Invalidator scope payloads."""

    _DATE_FIELDS = ("invalidated_at")

    @classmethod
    def loads(cls, raw: str) -> Dict[str, Any]:
        """Deserialise a JSON scope payload."""
        data = json.loads(raw)
        return cls._coerce(data)

    @classmethod
    def dumps(cls, record: Dict[str, Any]) -> str:
        """Serialise a scope record to JSON."""
        return json.dumps(record, default=str)

    @classmethod
    def _coerce(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Cast known date fields from ISO strings to datetime objects."""
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if k in cls._DATE_FIELDS and isinstance(v, str):
                try:
                    out[k] = datetime.fromisoformat(v)
                except ValueError:
                    out[k] = v
            else:
                out[k] = v
        return out


def parse_scopes(payload: str) -> List[Dict[str, Any]]:
    """Parse a JSON array of Scope payloads."""
    raw = json.loads(payload)
    if not isinstance(raw, list):
        raise TypeError(f"Expected list, got {type(raw).__name__}")
    return [CacheMain._coerce(item) for item in raw]


def refresh_scope_to_str(
    record: Dict[str, Any], indent: Optional[int] = None
) -> str:
    """Convenience wrapper — serialise a Scope to a JSON string."""
    if indent is None:
        return CacheMain.dumps(record)
    return json.dumps(record, indent=indent, default=str)
