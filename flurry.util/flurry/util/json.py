from typing import Any

import json
import datetime as dt

__all__ = ("JSON",)


class JSON(json.JSONEncoder):
    """A JSON encoder that supports datetime values."""

    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.astimezone(dt.timezone.utc).isoformat()
        return super().default(o)

    @classmethod
    def dumps(cls, val: Any) -> str:
        return json.dumps(val, cls=cls)
