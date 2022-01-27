from typing import Any

import json
import base64
import binascii
import datetime as dt


__all__ = ("JSON",)


class JSON(json.JSONEncoder):
    """A JSON encoder that supports datetime values."""

    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.astimezone(dt.timezone.utc).isoformat()
        if isinstance(o, bytes):
            try:
                return str(o, "utf-8")
            except UnicodeDecodeError:
                return f"base64:{str(base64.b64encode(o), 'utf-8')}"
        return super().default(o)

    @classmethod
    def dumps(cls, val: Any) -> str:
        return json.dumps(val, cls=cls)

    @classmethod
    def loads(cls, val: str) -> Any:
        return cls.__transform(json.loads(val))

    @classmethod
    def __transform(cls, val: Any) -> Any:
        if isinstance(val, str) and val.startswith("base64:"):
            try:
                return base64.b64decode(val[7:], validate=True)
            except binascii.Error:
                return val
        if isinstance(val, list):
            return [cls.__transform(elt) for elt in val]
        if isinstance(val, dict):
            return {k: cls.__transform(v) for k, v in val.items()}
        return val
