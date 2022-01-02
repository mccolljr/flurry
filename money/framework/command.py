from typing import Any, Dict, Tuple

from money.framework.schema import SchemaBase, SchemaMeta


class CommandMeta(SchemaMeta):
    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        x = super().__new__(cls, name, bases, attrs)
        return x


class CommandBase(SchemaBase, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
