import json
from typing import Any, Dict, List, overload
import graphene
import graphql.language.ast as gqlast

import money.framework.predicate as P


class JSONScalar(graphene.Scalar):
    @staticmethod
    def serialize(value):
        return json.dumps(value)

    @staticmethod
    def parse_literal(ast: gqlast.Node):
        if isinstance(ast, gqlast.StringValueNode):
            return json.loads(ast.value)
        raise ValueError("invalid JSON")


class PredicateScalar(graphene.Scalar):
    @staticmethod
    def serialize(value):
        if isinstance(value, P.Predicate):
            return PredicateScalar._serialize_toplevel(value)
        raise ValueError(f"cannot serialize {value} as Predicate scalar")

    @staticmethod
    def _serialize_toplevel(p: P.Predicate):
        if isinstance(p, P.And):
            preds = [PredicateScalar._serialize_toplevel(pred) for pred in p.preds]
            return f"{{ and: [{', '.join(preds)}] }}"
        elif isinstance(p, P.Or):
            alts = [PredicateScalar._serialize_toplevel(pred) for pred in p.alts]
            return f"{{ or: [{', '.join(alts)}] }}"
        elif isinstance(p, P.Where):
            fields = {
                name: PredicateScalar._serialize_field_pred(fp)
                for name, fp in p.fields.items()
            }
            fields_as_str = [f"{name}: {value}" for name, value in fields.items()]
            return f"{{ where: {{ {', '.join(fields_as_str)} }} }}"

        raise ValueError(f"cannot serialize predicate {p}")

    @staticmethod
    def _serialize_field_pred(p: P.FieldPredicate):
        if isinstance(p, P.Eq):
            return f"{{ eq: {PredicateScalar._serialize_field_pred_value(p.expect)} }}"
        elif isinstance(p, P.Less):
            return f"{{ less: {PredicateScalar._serialize_field_pred_value(p.limit)} }}"
        elif isinstance(p, P.More):
            return f"{{ more: {PredicateScalar._serialize_field_pred_value(p.limit)} }}"
        elif isinstance(p, P.LessEq):
            return (
                f"{{ less_eq: {PredicateScalar._serialize_field_pred_value(p.limit)} }}"
            )
        elif isinstance(p, P.MoreEq):
            return (
                f"{{ more_eq: {PredicateScalar._serialize_field_pred_value(p.limit)} }}"
            )
        elif isinstance(p, P.Between):
            lower = PredicateScalar._serialize_field_pred_value(p.lower)
            upper = PredicateScalar._serialize_field_pred_value(p.upper)
            return f"{{ between: [{lower}, {upper}] }}"
        elif isinstance(p, P.OneOf):
            options = [
                PredicateScalar._serialize_field_pred_value(opt) for opt in p.options
            ]
            return f"{{ one_of: [{', '.join(options)}] }}"
        raise ValueError(f"cannot serialize field predicate {p}")

    @staticmethod
    def _serialize_field_pred_value(v: Any):
        if v is None or isinstance(v, (str, int, float, bool)):
            return json.dumps(v)
        raise ValueError(f"cannot serialize field predicate value {v}")

    @staticmethod
    def parse_literal(ast: gqlast.Node):
        if isinstance(ast, gqlast.ObjectValueNode):
            return P.Predicate.from_dict(PredicateScalar._node_to_value(ast))

        raise ValueError("bad predicate")

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.ObjectValueNode) -> Dict[str, Any]:
        ...

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.ListValueNode) -> List[Any]:
        ...

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.StringValueNode) -> str:
        ...

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.IntValueNode) -> int:
        ...

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.FloatValueNode) -> float:
        ...

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.BooleanValueNode) -> bool:
        ...

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.NullValueNode) -> None:
        ...

    @overload
    @staticmethod
    def _node_to_value(node: gqlast.ValueNode) -> Any:
        ...

    @staticmethod
    def _node_to_value(node: gqlast.ValueNode):
        if isinstance(node, gqlast.ListValueNode):
            return [PredicateScalar._node_to_value(v) for v in node.values]
        if isinstance(node, gqlast.ObjectValueNode):
            return {
                field.name.value: PredicateScalar._node_to_value(field.value)
                for field in node.fields
            }
        if isinstance(node, gqlast.StringValueNode):
            return node.value
        if isinstance(node, gqlast.IntValueNode):
            return int(node.value)
        if isinstance(node, gqlast.FloatValueNode):
            return float(node.value)
        if isinstance(node, gqlast.BooleanValueNode):
            return node.value
        if isinstance(node, gqlast.NullValueNode):
            return None
        raise ValueError(f"unable to convert {node} to a value")
