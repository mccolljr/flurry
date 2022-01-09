import json
from typing import Any, Dict, List, overload
import graphene
import graphql.language.ast as gqlast

import money.predicate as P


class JSONScalar(graphene.Scalar):
    """A JSON scalar type for GraphQL."""

    @staticmethod
    def serialize(value):
        return json.dumps(value)

    @staticmethod
    def parse_literal(ast: gqlast.Node):
        if isinstance(ast, gqlast.StringValueNode):
            return json.loads(ast.value)
        raise ValueError("invalid JSON")


class PredicateScalar(graphene.Scalar):
    """A scalar type for query predicates for GraphQL."""

    @staticmethod
    def serialize(value):
        if isinstance(value, P.Predicate):
            return PredicateScalar._serialize_toplevel(value)
        raise ValueError(f"cannot serialize {value} as Predicate scalar")

    @staticmethod
    def _serialize_toplevel(pred: P.Predicate):
        if isinstance(pred, P.And):
            preds = [PredicateScalar._serialize_toplevel(pred) for pred in pred.preds]
            return f"{{ and: [{', '.join(preds)}] }}"
        if isinstance(pred, P.Or):
            alts = [PredicateScalar._serialize_toplevel(pred) for pred in pred.alts]
            return f"{{ or: [{', '.join(alts)}] }}"
        if isinstance(pred, P.Where):
            fields = {
                name: PredicateScalar._serialize_field_pred(fp)
                for name, fp in pred.fields.items()
            }
            fields_as_str = [f"{name}: {value}" for name, value in fields.items()]
            return f"{{ where: {{ {', '.join(fields_as_str)} }} }}"

        raise ValueError(f"cannot serialize predicate {pred}")

    @staticmethod
    def _serialize_field_pred(pred: P.FieldPredicate):
        if isinstance(pred, P.Eq):
            return (
                f"{{ eq: {PredicateScalar._serialize_field_pred_value(pred.expect)} }}"
            )
        if isinstance(pred, P.Less):
            return (
                f"{{ less: {PredicateScalar._serialize_field_pred_value(pred.limit)} }}"
            )
        if isinstance(pred, P.More):
            return (
                f"{{ more: {PredicateScalar._serialize_field_pred_value(pred.limit)} }}"
            )
        if isinstance(pred, P.LessEq):
            return f"{{ less_eq: {PredicateScalar._serialize_field_pred_value(pred.limit)} }}"
        if isinstance(pred, P.MoreEq):
            return f"{{ more_eq: {PredicateScalar._serialize_field_pred_value(pred.limit)} }}"
        if isinstance(pred, P.Between):
            lower = PredicateScalar._serialize_field_pred_value(pred.lower)
            upper = PredicateScalar._serialize_field_pred_value(pred.upper)
            return f"{{ between: [{lower}, {upper}] }}"
        if isinstance(pred, P.OneOf):
            options = [
                PredicateScalar._serialize_field_pred_value(opt) for opt in pred.options
            ]
            return f"{{ one_of: [{', '.join(options)}] }}"
        raise ValueError(f"cannot serialize field predicate {pred}")

    @staticmethod
    def _serialize_field_pred_value(val: Any):
        if val is None or isinstance(val, (str, int, float, bool)):
            return json.dumps(val)
        raise ValueError(f"cannot serialize field predicate value {val}")

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
