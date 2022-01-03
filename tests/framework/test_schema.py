# pylint: disable-all

from typing import Any
import pytest
import datetime

import money.framework.schema as schema


class SchemaBaseForTests(schema.SimpleSchema):
    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.__dict__ == other.__dict__


@pytest.mark.xfail(raises=RuntimeError, strict=True)
def test_no_schema_metaclass():
    class OopsForgotSchema:
        field = schema.Field(schema.Str)


def test_schema_fields():
    const_dt = datetime.datetime(
        year=2021, month=12, day=29, hour=0, minute=0, second=0, microsecond=0
    )

    class Test(SchemaBaseForTests):
        str_field = schema.Field(schema.Str, default="1")
        int_field = schema.Field(schema.Int, default=2)
        flt_field = schema.Field(schema.Float, default=3.0)
        byt_field = schema.Field(schema.Bytes, default=b"4")
        dtm_field = schema.Field(schema.DateTime, default=lambda: const_dt)
        bool_field = schema.Field(schema.Bool, default=False)

    inst = Test()
    assert inst.str_field == "1"
    assert inst.int_field == 2
    assert inst.flt_field == 3.0
    assert inst.byt_field == b"4"
    assert inst.dtm_field == const_dt
    assert inst.bool_field == False

    now_dt = datetime.datetime.now().replace(microsecond=0)
    inst = Test(dtm_field=now_dt)
    assert inst.str_field == "1"
    assert inst.int_field == 2
    assert inst.flt_field == 3.0
    assert inst.byt_field == b"4"
    assert inst.dtm_field == now_dt
    assert inst.bool_field == False

    init_vals = {
        "str_field": "test",
        "int_field": "100",
        "flt_field": "100.25",
        "byt_field": "xyz",
        "dtm_field": now_dt.strftime("%a %b %d %H:%M:%S %Y"),
        "bool_field": True,
    }
    want_vals = {
        "str_field": "test",
        "int_field": 100,
        "flt_field": 100.25,
        "byt_field": b"xyz",
        "dtm_field": now_dt,
        "bool_field": True,
    }
    inst = Test(**init_vals)
    for k in init_vals:
        assert getattr(inst, k) == want_vals[k]
    assert inst.to_dict() == want_vals


def test_schema_inheritance():
    class Base(SchemaBaseForTests):
        base_field = schema.Field(schema.Str)

    class Child(Base):
        child_field = schema.Field(schema.Str)

    class SubChild(Child):
        sub_child_field = schema.Field(schema.Str)

    assert Base.__schema__ == {
        "base_field": Base.base_field,
    }
    assert Child.__schema__ == {
        "base_field": Base.base_field,
        "child_field": Child.child_field,
    }
    assert SubChild.__schema__ == {
        "base_field": Base.base_field,
        "child_field": Child.child_field,
        "sub_child_field": SubChild.sub_child_field,
    }
    assert isinstance(SubChild.base_field.kind, schema.Str)
    assert isinstance(SubChild.child_field.kind, schema.Str)
    assert isinstance(SubChild.sub_child_field.kind, schema.Str)
    assert SubChild.base_field == Base.base_field
    assert SubChild.child_field == Child.child_field


def test_complex_schema_fields():
    class SubObj(SchemaBaseForTests):
        foo = schema.Field(schema.Str)
        bar = schema.Field(schema.Int)

    class TopObj(SchemaBaseForTests):
        coll = schema.Field(
            schema.Collection(schema.Object(SubObj)), default=lambda: []
        )

    assert SubObj.__schema__ == {"foo": SubObj.foo, "bar": SubObj.bar}
    assert TopObj.__schema__ == {"coll": TopObj.coll}
    assert (
        isinstance(TopObj.coll.kind, schema.Collection)
        and isinstance(TopObj.coll.kind.of_kind, schema.Object)
        and TopObj.coll.kind.of_kind.of_typ == SubObj
    )

    obj = TopObj()
    assert obj.coll == []
    obj.coll = [{"foo": "test", "bar": 12}]
    assert obj.coll == [SubObj(foo="test", bar=12)]


def test_complex_schema_fields_nested():
    class NestedC(SchemaBaseForTests):
        a = schema.Field(schema.Int)
        b = schema.Field(schema.Collection(schema.Str), default=lambda: [])

    class NestedB(SchemaBaseForTests):
        c = schema.Field(schema.Str)
        d = schema.Field(schema.Float)

    class NestedA(SchemaBaseForTests):
        x = schema.Field(schema.Object(NestedB))
        y = schema.Field(schema.Object(NestedC))

    class Top(SchemaBaseForTests):
        z = schema.Field(schema.Object(NestedA))

    obj = Top(
        z={"x": {"c": "test", "d": 12.5}, "y": {"a": 100, "b": ["q", "r", "s", "t"]}}
    )

    assert obj.z.x.c == "test"
    assert obj.z.x.d == 12.5
    assert obj.z.y.a == 100
    assert obj.z.y.b == ["q", "r", "s", "t"]
    assert obj.to_dict() == {
        "z": {"x": {"c": "test", "d": 12.5}, "y": {"a": 100, "b": ["q", "r", "s", "t"]}}
    }
    copied = Top(**obj.to_dict())
    assert obj == copied
