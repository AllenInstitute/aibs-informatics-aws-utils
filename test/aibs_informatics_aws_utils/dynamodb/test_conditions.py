from decimal import Decimal
from typing import Any, Dict, Optional, Union

from aibs_informatics_core.models.aws.dynamodb import (
    AttributeBaseExpression,
    ConditionBaseExpression,
)
from aibs_informatics_test_resources import does_not_raise
from boto3.dynamodb.conditions import Attr, ConditionBase, Key
from pytest import mark, param

from aibs_informatics_aws_utils.dynamodb.conditions import (
    ConditionBaseTranslator,
    ConditionExpressionComponents,
    UpdateExpressionComponents,
)


@mark.parametrize(
    "condition, is_key, expected, expected_serialized_values",
    [
        param(
            Key("key1").eq("str_value"),
            True,
            ConditionExpressionComponents("#n0 = :v0", {"#n0": "key1"}, {":v0": "str_value"}),
            {":v0": {"S": "str_value"}},
            id="Simple Key Condition",
        ),
        param(
            Key("key1").eq("str_value") & Key("key2").eq(Decimal.from_float(1.5)),
            True,
            ConditionExpressionComponents(
                "(#n0 = :v0 AND #n1 = :v1)",
                {"#n0": "key1", "#n1": "key2"},
                {":v0": "str_value", ":v1": Decimal.from_float(1.5)},
            ),
            {":v0": {"S": "str_value"}, ":v1": {"N": "1.5"}},
            id="Compound AND Key Condition",
        ),
        param(
            Attr("A.B").eq("str_value") & Attr("A.C").lt(Decimal.from_float(1.5)),
            False,
            ConditionExpressionComponents(
                "(#n0.#n1 = :v0 AND #n2.#n3 < :v1)",
                {"#n0": "A", "#n1": "B", "#n2": "A", "#n3": "C"},
                {":v0": "str_value", ":v1": Decimal.from_float(1.5)},
            ),
            {":v0": {"S": "str_value"}, ":v1": {"N": "1.5"}},
            id="Compound AND Attr Condition",
        ),
    ],
)
def test__ExpressionComponents__from_condition(
    condition: ConditionBase,
    is_key: bool,
    expected: ConditionExpressionComponents,
    expected_serialized_values: dict,
):
    actual = ConditionExpressionComponents.from_condition(condition, is_key)
    assert actual == expected
    assert actual.expression_attribute_values__serialized == expected_serialized_values


@mark.parametrize(
    "attributes, expected, expected_serialized_values",
    [
        param(
            {
                "str_attr": "123",
                "int_attr": 123,
                "bool_attr": False,
                "list_attr": [1, 2, 3],
                "set_attr": {1, 2, 3},
                "map_attr": {"a": 1},
            },
            UpdateExpressionComponents(
                "SET #n0 = :v0, #n1 = :v1, #n2 = :v2, #n3 = :v3, #n4 = :v4, #n5 = :v5",
                {
                    "#n0": "str_attr",
                    "#n1": "int_attr",
                    "#n2": "bool_attr",
                    "#n3": "list_attr",
                    "#n4": "set_attr",
                    "#n5": "map_attr",
                },
                {
                    ":v0": "123",
                    ":v1": 123,
                    ":v2": False,
                    ":v3": [1, 2, 3],
                    ":v4": {1, 2, 3},
                    ":v5": {"a": 1},
                },
            ),
            {
                ":v0": {"S": "123"},
                ":v1": {"N": "123"},
                ":v2": {"BOOL": False},
                ":v3": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                ":v4": {"NS": ["1", "2", "3"]},
                ":v5": {"M": {"a": {"N": "1"}}},
            },
            id="handles suite of types",
        ),
        param(
            {"str with spaces": "123"},
            UpdateExpressionComponents(
                "SET #n0 = :v0",
                {"#n0": "str with spaces"},
                {":v0": "123"},
            ),
            {":v0": {"S": "123"}},
            id="handles attribute with space",
        ),
    ],
)
def test__UpdateExpressionComponents__from_dict(
    attributes: Dict[str, Any],
    expected: UpdateExpressionComponents,
    expected_serialized_values: dict,
):
    actual = UpdateExpressionComponents.from_dict(attributes)
    assert actual == expected
    assert actual.expression_attribute_values__serialized == expected_serialized_values


@mark.parametrize(
    "this, other, expected",
    [
        param(
            ConditionExpressionComponents(
                "#n0 = :v0", {"#n0": "key1"}, {":v0": {"S": "str_value"}}
            ),
            ConditionExpressionComponents(
                "#n1 = :v1", {"#n1": "key2"}, {":v1": {"S": "str_value"}}
            ),
            ConditionExpressionComponents(
                "#n1 = :v1", {"#n1": "key2"}, {":v1": {"S": "str_value"}}
            ),
            id="Simple Expressions with no collisions",
        ),
        param(
            ConditionExpressionComponents(
                "#n0 = :v0", {"#n0": "key1"}, {":v0": {"S": "str_value"}}
            ),
            ConditionExpressionComponents(
                "#n0 = :v0", {"#n0": "key2"}, {":v0": {"S": "str_value"}}
            ),
            ConditionExpressionComponents(
                "#n1 = :v1", {"#n1": "key2"}, {":v1": {"S": "str_value"}}
            ),
            id="Simple Expressions with full collisions",
        ),
        param(
            ConditionExpressionComponents(
                "(#n0 = :v0 AND #n1 = :v1)",
                {"#n0": "key1", "#n1": "key2"},
                {":v0": {"S": "str_value"}, ":v1": {"N": "1.5"}},
            ),
            ConditionExpressionComponents(
                "(#n1 = :x0 AND #n3 = :v1)",
                {"#n1": "key1", "#n3": "key2"},
                {":x0": {"S": "str_value"}, ":v1": {"N": "1.6"}},
            ),
            ConditionExpressionComponents(
                "(#n4 = :x0 AND #n3 = :v2)",
                {"#n4": "key1", "#n3": "key2"},
                {":x0": {"S": "str_value"}, ":v2": {"N": "1.6"}},
            ),
            id="Complex Expressions with partial collisions",
        ),
    ],
)
def test__ExpressionComponents__fix_collisions(
    this: ConditionExpressionComponents,
    other: ConditionExpressionComponents,
    expected: ConditionExpressionComponents,
):
    actual = this.fix_collisions(other)
    assert actual == expected


@mark.parametrize(
    "condition, expression, raises_error",
    [
        param(
            Key("k1").eq("s1"),
            ConditionBaseExpression(
                format="{0} {operator} {1}",
                operator="=",
                values=[AttributeBaseExpression("Key", "k1"), "s1"],
            ),
            does_not_raise(),
            id="KEY.EQ Condition",
        ),
        param(
            Attr("a1").eq(1),
            ConditionBaseExpression(
                format="{0} {operator} {1}",
                operator="=",
                values=[AttributeBaseExpression("Attr", "a1"), 1],
            ),
            does_not_raise(),
            id="ATTR.EQ Condition",
        ),
        param(
            Attr("a1").between("123", "125"),
            ConditionBaseExpression(
                format="{0} {operator} {1} AND {2}",
                operator="BETWEEN",
                values=[AttributeBaseExpression(attr_class="Attr", attr_name="a1"), "123", "125"],
            ),
            does_not_raise(),
            id="ATTR.BETWEEN Condition",
        ),
        param(
            Key("k1").eq("s1") & Attr("a1").lt(1),
            ConditionBaseExpression(
                format="({0} {operator} {1})",
                operator="AND",
                values=[
                    ConditionBaseExpression(
                        format="{0} {operator} {1}",
                        operator="=",
                        values=[AttributeBaseExpression(attr_class="Key", attr_name="k1"), "s1"],
                    ),
                    ConditionBaseExpression(
                        format="{0} {operator} {1}",
                        operator="<",
                        values=[AttributeBaseExpression(attr_class="Attr", attr_name="a1"), 1],
                    ),
                ],
            ),
            does_not_raise(),
            id="KEY.EQ AND ATTR.LT Condition",
        ),
    ],
)
def test__ConditionBaseTranslator__serialize_condition(
    condition: ConditionBase, expression: Optional[ConditionBaseExpression], raises_error
):
    with raises_error:
        actual = ConditionBaseTranslator.serialize_condition(condition)

    if expression:
        assert actual == expression


@mark.parametrize(
    "expression, condition, raises_error",
    [
        param(
            ConditionBaseExpression(
                format="{0} {operator} {1}",
                operator="=",
                values=[AttributeBaseExpression("Key", "k1"), "s1"],
            ),
            Key("k1").eq("s1"),
            does_not_raise(),
            id="KEY.EQ Condition",
        ),
        param(
            "k1=s1",
            Key("k1").eq("s1"),
            does_not_raise(),
            id="KEY.EQ Condition (String)",
        ),
        param(
            ConditionBaseExpression(
                format="{0} {operator} {1}",
                operator="=",
                values=[AttributeBaseExpression("Attr", "a1"), 1],
            ),
            Attr("a1").eq(1),
            does_not_raise(),
            id="ATTR.EQ Condition",
        ),
        param(
            ConditionBaseExpression(
                format="({0} {operator} {1})",
                operator="AND",
                values=[
                    ConditionBaseExpression(
                        format="{0} {operator} {1}",
                        operator="=",
                        values=[AttributeBaseExpression(attr_class="Key", attr_name="k1"), "s1"],
                    ),
                    ConditionBaseExpression(
                        format="{0} {operator} {1}",
                        operator="<",
                        values=[AttributeBaseExpression(attr_class="Attr", attr_name="a1"), 1],
                    ),
                ],
            ),
            Key("k1").eq("s1") & Attr("a1").lt(1),
            does_not_raise(),
            id="KEY.EQ AND ATTR.LT Condition",
        ),
    ],
)
def test__ConditionBaseTranslator__deserialize_condition(
    expression: Union[str, ConditionBaseExpression],
    condition: Optional[ConditionBase],
    raises_error,
):
    with raises_error:
        actual = ConditionBaseTranslator.deserialize_condition(expression)

    if condition:
        assert actual == condition
