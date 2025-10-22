import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Match, Optional, Union, cast

from aibs_informatics_core.collections import ValidatedStr
from aibs_informatics_core.models.aws.dynamodb import (
    AttributeBaseExpression,
    ConditionBaseExpression,
    ConditionBaseExpressionString,
)
from aibs_informatics_core.utils.decorators import cached_property
from aibs_informatics_core.utils.logging import get_logger
from aibs_informatics_core.utils.modules import get_all_subclasses
from boto3.dynamodb.conditions import (
    Attr,
    AttributeBase,
    ConditionBase,
    ConditionExpressionBuilder,
    Key,
)
from boto3.dynamodb.types import TypeSerializer

logger = get_logger(__name__)


def condition_to_str(condition: Optional[ConditionBase]) -> Optional[str]:
    """Converts a ConditionBase Boto3 object to its str representation

    NOTE: Function should be removed if this PR is merged: https://github.com/boto/boto3/pull/3254

    Examples:

    >>> condition_to_str(Key("name").eq("new_name") & Attr("description").begins_with("new"))
    (name = new_name AND begins_with(description, new))

    >>> condition_to_str(Attr("description").contains("cool"))
    contains(description, cool)

    >>> condition_to_str(None)
    None
    """
    if condition is None:
        return None

    builder = ConditionExpressionBuilder()
    expression = builder.build_expression(condition)

    condition_expression = expression.condition_expression

    for name_placeholder, actual_name in expression.attribute_name_placeholders.items():
        condition_expression = condition_expression.replace(name_placeholder, str(actual_name))

    for value_placeholder, actual_value in expression.attribute_value_placeholders.items():
        condition_expression = condition_expression.replace(value_placeholder, str(actual_value))

    return condition_expression


@dataclass
class ExpressionComponentsBase:
    expression: str
    expression_attribute_names: Dict[str, str]
    expression_attribute_values: Dict[str, Any]

    @cached_property
    def expression_attribute_values__serialized(self) -> Dict[str, Dict[str, Any]]:
        serializer = TypeSerializer()
        return {
            k: cast(Dict[str, Any], serializer.serialize(v))
            for k, v in self.expression_attribute_values.items()
        }


@dataclass
class UpdateExpressionComponents(ExpressionComponentsBase):
    @property
    def update_expression(self) -> str:
        return self.expression

    @classmethod
    def from_dict(cls, attributes: Mapping[str, Any]) -> "UpdateExpressionComponents":
        expression_attr_names = {}
        expression_attr_values = {}
        update_expressions = []
        for i, (attr_name, attr_value) in enumerate(attributes.items()):
            attr_name_key = f"#n{i}"
            attr_value_key = f":v{i}"
            expression_attr_names[attr_name_key] = attr_name
            expression_attr_values[attr_value_key] = attr_value
            update_expressions.append(f"{attr_name_key} = {attr_value_key}")
        update_expression = "SET " + ", ".join(update_expressions)
        return UpdateExpressionComponents(
            expression=update_expression,
            expression_attribute_names=expression_attr_names,
            expression_attribute_values=expression_attr_values,
        )


@dataclass
class ConditionExpressionComponents(ExpressionComponentsBase):
    @property
    def condition_expression(self) -> str:
        return self.expression

    @classmethod
    def from_condition(
        cls, condition: ConditionBase, is_key_condition: bool
    ) -> "ConditionExpressionComponents":
        """Create ConditionExpressionComponents from a ConditionBase

        Args:
            condition (ConditionBase): The ConditionBase to create expression components from
            is_key_condition (bool): If the provided condition is for a query()
                KeyConditionExpression then this should be True. Otherwise if the condition is for
                a query() FilterExpression or for a put_item() ConditionExpression then this
                should be set to false.

        Returns:
            ConditionExpressionComponents
        """
        builder = ConditionExpressionBuilder()
        bce = builder.build_expression(condition, is_key_condition=is_key_condition)

        return ConditionExpressionComponents(
            expression=bce.condition_expression,
            expression_attribute_names=cast(Dict[str, str], bce.attribute_name_placeholders),
            expression_attribute_values=cast(Dict[str, Any], bce.attribute_value_placeholders),
        )

    def fix_collisions(
        self, other: "ConditionExpressionComponents"
    ) -> "ConditionExpressionComponents":
        """Modify other expression components such that no attribute name/values collide

        For example, given the following Expression components
            this:   ConditionExpressionComponents("#n0 = :v0", {"#n0": "key1"}, {":v0": {"S": "str_value"}})
            other:  ConditionExpressionComponents("#n0 = :v1", {"#n0": "key2"}, {":v1": {"S": "str_value"}})

        There is a collision of the attribute name '#n0' which would get converted to '#n1'
            output: ConditionExpressionComponents("#n1 = :v1", {"#n1": "key2"}, {":v1": {"S": "str_value"}})

        Args:
            other (ConditionExpressionComponents): the other Expression Condition to modify

        Returns:
            ConditionExpressionComponents: The other, as a modified non-overlapping Expression Condition
        """  # noqa: E501

        this_placeholder_names = AttrPlaceholder.sorted(self.expression_attribute_names.keys())
        this_placeholder_values = AttrPlaceholder.sorted(self.expression_attribute_values.keys())

        other_placeholder_names = AttrPlaceholder.sorted(other.expression_attribute_names.keys())
        other_placeholder_values = AttrPlaceholder.sorted(other.expression_attribute_values.keys())

        prefix_max_num_map = AttrPlaceholder.build_prefix_max_number_map(
            *this_placeholder_names,
            *this_placeholder_values,
            *other_placeholder_names,
            *other_placeholder_values,
        )

        # Colliding placeholders
        intersect_names = set(this_placeholder_names).intersection(other_placeholder_names)
        intersect_values = set(this_placeholder_values).intersection(other_placeholder_values)

        # create mapping for placeholder updates
        update_map: Dict[AttrPlaceholder, AttrPlaceholder] = {}

        # Now create new placeholders for colliding placeholders,
        # and add to the placeholder update mapping.
        for placeholder in intersect_names.union(intersect_values):
            prefix_max_num_map[placeholder.prefix] += 1
            update_map[placeholder] = AttrPlaceholder.from_components(
                placeholder.prefix, prefix_max_num_map[placeholder.prefix]
            )

        def _fetch(m: Match) -> AttrPlaceholder:
            ap = AttrPlaceholder(m.group(0))
            return update_map.get(ap, ap)

        # Now build the new expression components object
        return ConditionExpressionComponents(
            expression=re.sub(AttrPlaceholder.regex_pattern, _fetch, other.condition_expression),
            expression_attribute_names={
                update_map.get(AttrPlaceholder(p), p): other.expression_attribute_names[p]
                for p in other.expression_attribute_names
            },
            expression_attribute_values={
                update_map.get(AttrPlaceholder(p), p): other.expression_attribute_values[p]
                for p in other.expression_attribute_values
            },
        )


class AttrPlaceholder(ValidatedStr):
    regex_pattern = re.compile(r"([#:][a-zA-Z])([\d]+)")

    @property
    def prefix(self) -> str:
        return self.get_match_groups()[0]

    @property
    def number(self) -> int:
        return int(self.get_match_groups()[1])

    @classmethod
    def from_components(cls, prefix: str, number: int) -> "AttrPlaceholder":
        return cls(f"{prefix}{number}")

    @classmethod
    def sorted(
        cls, placeholders: Iterable[Union[str, "AttrPlaceholder"]]
    ) -> List["AttrPlaceholder"]:
        return sorted(map(AttrPlaceholder, placeholders), key=lambda _: _.number)

    @classmethod
    def build_prefix_max_number_map(cls, *placeholders: "AttrPlaceholder") -> Dict[str, int]:
        map: Dict[str, int] = defaultdict(int)
        for placeholder in placeholders:
            if map[placeholder.prefix] < placeholder.number:
                map[placeholder.prefix] = placeholder.number
        return map


class ConditionBaseTranslator:
    _ATTRIBUTE_BASE_CLASS_LOOKUP = {_.__name__: _ for _ in get_all_subclasses(AttributeBase)}
    _CONDITION_BASE_CLASS_LOOKUP = {
        (_.expression_format, _.expression_operator): _ for _ in get_all_subclasses(ConditionBase)
    }

    @classmethod
    def deserialize_attribute(
        cls, attribute_expression: AttributeBaseExpression
    ) -> Union[Key, Attr, AttributeBase]:
        return cls._ATTRIBUTE_BASE_CLASS_LOOKUP[attribute_expression.attr_class](
            attribute_expression.attr_name
        )

    @classmethod
    def deserialize_condition(
        cls, condition_expression: Union[str, ConditionBaseExpression], is_key: bool = True
    ) -> ConditionBase:
        """Convert ConditionBase Expression into ConditionBase

        Args:
            condition_expression (str|ConditionBaseExpression): The expression to convert.
            is_key (bool, optional): Used to infer attribute name if expression is string.
                Defaults to True.

        Returns:
            ConditionBase
        """

        def _deserialize_condition(ce: ConditionBaseExpression) -> ConditionBase:
            ce_key = (ce.format, ce.operator)
            condition_base_cls = cls._CONDITION_BASE_CLASS_LOOKUP[ce_key]
            ce_values: list[Union[AttributeBase, ConditionBase]] = []
            for ce_value in ce.values:
                if isinstance(ce_value, ConditionBaseExpression):
                    ce_values.append(_deserialize_condition(ce_value))
                elif isinstance(ce_value, AttributeBaseExpression):
                    ce_values.append(cls.deserialize_attribute(ce_value))
                else:
                    ce_values.append(ce_value)
            return condition_base_cls(*ce_values)

        if isinstance(condition_expression, str):
            condition_string = ConditionBaseExpressionString(condition_expression)
            condition_expression = condition_string.get_condition_expression(is_key=is_key)
        return _deserialize_condition(condition_expression)

    @classmethod
    def serialize_attribute(cls, attribute: AttributeBase) -> AttributeBaseExpression:
        return AttributeBaseExpression(attribute.__class__.__name__, attribute.name)

    @classmethod
    def serialize_condition(cls, condition: ConditionBase) -> ConditionBaseExpression:
        raw_expression = condition.get_expression()
        return ConditionBaseExpression(
            format=raw_expression["format"],
            operator=raw_expression["operator"],
            values=[
                cls.serialize_condition(value)
                if isinstance(value, ConditionBase)
                else (
                    cls.serialize_attribute(value) if isinstance(value, (AttributeBase)) else value
                )
                for value in raw_expression["values"]
            ],
        )

    @classmethod
    def _get_condition_base_operators(cls) -> List[str]:
        return [
            _.expression_operator
            for _ in ConditionBaseTranslator._CONDITION_BASE_CLASS_LOOKUP.values()
        ]
