from copy import deepcopy
from decimal import Decimal
from operator import itemgetter
from test.aibs_informatics_aws_utils.base import AwsBaseTest
from typing import Any, Dict, List

import boto3
import moto
import pytest
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.dynamodb import (
    get_dynamodb_client,
    get_dynamodb_resource,
    table_as_resource,
    table_get_item,
    table_get_key_schema,
    table_put_item,
)

# Assuming the two functions are in a module named 'module_name'
from aibs_informatics_aws_utils.dynamodb.functions import (
    convert_decimals_to_floats,
    convert_floats_to_decimals,
)


class DynamoDBTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_base_env_var()
        self.set_region(self.DEFAULT_REGION)
        self.DEFAULT_TABLE_NAME = "a-random-table"
        self.DEFAULT_KEY_SCHEMA = [{"AttributeName": "key", "KeyType": "HASH"}]
        self.DEFAULT_ATTR_DEFS = [{"AttributeName": "key", "AttributeType": "S"}]
        self.DEFAULT_PROV_THROUGHPUT = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}

    def setUpTable(
        self,
        table_name: str = None,
        key_schema: List[Dict[str, str]] = None,
        attr_definitions: List[Dict[str, str]] = None,
        **kwargs,
    ) -> str:
        table_name = table_name or self.DEFAULT_TABLE_NAME
        self.ddb.create_table(
            TableName=table_name,
            KeySchema=key_schema or self.DEFAULT_KEY_SCHEMA,
            AttributeDefinitions=attr_definitions or self.DEFAULT_ATTR_DEFS,
            ProvisionedThroughput=self.DEFAULT_PROV_THROUGHPUT,
            **kwargs,
        )
        return table_name

    @property
    def ddb(self):
        return get_dynamodb_client(region=self.DEFAULT_REGION)

    @property
    def ddb_resource(self):
        return get_dynamodb_resource(region=self.DEFAULT_REGION)

    def test__table_as_resource__works(self):
        with moto.mock_aws():
            self.setUpTable()
            table = table_as_resource(self.DEFAULT_TABLE_NAME)
            self.assertEqual(table.table_name, self.DEFAULT_TABLE_NAME)

    def test__table_get_key_schema__returns_key_schema(self):
        with moto.mock_aws():
            self.setUpTable()
            key_schema = table_get_key_schema(self.DEFAULT_TABLE_NAME)
            self.assertEqual(key_schema, {"HASH": "key"})

    def test__table_put_item__does_not_put_on_condition(self):
        with moto.mock_aws():
            table_name = self.setUpTable()
            table_put_item(table_name, {"key": "k1", "my_attr": False})
            self.assertDictEqual(
                table_get_item(table_name, {"key": "k1"}), {"key": "k1", "my_attr": False}
            )
            table_put_item(table_name, {"key": "k1", "my_attr": True})
            self.assertDictEqual(
                table_get_item(table_name, {"key": "k1"}), {"key": "k1", "my_attr": True}
            )
            with self.assertRaises(ClientError):
                table_put_item(
                    table_name,
                    {"key": "k1", "my_attr": True},
                    condition_expression=Attr("my_attr").not_exists(),
                )

            table_put_item(
                table_name,
                {"key": "k1", "my_attr": True},
                condition_expression=Attr("my_attr").eq(True),
            )
            self.assertDictEqual(
                table_get_item(table_name, {"key": "k1"}), {"key": "k1", "my_attr": True}
            )


@pytest.fixture(scope="function")
def mock_dynamodb_fixture(aws_credentials_fixture, request):
    default_attr_def = [
        {"AttributeName": "PK", "AttributeType": "S"},
        {"AttributeName": "SK", "AttributeType": "S"},
    ]
    default_key_schema = [
        {"AttributeName": "PK", "KeyType": "HASH"},
        {"AttributeName": "SK", "KeyType": "RANGE"},
    ]
    default_items = [{"PK": "pk_value_1", "SK": "sk_value_1", "ATTR1": "attr_value_1"}]
    default_index_updates = [
        {
            "Create": {
                "IndexName": "MockGSI",
                "KeySchema": [{"AttributeName": "ATTR1", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        }
    ]

    attribute_defs = request.param.get("attribute_definitions", default_attr_def)
    key_schema = request.param.get("key_schema", default_key_schema)
    table_default_items = request.param.get("table_default_items", default_items)
    index_updates = request.param.get("table_index_updates", default_index_updates)

    with moto.mock_aws():
        mock_table_name = "mock-table"
        mock_client = boto3.client("dynamodb")
        mock_client.create_table(
            AttributeDefinitions=attribute_defs,
            TableName=mock_table_name,
            KeySchema=key_schema,
            BillingMode="PAY_PER_REQUEST",
        )

        if index_updates:
            _ = mock_client.update_table(
                TableName=mock_table_name, GlobalSecondaryIndexUpdates=index_updates
            )

        mock_table = boto3.resource("dynamodb").Table(mock_table_name)
        for i in table_default_items:
            mock_table.put_item(Item=i)

        yield mock_table_name


@pytest.mark.parametrize(
    "mock_dynamodb_fixture, key_condition_expression, index_name, filter_expression, expected",
    [
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ],
            },
            # key_condition_expression
            Key("PK").eq("d"),
            # index_name
            None,
            # filter_expression
            None,
            # expected
            [{"PK": "d", "SK": "e", "ATTR1": "f"}],
            # id
            id="Query - no index - no filter expression",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ],
            },
            # key_condition_expression
            Key("PK").eq("a"),
            # index_name
            None,
            # filter_expression
            None,
            # expected
            [
                {"PK": "a", "SK": "b", "ATTR1": "c"},
                {"PK": "a", "SK": "g", "ATTR1": "h"},
            ],
            # id
            id="Query - no index - no filter expression - returns multiple items",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ],
            },
            # key_condition_expression
            Key("PK").eq("a"),
            # index_name
            None,
            # filter_expression
            Attr("ATTR1").eq("h"),
            # expected
            [{"PK": "a", "SK": "g", "ATTR1": "h"}],
            # id
            id="Query - no index - basic filter expression",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ],
            },
            # key_condition_expression
            Key("PK").eq("a"),
            # index_name
            None,
            # filter_expression
            Attr("ATTR1").eq("f"),
            # expected
            [],
            # id
            id="Query - no index - basic filter expression - returns empty",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ]
            },
            # key_condition_expression
            Key("ATTR1").eq("c"),
            # index_name
            "MockGSI",
            # filter_expression
            None,
            # expected
            [{"PK": "a", "SK": "b", "ATTR1": "c"}],
            # id
            id="Query - with index - no filter expression",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "c"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ]
            },
            # key_condition_expression
            Key("ATTR1").eq("c"),
            # index_name
            "MockGSI",
            # filter_expression
            Attr("PK").eq("d"),
            # expected
            [{"PK": "d", "SK": "e", "ATTR1": "c"}],
            # id
            id="Query - with index - basic filter expression",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "c"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ]
            },
            # key_condition_expression
            Key("ATTR1").eq("c"),
            # index_name
            "MockGSI",
            # filter_expression
            Attr("PK").eq("f"),
            # expected
            [],
            # id
            id="Query - with index - basic filter expression - returns empty",
        ),
    ],
    indirect=["mock_dynamodb_fixture"],
)
def test__table_query(
    mock_dynamodb_fixture, key_condition_expression, index_name, filter_expression, expected
):
    mock_table_name = mock_dynamodb_fixture

    # Follows best practices outlined in:
    # http://docs.getmoto.org/en/latest/docs/getting_started.html#what-about-those-pesky-imports
    from aibs_informatics_aws_utils.dynamodb import table_query

    actual_result = table_query(
        table_name=mock_table_name,
        key_condition_expression=key_condition_expression,
        index_name=index_name,
        filter_expression=filter_expression,
    )
    assert expected == actual_result


@pytest.mark.parametrize(
    "mock_dynamodb_fixture, index_name, filter_expression, expected",
    [
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ],
            },
            # index_name
            None,
            # filter_expression
            None,
            # expected
            [
                {"PK": "a", "SK": "b", "ATTR1": "c"},
                {"PK": "d", "SK": "e", "ATTR1": "f"},
                {"PK": "a", "SK": "g", "ATTR1": "h"},
            ],
            # id
            id="Scan - no index - no filter expression",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ],
            },
            # index_name
            None,
            # filter_expression
            Attr("PK").eq("d"),
            # expected
            [{"PK": "d", "SK": "e", "ATTR1": "f"}],
            # id
            id="Scan - no index - filter expression on PK",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ],
            },
            # index_name
            None,
            # filter_expression
            Attr("ATTR1").eq("h"),
            # expected
            [{"PK": "a", "SK": "g", "ATTR1": "h"}],
            # id
            id="Scan - no index - filter expression on ATTR",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ]
            },
            # index_name
            "MockGSI",
            # filter_expression
            None,
            # expected
            [
                {"PK": "a", "SK": "b", "ATTR1": "c"},
                {"PK": "d", "SK": "e", "ATTR1": "f"},
                {"PK": "a", "SK": "g", "ATTR1": "h"},
            ],
            # id
            id="Scan - with index - no filter expression",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                ]
            },
            # index_name
            "MockGSI",
            # filter_expression
            Attr("SK").eq("e"),
            # expected
            [{"PK": "d", "SK": "e", "ATTR1": "f"}],
            # id
            id="Scan - with index - basic filter expression",
        ),
    ],
    indirect=["mock_dynamodb_fixture"],
)
def test__table_scan(mock_dynamodb_fixture, index_name, filter_expression, expected):
    mock_table_name = mock_dynamodb_fixture

    # Follows best practices outlined in:
    # http://docs.getmoto.org/en/latest/docs/getting_started.html#what-about-those-pesky-imports
    from aibs_informatics_aws_utils.dynamodb import table_scan

    actual_result = table_scan(
        table_name=mock_table_name, index_name=index_name, filter_expression=filter_expression
    )
    assert sorted(expected, key=itemgetter("PK")) == sorted(actual_result, key=itemgetter("PK"))


@pytest.mark.parametrize(
    "mock_dynamodb_fixture, keys, attrs, expected",
    [
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                    {"PK": "i", "SK": "j", "ATTR1": "k"},
                ],
            },
            # keys
            [
                # Must provide both PK and SK for get_item and batch_get_item
                {"PK": "d", "SK": "e"},
                {"PK": "i", "SK": "j"},
            ],
            # attrs
            None,
            # expected
            [
                {"PK": "d", "SK": "e", "ATTR1": "f"},
                {"PK": "i", "SK": "j", "ATTR1": "k"},
            ],
            # id
            id="Get_items - basic",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": "a", "SK": "b", "ATTR1": "c"},
                    {"PK": "d", "SK": "e", "ATTR1": "f"},
                    {"PK": "a", "SK": "g", "ATTR1": "h"},
                    {"PK": "i", "SK": "j", "ATTR1": "k"},
                ],
            },
            # keys
            [
                # Must provide both PK and SK for get_item and batch_get_item
                {"PK": "d", "SK": "e"},
                {"PK": "i", "SK": "z"},
            ],
            # attrs
            None,
            # expected
            [{"PK": "d", "SK": "e", "ATTR1": "f"}],
            # id
            id="Get_items - one key doesn't return a result",
        ),
        pytest.param(
            # mock_dynamodb_fixture
            {
                "table_index_updates": [],
                "table_default_items": [
                    {"PK": str(i), "SK": str(i + 1), "ATTR1": str(i)} for i in range(500)
                ],
            },
            # keys
            [{"PK": str(i), "SK": str(i + 1)} for i in range(250)],
            # attrs
            None,
            # expected
            [{"PK": str(i), "SK": str(i + 1), "ATTR1": str(i)} for i in range(250)],
            # id
            id="Get_items - handles overflow",
        ),
    ],
    indirect=["mock_dynamodb_fixture"],
)
def test__table_get_items(mock_dynamodb_fixture, keys, attrs, expected):
    mock_table_name = mock_dynamodb_fixture

    # Follows best practices outlined in:
    # http://docs.getmoto.org/en/latest/docs/getting_started.html#what-about-those-pesky-imports
    from aibs_informatics_aws_utils.dynamodb import table_get_items

    actual_result = table_get_items(table_name=mock_table_name, keys=keys, attrs=attrs)
    print(actual_result)
    assert sorted(expected, key=itemgetter("PK")) == sorted(actual_result, key=itemgetter("PK"))


def test__convert_decimals_to_floats__default_args():
    item = {"a": Decimal("10.5"), "b": {"c": Decimal("20.5")}, "d": [Decimal("30.5")]}
    expected = {"a": 10.5, "b": {"c": 20.5}, "d": [30.5]}
    actual = convert_decimals_to_floats(item)
    assert actual == expected
    assert item is actual


def test__convert_floats_to_decimals__default_args():
    item = {"a": 10.5, "b": {"c": 20.5}, "d": [30.5]}
    expected = {"a": Decimal("10.5"), "b": {"c": Decimal("20.5")}, "d": [Decimal("30.5")]}
    actual = convert_floats_to_decimals(item)
    assert actual == expected
    assert item is actual


def test__convert_decimals_to_floats__not_in_place():
    item = {"a": Decimal("10.5"), "b": {"c": Decimal("20.5")}, "d": [Decimal("30.5")]}
    expected = deepcopy(item)
    actual = convert_decimals_to_floats(expected, in_place=False)
    assert actual == expected
    assert item is not actual


def test__convert_floats_to_decimals__not_in_place():
    item = {"a": 10.5, "b": {"c": 20.5}, "d": [30.5]}
    expected = deepcopy(item)
    actual = convert_floats_to_decimals(expected, in_place=False)
    assert actual == expected
    assert item is not actual


def test__convert_decimals_to_floats__deeply_nested():
    item = {"a": {"b": {"c": {"d": Decimal("10.5")}}}}
    expected = {"a": {"b": {"c": {"d": 10.5}}}}
    actual = convert_decimals_to_floats(item)
    assert actual == expected
    assert item is actual


def test__convert_floats_to_decimals__deeply_nested():
    item = {"a": {"b": {"c": {"d": 10.5}}}}
    expected = {"a": {"b": {"c": {"d": Decimal("10.5")}}}}
    assert convert_floats_to_decimals(item) == expected


def test__convert_decimals_to_floats__handles_multiple_calls():
    item = {"a": Decimal("10.5")}
    expected = {"a": 10.5}
    actual = convert_decimals_to_floats(item)
    assert actual == expected
    assert item is actual
    actual = convert_decimals_to_floats(actual)
    assert actual == expected
    assert item is actual


def test__convert_floats_to_decimals__handles_multiple_calls():
    item = {"a": 10.5}
    expected = {"a": Decimal("10.5")}
    actual = convert_floats_to_decimals(item)
    assert actual == expected
    assert item is actual
    actual = convert_floats_to_decimals(actual)
    assert actual == expected
    assert item is actual
