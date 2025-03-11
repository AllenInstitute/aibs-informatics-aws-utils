from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

import moto
import pytest
from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.base import (
    CustomAwareDateTime,
    FloatField,
    IntegerField,
    StringField,
    custom_field,
)
from aibs_informatics_core.models.db import (
    DBIndex,
    DBIndexNameEnum,
    DBKeyNameEnum,
    DBModel,
    DBSortKeyNameEnum,
)
from aibs_informatics_core.utils.time import from_isoformat_8601
from aibs_informatics_test_resources import does_not_raise

from aibs_informatics_aws_utils.dynamodb.conditions import Attr, Key
from aibs_informatics_aws_utils.dynamodb.functions import (
    get_dynamodb_client,
    get_dynamodb_resource,
)
from aibs_informatics_aws_utils.dynamodb.table import (
    DynamoDBEnvBaseTable,
    build_optimized_condition_expression_set,
    check_index_supports_strongly_consistent_read,
    check_table_name_and_index_match,
)
from aibs_informatics_aws_utils.exceptions import (
    DBQueryException,
    DBReadException,
    DBWriteException,
    EmptyQueryResultException,
    NonUniqueQueryResultException,
)
from test.aibs_informatics_aws_utils.base import AwsBaseTest

SIMPLE_TABLE_NAME = "simple-table"


class SimpleKeyName(DBKeyNameEnum):
    PRIMARY_KEY = "primary_key"
    STR_ATTR = "str_attr"


class SimpleSortKeyName(DBSortKeyNameEnum):
    DATETIME_ATTR = "datetime_attr"
    INT_ATTR = "int_attr"


class SimpleIndexName(DBIndexNameEnum):
    STR_DT_INDEX = DBIndexNameEnum.from_name_and_key(
        SIMPLE_TABLE_NAME, key=SimpleKeyName.STR_ATTR, sort_key=SimpleSortKeyName.DATETIME_ATTR
    )
    STR_INT_INDEX = DBIndexNameEnum.from_name_and_key(
        SIMPLE_TABLE_NAME, key=SimpleKeyName.STR_ATTR, sort_key=SimpleSortKeyName.INT_ATTR
    )


class SimpleIndex(DBIndex):
    PRIMARY_KEY = (
        SimpleKeyName.PRIMARY_KEY,
        SimpleKeyName.PRIMARY_KEY,
        None,
        None,
    )
    STR_DT_INDEX = (
        SimpleIndexName.STR_DT_INDEX,
        SimpleKeyName.STR_ATTR,
        SimpleSortKeyName.DATETIME_ATTR,
        SimpleIndexName.STR_DT_INDEX,
    )
    STR_INT_INDEX = (
        SimpleIndexName.STR_INT_INDEX,
        SimpleKeyName.STR_ATTR,
        SimpleSortKeyName.INT_ATTR,
        SimpleIndexName.STR_INT_INDEX,
        ["primary_key", "int_attr"],
    )

    @classmethod
    def table_name(cls) -> str:
        return SIMPLE_TABLE_NAME


@dataclass
class SimpleModel(DBModel):
    primary_key: str = custom_field(mm_field=StringField())
    str_attr: str = custom_field(mm_field=StringField())
    int_attr: int = custom_field(mm_field=IntegerField())
    float_attr: float = custom_field(mm_field=FloatField())
    datetime_attr: datetime = custom_field(mm_field=CustomAwareDateTime(format="iso8601"))


@dataclass
class SimpleTable(DynamoDBEnvBaseTable[SimpleModel, SimpleIndex]):
    pass


def test__check_table_name_and_index_match__fails_for_mismatch():
    with pytest.raises(DBQueryException):
        check_table_name_and_index_match("does-not-match", SimpleIndex)


def test__check_index_supports_strongly_consistent_read__works():
    check_index_supports_strongly_consistent_read(SimpleIndex.PRIMARY_KEY)
    with pytest.raises(DBQueryException):
        check_index_supports_strongly_consistent_read(SimpleIndex.STR_DT_INDEX)


@moto.mock_aws
class SimpleTableTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_base_env_var()
        self.set_region(self.DEFAULT_REGION)
        self.table = self.setUpTable()

    def setUpTable(self, env_base: Optional[EnvBase] = None) -> SimpleTable:
        table = SimpleTable(env_base or self.env_base)

        self.ddb.create_table(
            TableName=table.table_name,
            KeySchema=[
                {"AttributeName": SimpleKeyName.PRIMARY_KEY, "KeyType": "HASH"},  # Primary key
            ],
            AttributeDefinitions=[
                {"AttributeName": SimpleKeyName.PRIMARY_KEY, "AttributeType": "S"},
                {"AttributeName": SimpleKeyName.STR_ATTR, "AttributeType": "S"},
                {"AttributeName": SimpleSortKeyName.DATETIME_ATTR, "AttributeType": "S"},
                {"AttributeName": SimpleSortKeyName.INT_ATTR, "AttributeType": "N"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            GlobalSecondaryIndexes=[
                {
                    "IndexName": self.env_base.prefixed(SimpleIndexName.STR_DT_INDEX),
                    "KeySchema": [
                        {"AttributeName": SimpleKeyName.STR_ATTR, "KeyType": "HASH"},
                        {"AttributeName": SimpleSortKeyName.DATETIME_ATTR, "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "ALL",
                    },
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                },
                {
                    "IndexName": self.env_base.prefixed(SimpleIndexName.STR_INT_INDEX),
                    "KeySchema": [
                        {"AttributeName": SimpleKeyName.STR_ATTR, "KeyType": "HASH"},
                        {"AttributeName": SimpleSortKeyName.INT_ATTR, "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": ["primary_key", "int_attr"],
                    },
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                },
            ],
        )

        return table

    @property
    def ddb(self):
        return get_dynamodb_client(region=self.DEFAULT_REGION)

    @property
    def ddb_resource(self):
        return get_dynamodb_resource(region=self.DEFAULT_REGION)

    def test__table_name__works(self):
        self.assertEqual(self.table.table_name, f"{self.env_base}-{SIMPLE_TABLE_NAME}")

    def test__put__get__works(self):
        entry = self.create_entry()
        self.table.put(entry)
        new_entry = self.table.get(entry.primary_key)
        self.assertEqual(entry, new_entry)

    def test__put__batch_get__works(self):
        entry1 = self.create_entry(primary_key="primary_key1", str_attr="str_attr1")
        entry2 = self.create_entry(primary_key="primary_key2", str_attr="str_attr2")
        self.table.put(entry1)
        self.table.put(entry2)
        new_entries = self.table.batch_get([entry1.primary_key, entry2.primary_key])
        self.assertEqual(new_entries, [entry1, entry2])

    def test__put__does_not_put_on_condition(self):
        entry1 = self.create_entry(primary_key="primary_key", str_attr="str_attr1")
        entry2 = self.create_entry(primary_key="primary_key", str_attr="str_attr2")
        self.table.put(entry1, condition_expression=Attr("primary_key").not_exists())
        with self.assertRaises(DBWriteException):
            self.table.put(entry2, condition_expression=Attr("primary_key").not_exists())

    def test__update__works(self):
        e1 = self.create_entry(str_attr="str_attr1")
        e2 = self.create_entry(str_attr="str_attr2")
        self.table.put(e1)

        key = self.table.build_key_from_entry(e1)
        # updates all fields for full entry
        e3 = self.table.update(key, e2, old_entry=e1)
        self.assertEqual(e3, e2)
        # updates all fields for full dict
        e4 = self.table.update(key, e1.to_dict())
        self.assertEqual(e4, e1)
        # updates only specified fields from partial entry
        e5 = self.table.update(key, SimpleModel.from_dict({"int_attr": 4}, partial=True))
        self.assertEqual(e5.int_attr, 4)
        # updates only specified fields from dict
        e6 = self.table.update(key, {"int_attr": 3})
        self.assertEqual(e6.int_attr, 3)
        # does not update, fetches old entry
        e7 = self.table.update(key, {})
        self.assertEqual(e6, e7)

    def test__update__does_not_update_on_condition(self):
        entry1 = self.create_entry(primary_key="primary_key", str_attr="str_attr1")
        entry2 = self.create_entry(primary_key="primary_key", str_attr="str_attr2")
        self.table.put(entry1, condition_expression=Attr("primary_key").not_exists())
        key = self.table.build_key_from_entry(entry1)
        self.table.update(key, entry2)
        with self.assertRaises(DBWriteException):
            self.table.update(key, entry2, ConditionExpression=Attr("primary_key").not_exists())

    def test__get__fails_for_missing_value(self):
        with self.assertRaises(DBReadException):
            self.table.get("primary_key")

    def test__batch_get__fails_for_some_missing_values(self):
        entry = self.create_entry(primary_key="primary_key1", str_attr="str_attr1")
        self.table.put(entry)
        with self.assertRaises(DBReadException):
            self.table.batch_get(["primary_key1", "primary_key2"])

    def test__query__expect_non_empty__expect_unique__work_as_intented(self):
        [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(3)
        ]
        # expect_non_empty tests
        # expect_non_empty=False
        actual = self.table.query(
            index=SimpleIndex.STR_INT_INDEX,
            partition_key="str_attr",
            sort_key_condition_expression=Key("int_attr").between(-1, -1),
            filters=[Attr("float_attr").eq(Decimal("-1.0"))],
            expect_non_empty=False,
            expect_unique=False,
        )
        self.assertEqual(len(actual), 0)

        # expect_non_empty=True
        with self.assertRaises(EmptyQueryResultException):
            self.table.query(
                index=SimpleIndex.STR_INT_INDEX,
                partition_key="str_attr",
                sort_key_condition_expression=Key("int_attr").between(-1, -1),
                filters=[Attr("float_attr").eq(Decimal(-1.0))],
                expect_non_empty=True,
                expect_unique=False,
            )

        # expect_non_empty tests
        # expect_non_empty=False
        actual = self.table.query(
            index=SimpleIndex.STR_INT_INDEX,
            partition_key="str_attr",
            sort_key_condition_expression=Key("int_attr").between(0, 1),
            filters=[Attr("float_attr").ne(Decimal("0.0"))],
            expect_non_empty=False,
            expect_unique=False,
        )
        self.assertEqual(len(actual), 2)

        # expect_non_empty=True
        with self.assertRaises(NonUniqueQueryResultException):
            self.table.query(
                index=SimpleIndex.STR_INT_INDEX,
                partition_key="str_attr",
                sort_key_condition_expression=Key("int_attr").between(0, 1),
                filters=[Attr("float_attr").ne(Decimal("0.0"))],
                expect_non_empty=False,
                expect_unique=True,
            )

    def test__query__fills_in_values_for_partial_gsi(self):
        entries = [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(10)
        ]
        expected = entries[:5]
        actual = self.table.query(
            index=SimpleIndex.STR_INT_INDEX,
            partition_key="str_attr",
            sort_key_condition_expression=Key("int_attr").between(0, 4),
        )
        self.assertListEqual(expected, actual)

    def test__query__handles_no_sort_key(self):
        entries = [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(10)
        ]
        actual = self.table.query(
            index=SimpleIndex.STR_INT_INDEX,
            partition_key=Key("str_attr").eq("str_attr"),
        )
        self.assertListEqual(entries, actual)

    def test__query__fails_for_incorrect_sort_key(self):
        with self.assertRaises(DBQueryException):
            self.table.query(
                index=SimpleIndex.STR_INT_INDEX,
                partition_key="str_attr",
                sort_key_condition_expression=Key("dt_attr").between(0, 4),
            )

    def test__query__consistent_read__works(self):
        with self.assertRaises(DBQueryException):
            self.table.query(
                index=SimpleIndex.STR_INT_INDEX,
                partition_key="str_attr",
                sort_key_condition_expression=Key("int_attr").between(0, 4),
                consistent_read=True,
            )

    def test__scan__no_filters_returns_all(self):
        entries = [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(3)
        ]
        actual = self.table.scan()
        self.assertEqual(len(entries), len(actual))

    def test__scan__expect_non_empty__expect_unique__work_as_intented(self):
        [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(3)
        ]
        # expect_non_empty tests
        # expect_non_empty=False
        actual = self.table.scan(
            filters=[Attr("int_attr").between(-1, -1)],
            expect_non_empty=False,
            expect_unique=False,
        )
        self.assertEqual(len(actual), 0)

        # expect_non_empty=True
        with self.assertRaises(EmptyQueryResultException):
            self.table.scan(
                index=SimpleIndex.STR_INT_INDEX,
                filters=[Attr("int_attr").between(-1, -1)],
                expect_non_empty=True,
                expect_unique=False,
            )

        # expect_non_empty tests
        # expect_non_empty=False
        actual = self.table.scan(
            filters=[Attr("int_attr").between(0, 2)],
            expect_non_empty=False,
            expect_unique=False,
        )
        self.assertEqual(len(actual), 2)

        # expect_non_empty=True
        with self.assertRaises(NonUniqueQueryResultException):
            self.table.scan(
                filters=[Attr("int_attr").between(0, 2)],
                expect_non_empty=False,
                expect_unique=True,
            )

    def test__scan__consistent_read__works(self):
        with self.assertRaises(DBQueryException):
            self.table.scan(
                index=SimpleIndex.STR_INT_INDEX,
                filters=[Key("int_attr").between(0, 4)],
                consistent_read=True,
            )

    def test__smart_query__no_args_does_scan(self):
        entries = [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(5)
        ]
        actual = self.table.smart_query()
        self.assertEqual(len(actual), len(entries))

    def test__smart_query__no_args__fails_if_scan_not_allowed(self):
        with self.assertRaises(DBQueryException):
            self.table.smart_query(allow_scan=False)

    def test__smart_query__only_attributes_does_scan(self):
        [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(5)
        ]
        actual = self.table.smart_query(int_attr=1)
        self.assertEqual(len(actual), 1)

    def test__smart_query__only_key_attributes_does_query(self):
        [
            self.table.put(
                self.create_entry(primary_key=f"primary_key_{i}", str_attr="str_attr", int_attr=i)
            )
            for i in range(5)
        ]
        actual = self.table.smart_query(str_attr="str_attr", int_attr=1)
        self.assertEqual(len(actual), 1)

    def test__delete__works(self):
        entry = self.create_entry()
        entry_key = self.table.build_key_from_entry(entry)
        put_entry = self.table.put(entry)
        fetched_entry = self.table.get(entry_key)
        deleted_entry = self.table.delete(entry, error_on_nonexistent=True)
        with self.assertRaises(DBReadException):
            self.table.get(entry_key)
        self.assertEqual(entry, put_entry)
        self.assertEqual(entry, fetched_entry)
        self.assertEqual(entry, deleted_entry)

    def test__delete__only_fails_for_nonexistent_if_true(self):
        self.table.delete({"primary_key": "primary_key"}, error_on_nonexistent=False)

        with self.assertRaises(DBWriteException):
            self.table.delete({"primary_key": "primary_key"}, error_on_nonexistent=True)

    def test__build_key_from_item__works(self):
        entry_dict = self.create_entry().to_dict()
        self.assertDictEqual(
            self.table.build_key_from_item(entry_dict), {"primary_key": "primary_key"}
        )
        self.assertDictEqual(
            self.table.build_key_from_item(entry_dict, index=SimpleIndex.STR_INT_INDEX),
            {"int_attr": 1, "str_attr": "str_attr"},
        )

    def test__build_key_from_entry__works(self):
        entry = self.create_entry()
        self.assertDictEqual(
            self.table.build_key_from_entry(entry), {"primary_key": "primary_key"}
        )
        self.assertDictEqual(
            self.table.build_key_from_entry(entry, index=SimpleIndex.STR_INT_INDEX),
            {"int_attr": 1, "str_attr": "str_attr"},
        )

    def test__from_env__works(self):
        SimpleTable.from_env()

    def create_entry(self, **kwargs) -> SimpleModel:
        entry_dict = dict(
            primary_key="primary_key",
            str_attr="str_attr",
            int_attr=1,
            float_attr=1.0,
            datetime_attr=from_isoformat_8601("2021-01-01T00:00:00.000+00:00"),
        )
        entry_dict.update(kwargs)
        return SimpleModel.from_dict(entry_dict)


@pytest.mark.parametrize(
    "candidate_indexes, args, kwargs, expected, raise_expectation",
    [
        pytest.param(
            SimpleIndex,
            [],
            {},
            (None, None, None, []),
            does_not_raise(),
            id="no args (SCAN)",
        ),
        pytest.param(
            SimpleIndex,
            [Key("float_attr").eq(Decimal("1.0"))],
            {},
            (None, None, None, [Key("float_attr").eq(Decimal("1.0"))]),
            does_not_raise(),
            id="only args with only filter expressions as condition (SCAN)",
        ),
        pytest.param(
            SimpleIndex,
            [Key("float_attr").eq(Decimal("1.0"))],
            {"float_attr": Decimal("1.0")},
            (
                None,
                None,
                None,
                [Attr("float_attr").eq(Decimal("1.0")), Key("float_attr").eq(Decimal("1.0"))],
            ),
            does_not_raise(),
            id="only kwargs with only filter expressions with duplicates (SCAN)",
        ),
        pytest.param(
            SimpleIndex,
            [{"primary_key": "primary_key"}],
            {},
            (SimpleIndex.PRIMARY_KEY, Key("primary_key").eq("primary_key"), None, []),
            does_not_raise(),
            id="only args with one partition key as dict (QUERY)",
        ),
        pytest.param(
            SimpleIndex,
            [Key("primary_key").eq("primary_key")],
            {},
            (SimpleIndex.PRIMARY_KEY, Key("primary_key").eq("primary_key"), None, []),
            does_not_raise(),
            id="only args with one partition key as condition (QUERY)",
        ),
        pytest.param(
            SimpleIndex,
            [],
            {"primary_key": "primary_key"},
            (SimpleIndex.PRIMARY_KEY, Key("primary_key").eq("primary_key"), None, []),
            does_not_raise(),
            id="only kwargs with one partition key as dict (QUERY)",
        ),
        pytest.param(
            SimpleIndex,
            [{"primary_key": "primary_key"}, {"str_attr": "str_attr"}],
            {},
            (
                SimpleIndex.PRIMARY_KEY,
                Key("primary_key").eq("primary_key"),
                None,
                [Key("str_attr").eq("str_attr")],
            ),
            does_not_raise(),
            id="only args with competing keys for indices",
        ),
        pytest.param(
            [SimpleIndex.STR_INT_INDEX, SimpleIndex.STR_DT_INDEX],
            [{"str_attr": "str_attr"}, {"datetime_attr": "date"}],
            {},
            (
                SimpleIndex.STR_INT_INDEX,
                Key("str_attr").eq("str_attr"),
                None,
                [Key("datetime_attr").eq("date")],
            ),
            does_not_raise(),
            id="only args with competing keys and index order specified",
        ),
        pytest.param(
            [SimpleIndex.STR_INT_INDEX, SimpleIndex.PRIMARY_KEY],
            [
                Key("primary_key").eq("primary_key"),
                Key("str_attr").eq("str_attr"),
                Key("int_attr").gt(1),
            ],
            {"str_attr": "str_attr"},
            (
                SimpleIndex.STR_INT_INDEX,
                Key("str_attr").eq("str_attr"),
                Key("int_attr").gt(1),
                [Key("primary_key").eq("primary_key")],
            ),
            does_not_raise(),
            id="args/kwargs with competing keys and index order specified (QUERY)",
        ),
        pytest.param(
            SimpleIndex,
            [
                Key("str_attr").eq("str_attr"),
                Attr("int_attr").gt(1) & Attr("int_attr").lt(3),
            ],
            {},
            (
                SimpleIndex.STR_DT_INDEX,
                Key("str_attr").eq("str_attr"),
                None,
                [Attr("int_attr").gt(1) & Attr("int_attr").lt(3)],
            ),
            does_not_raise(),
            id="args with complex conditions and index order specified (QUERY)",
        ),
        pytest.param(
            SimpleIndex,
            [
                {"primary_key": "primary_key"},
                Key("primary_key").eq("primary_key"),
                Attr("primary_key").eq("primary_key"),
            ],
            {"primary_key": "primary_key"},
            (SimpleIndex.PRIMARY_KEY, Key("primary_key").eq("primary_key"), None, []),
            does_not_raise(),
            id="args/kwargs handles duplicate key/values (QUERY)",
        ),
        pytest.param(
            SimpleIndex,
            [Key("primary_key").eq("primary_key")],
            {"primary_key": "something_else"},
            None,
            pytest.raises(DBQueryException),
            id="INVALID args/kwargs contain duplicate keys with different values",
        ),
        pytest.param(
            SimpleIndex,
            [Key("primary_key").eq("primary_key"), {"primary_key": "something_else"}],
            {},
            None,
            pytest.raises(DBQueryException),
            id="INVALID args/kwargs contain duplicate keys with different values (dict check)",
        ),
    ],
)
def test__build_optimized_condition_expression_set__works(
    candidate_indexes,
    args,
    kwargs,
    expected,
    raise_expectation,
):
    with raise_expectation:
        actual = build_optimized_condition_expression_set(candidate_indexes, *args, **kwargs)

    if expected:
        for i in range(len(expected)):
            assert actual[i] == expected[i]
