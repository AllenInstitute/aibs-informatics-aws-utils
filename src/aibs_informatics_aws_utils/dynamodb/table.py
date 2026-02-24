import functools
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from typing import (
    Any,
    Generic,
    Literal,
    TypeVar,
    cast,
    overload,
)

from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.db import (
    DBIndex,
    DBModel,
    DynamoDBItemValue,
    DynamoDBKey,
    DynamoDBPrimaryKeyItemValue,
)
from aibs_informatics_core.utils.logging import LoggingMixin
from boto3.dynamodb.conditions import (
    Attr,
    BeginsWith,
    Between,
    BuiltConditionExpression,
    ConditionBase,
    ConditionExpressionBuilder,
    Equals,
    GreaterThan,
    GreaterThanEquals,
    Key,
    LessThan,
    LessThanEquals,
)
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import get_client_error_code
from aibs_informatics_aws_utils.dynamodb.conditions import condition_to_str
from aibs_informatics_aws_utils.dynamodb.functions import (
    convert_floats_to_decimals,
    execute_partiql_statement,
    table_delete_item,
    table_get_item,
    table_get_items,
    table_put_item,
    table_query,
    table_scan,
    table_update_item,
)
from aibs_informatics_aws_utils.exceptions import (
    DBQueryException,
    DBReadException,
    DBWriteException,
    EmptyQueryResultException,
    NonUniqueQueryResultException,
)

DB_TABLE = TypeVar("DB_TABLE", bound="DynamoDBTable")
DB_MODEL = TypeVar("DB_MODEL", bound=DBModel)
DB_INDEX = TypeVar("DB_INDEX", bound=DBIndex)


def check_db_query_unique(
    index: DBIndex | None,
    query_result: list[dict[str, Any]],
    key_condition_expression: ConditionBase | None = None,
    filter_expression: ConditionBase | None = None,
):
    if len(query_result) > 1:
        readable_key_expression: BuiltConditionExpression | None = None
        if key_condition_expression:
            expression_builder = ConditionExpressionBuilder()
            readable_key_expression = expression_builder.build_expression(
                condition=key_condition_expression, is_key_condition=True
            )
        raise NonUniqueQueryResultException(
            f"Querying '{index.table_name() if index else ''}' table "
            f"(index: {index.index_name if index else ''}, "
            f"key condition: {readable_key_expression}, "
            f"filters: {filter_expression}) "
            f"did not return EXACTLY 1 result! Query results: '{query_result}'"
        )


def check_db_query_non_empty(
    index: DBIndex | None,
    query_result: list[dict[str, Any]],
    key_condition_expression: ConditionBase | None = None,
    filter_expression: ConditionBase | None = None,
):
    if len(query_result) == 0:
        readable_key_expression: BuiltConditionExpression | None = None
        if key_condition_expression:
            expression_builder = ConditionExpressionBuilder()
            readable_key_expression = expression_builder.build_expression(
                condition=key_condition_expression, is_key_condition=True
            )
        raise EmptyQueryResultException(
            f"Querying '{index.table_name() if index else ''}' table "
            f"(index: {index.index_name if index else ''}, "
            f"key condition: {readable_key_expression}, "
            f"filters: {filter_expression}) "
            f"returned NO results when at least 1 result was expected!"
        )


def check_table_name_and_index_match(table_name: str, index: DBIndex | type[DBIndex]):
    if not table_name.endswith(index.table_name()):
        raise DBQueryException(
            f"The provided DBIndex ({index}) is not valid for the table to be queried "
            f"({table_name})!"
        )


def check_index_supports_strongly_consistent_read(index: DBIndex):
    if not index.supports_strongly_consistent_read:
        raise DBQueryException(
            f"The provided DBIndex ({index}) is a GSI/LSI of the table "
            f"({index.table_name()}) and does not support strongly consistent reads!"
        )


def build_optimized_condition_expression_set(
    candidate_indexes: type[DB_INDEX] | Sequence[DB_INDEX],
    *args: DynamoDBKey | ConditionBase,
    **kwargs: Any,
) -> tuple[DB_INDEX | None, ConditionBase | None, ConditionBase | None, list[ConditionBase]]:
    """Builds an optimized set of conditions for a query or scan


    Args:
        candidate_indexes (Type[DB_INDEX]|Sequence[DB_INDEX]): index class or subset of indexes
            the order of the indexes matters! The first index that matches the provided
            conditions will be used.
        *args (Union[DynamoDBKey, ConditionBase]): varargs of DynamoDBKey or ConditionBase
        **kwargs (Any): kwargs of DynamoDBKey or ConditionBase

    Returns:
        A tuple containing:
            the target index,
            partition key condition,
            sort key condition,
            and filter expressions.

    """
    target_index: DB_INDEX | None = None
    partition_key: ConditionBase | None = None
    sort_key_condition_expression: ConditionBase | None = None
    filter_expressions: list[ConditionBase] = []

    if not args and not kwargs:
        return target_index, partition_key, sort_key_condition_expression, filter_expressions

    if not isinstance(candidate_indexes, Sequence):
        candidate_indexes = [ci for ci in candidate_indexes]
    index_all_key_names = set(
        {
            *{_.key_name for _ in candidate_indexes},
            *{_.sort_key_name for _ in candidate_indexes if _.sort_key_name},
        }
    )

    SupportedKeyComparisonTypes = (
        Equals,
        GreaterThan,
        GreaterThanEquals,
        LessThan,
        LessThanEquals,
        BeginsWith,
        Between,
    )

    candidate_conditions: dict[
        str,
        (
            Equals
            | GreaterThan
            | GreaterThanEquals
            | LessThan
            | LessThanEquals
            | BeginsWith
            | Between
        ),
    ] = {}
    non_candidate_conditions: list[ConditionBase] = []

    for _ in (kwargs,) + args:
        if not isinstance(_, ConditionBase):
            for k, v in _.items():
                if k not in index_all_key_names:
                    non_candidate_conditions.append(Attr(k).eq(v))
                    continue
                new_condition = Key(k).eq(v)
                if (
                    k in candidate_conditions
                    and candidate_conditions[k]._values[1:] != new_condition._values[1:]  # type: ignore[attr-defined,union-attr]
                ):
                    raise DBQueryException(f"Multiple values provided for attribute {k}!")
                candidate_conditions[k] = Key(k).eq(v)
        elif len(_._values) and isinstance(_._values[0], (Key, Attr)):  # type: ignore[attr-defined,union-attr]
            attr_name = cast(str, _._values[0].name)  # type: ignore[attr-defined,union-attr]
            if attr_name not in index_all_key_names or not isinstance(
                _, SupportedKeyComparisonTypes
            ):
                non_candidate_conditions.append(_)
                continue
            if (
                attr_name in candidate_conditions
                and candidate_conditions[attr_name]._values[1:] != _._values[1:]  # type: ignore[union-attr]
            ):
                raise DBQueryException(f"Multiple values provided for attribute {attr_name}!")
            candidate_conditions[attr_name] = _
        else:
            non_candidate_conditions.append(_)

    for index in candidate_indexes:
        if index.key_name in candidate_conditions and isinstance(
            candidate_conditions[index.key_name], Equals
        ):
            target_index = index
            partition_key = candidate_conditions.pop(index.key_name)
            partition_key._values = (Key(index.key_name), *partition_key._values[1:])  # type: ignore[union-attr]
            if index.sort_key_name is not None and index.sort_key_name in candidate_conditions:
                sort_key_condition_expression = candidate_conditions.pop(index.sort_key_name)
                sort_key_condition_expression._values = (  # type: ignore[union-attr]
                    Key(index.sort_key_name),
                    *sort_key_condition_expression._values[1:],  # type: ignore[union-attr]
                )
            break

    # convert all remaining to filters
    filter_expressions = list(candidate_conditions.values()) + non_candidate_conditions

    return target_index, partition_key, sort_key_condition_expression, filter_expressions


@dataclass
class DynamoDBTable(LoggingMixin, Generic[DB_MODEL, DB_INDEX]):
    def __post_init__(self):
        check_table_name_and_index_match(self.table_name, self.get_db_index_cls())

    @property
    def table_name(self) -> str:
        return self.get_db_index_cls().table_name()

    def get_index_name(self, index: DB_INDEX | None = None) -> str | None:
        return self.index_or_default(index).index_name

    @classmethod
    @functools.cache
    def get_db_model_cls(cls) -> type[DB_MODEL]:
        return cls.__orig_bases__[0].__args__[0]  # type: ignore

    @classmethod
    @functools.cache
    def get_db_index_cls(cls) -> type[DB_INDEX]:
        return cls.__orig_bases__[0].__args__[1]  # type: ignore

    @classmethod
    def build_entry(cls, item: dict[str, Any], **kwargs) -> DB_MODEL:
        return cls.get_db_model_cls().from_dict(item, **kwargs)

    @classmethod
    def build_item(cls, entry: DB_MODEL, **kwargs) -> dict[str, Any]:
        entry_dict = entry.to_dict(**kwargs)
        return convert_floats_to_decimals(entry_dict)

    @classmethod
    def index_or_default(cls, index: DB_INDEX | None = None) -> DB_INDEX:
        return index if index is not None else cls.get_db_index_cls().get_default_index()

    @classmethod
    def build_key_from_entry(cls, entry: DB_MODEL, index: DB_INDEX | None = None) -> DynamoDBKey:
        index = cls.index_or_default(index)
        if index.sort_key_name:
            return index.get_primary_key(
                partition_value=getattr(entry, index.key_name),
                sort_value=getattr(entry, index.sort_key_name),
            )
        else:
            return index.get_primary_key(partition_value=getattr(entry, index.key_name))

    @classmethod
    def build_key_from_item(
        cls, item: dict[str, Any], index: DB_INDEX | None = None
    ) -> DynamoDBKey:
        index = cls.index_or_default(index)
        if index.sort_key_name:
            return index.get_primary_key(
                partition_value=item.get(index.key_name), sort_value=item.get(index.sort_key_name)
            )
        else:
            return index.get_primary_key(partition_value=item.get(index.key_name))

    @classmethod
    def build_key(
        cls,
        key: DynamoDBItemValue | tuple[DynamoDBItemValue, DynamoDBItemValue] | DynamoDBKey,
        index: DB_INDEX | None = None,
    ) -> DynamoDBKey:
        if isinstance(key, MutableMapping):
            return key
        index = cls.index_or_default(index)
        return (
            index.get_primary_key(key[0], key[1])
            if isinstance(key, tuple)
            else index.get_primary_key(key)
        )

    # --------------------------------------------------------------------------
    # DB read methods (get, batch_get, query, scan, smart_query)
    # --------------------------------------------------------------------------
    def get(
        self,
        key: DynamoDBKey | DynamoDBItemValue | tuple[DynamoDBItemValue, DynamoDBItemValue],
        partial: bool = False,
    ) -> DB_MODEL:
        """Get a single item from a DynamoDB table by providing a key.

        Args:
            key: The partition key value that should be used to search the table.
                Can be a single partition key value, a tuple of partition and sort key value,
                or a dictionary of attribute:value.
            partial: Whether partial values are allowed. Defaults to False.

        Raises:
            DBReadException: If the item could not be found.

        Returns:
            The database entry that was found.
        """
        item_key = self.build_key(key)

        item = table_get_item(table_name=self.table_name, key=item_key)
        if item is None:
            raise DBReadException(f"Could not resolve item from {item_key}")
        return self.build_entry(item, partial=partial)

    def batch_get(
        self,
        keys: (
            list[DynamoDBKey]
            | list[DynamoDBItemValue]
            | list[tuple[DynamoDBItemValue, DynamoDBItemValue]]
        ),
        partial: bool = False,
        ignore_missing: bool = False,
    ) -> list[DB_MODEL]:
        """Batch get items from a DynamoDB table by providing a list of keys.

        Args:
            keys: The partition key values that should be used to search the table.
                Each key can be one of:

                - **single partition key value**
                - **a tuple of partition and sort key value**
                - **a dictionary of attribute:value**

            partial: Whether partial values are allowed. Defaults to False.
            ignore_missing: If true, suppress errors for keys that are not found.
                Defaults to False.

        Raises:
            DBReadException: If any of the items could not be found.

        Returns:
            List of database entries that were found.
        """
        if not keys:
            return []

        index = self.index_or_default()
        item_keys = [self.build_key(key, index=index) for key in keys]

        items = table_get_items(table_name=self.table_name, keys=item_keys)
        if len(items) != len(item_keys) and not ignore_missing:
            missing_keys = {
                (_[index.key_name], _.get(index.sort_key_name or "")) for _ in item_keys
            }.difference((_[index.key_name], _.get(index.sort_key_name or "")) for _ in items)

            raise DBReadException(f"Could not find items for {missing_keys}")
        entries = [self.build_entry(_, partial=partial) for _ in items]
        return entries

    def query(
        self,
        index: DB_INDEX,
        partition_key: DynamoDBPrimaryKeyItemValue | ConditionBase,
        sort_key_condition_expression: ConditionBase | None = None,
        filters: list[ConditionBase] | None = None,
        consistent_read: bool = False,
        expect_non_empty: bool = False,
        expect_unique: bool = False,
        allow_partial: bool = False,
    ) -> list[DB_MODEL]:
        """Query a DynamoDB table by providing a DBIndex, partition_key, optional sort_key, and optional filter conditions.

        Args:
            index: Specifies the specific table index (e.g. main table, global secondary
                index, or local secondary index) that should be queried.
            partition_key: The partition key value that should be used to search the table.
            sort_key_condition_expression: The sort key condition expression to be used
                to query the table. Example:

                ```python
                from boto3.dynamodb.conditions import Key
                Key('my_sort_key_name').begins_with('prefix_')
                ```

            filters: A list of ConditionBase expressions where query results must satisfy.
            consistent_read: Whether a strongly consistent read should be used
                for the query. By default False which returns eventually consistent reads.
            expect_non_empty: Whether the resulting query should return at least
                one result. An error will be raised if expect_non_empty=True and 0 results were
                returned by the query.
            expect_unique: Whether the result of the query is expected to
                return AT MOST one result. An error will be raised if expect_unique=True and MORE
                than 1 result was returned for the query.
            allow_partial: Whether to allow partial entries. Defaults to False.

        Returns:
            A list of database model entries where partition_key/sort_key and
                filter conditions are satisfied.
        """  # noqa: E501

        if consistent_read:
            check_index_supports_strongly_consistent_read(index=index)

        index_name = self.get_index_name(index)

        key_condition_expression = self._build_key_condition_expression(
            index=index,
            partition_key=partition_key,
            sort_key_condition_expression=sort_key_condition_expression,
        )
        filter_expression = self._build_filter_condition_expression(filters=filters)

        self.log.info(
            f"Calling query on {self.table_name} table (index: {index_name}, "
            f"key condition: {condition_to_str(key_condition_expression)}, "
            f"filters: {condition_to_str(filter_expression)})"
        )

        items = table_query(
            table_name=self.table_name,
            index_name=index_name,
            key_condition_expression=key_condition_expression,
            filter_expression=filter_expression,
            consistent_read=consistent_read,
        )
        if expect_unique:
            check_db_query_unique(
                index=index,
                query_result=items,
                key_condition_expression=key_condition_expression,
                filter_expression=filter_expression,
            )
        if expect_non_empty:
            check_db_query_non_empty(
                index=index,
                query_result=items,
                key_condition_expression=key_condition_expression,
                filter_expression=filter_expression,
            )
        entries = [self.build_entry(_, partial=True) for _ in items]
        if not allow_partial:
            entries = self._fill_values(entries)
        return entries

    def scan(
        self,
        index: DB_INDEX | None = None,
        filters: list[ConditionBase] | None = None,
        consistent_read: bool = False,
        expect_non_empty: bool = False,
        expect_unique: bool = False,
        allow_partial: bool = False,
    ) -> list[DB_MODEL]:
        """Scan a DynamoDB table by providing a DBIndex and optional filter conditions.

        Args:
            index: Specifies the specific table index (e.g. main table, global secondary
                index, or local secondary index) that should be scanned.
            filters: A list of ConditionBase expressions where scan results must satisfy.
            consistent_read: Whether a strongly consistent read should be used
                for the scan. By default False which returns eventually consistent reads.
            expect_non_empty: Whether the resulting scan should return at least
                one result. An error will be raised if expect_non_empty=True and 0 results were
                returned by the scan.
            expect_unique: Whether the result of the scan is expected to
                return AT MOST one result. An error will be raised if expect_unique=True and MORE
                than 1 result was returned for the scan.
            allow_partial: Whether to allow partial entries. Defaults to False.

        Returns:
            A list of database model entries where filter conditions are satisfied.
        """
        index = self.index_or_default(index)
        if consistent_read:
            check_index_supports_strongly_consistent_read(index=index)

        index_name = self.get_index_name(index)
        filter_expression = self._build_filter_condition_expression(filters=filters)

        self.log.info(
            f"Calling scan on {self.table_name} table (index: {index_name},"
            f" filters: {condition_to_str(filter_expression)})"
        )

        items = table_scan(
            table_name=self.table_name,
            index_name=index_name,
            filter_expression=filter_expression,
            consistent_read=consistent_read,
        )
        if expect_unique:
            check_db_query_unique(
                index=index,
                query_result=items,
                filter_expression=filter_expression,
            )
        if expect_non_empty:
            check_db_query_non_empty(
                index=index,
                query_result=items,
                filter_expression=filter_expression,
            )
        entries = [self.build_entry(_, partial=True) for _ in items]
        if not allow_partial:
            entries = self._fill_values(entries)
        return entries

    def smart_query(
        self,
        *filters: DynamoDBKey | ConditionBase,
        consistent_read: bool = False,
        expect_non_empty: bool = False,
        expect_unique: bool = False,
        allow_partial: bool = False,
        allow_scan: bool = True,
        **kw_filters: Any,
    ) -> list[DB_MODEL]:
        """
        Perform a smart query on the DynamoDB table by automatically determining whether to
        use a query or scan operation based on the provided filters.

        Args:
            *filters (Union[DynamoDBKey, ConditionBase]):
                Varargs of DynamoDBKey or ConditionBase to filter the query/scan
            consistent_read (bool):
                Whether a strongly consistent read should be used for the query/scan.
                Defaults to False which returns eventually consistent reads.
            expect_non_empty (bool):
                Whether the resulting query/scan should return at least one result.
                An error will be raised if expect_non_empty=True and 0 results were returned.
            expect_unique (bool):
                Whether the result of the query/scan is expected to return AT MOST one result.
                An error will be raised if expect_unique=True and MORE than 1 result was returned.
            allow_partial (bool):
                Whether to allow partial entries. Defaults to False.
            allow_scan (bool):
                Whether to allow a scan operation if no partition key is found in the filters.
                Defaults to True.

        Raises:
            DBQueryException:
                If no partition key is found and allow_scan is False.

        Returns:
            A list of database model entries where partition_key/sort_key and filter
                conditions are satisfied.
        """  # noqa: E501
        (
            index,
            partition_key,
            sort_key_condition_expression,
            filter_expressions,
        ) = build_optimized_condition_expression_set(
            self.get_db_index_cls(),
            *filters,
            **kw_filters,
        )
        if partition_key:
            return self.query(
                index=self.index_or_default(index),
                partition_key=partition_key,
                sort_key_condition_expression=sort_key_condition_expression,
                filters=filter_expressions,
                consistent_read=consistent_read,
                expect_non_empty=expect_non_empty,
                expect_unique=expect_unique,
                allow_partial=allow_partial,
            )
        else:
            if not allow_scan:
                raise DBQueryException(
                    f"Could not find a partition key for {self.table_name} table! "
                    "Please provide a partition key or allow_scan=True."
                )
            if sort_key_condition_expression:
                filter_expressions.append(sort_key_condition_expression)
            return self.scan(
                index=index,
                filters=filter_expressions,
                consistent_read=consistent_read,
                expect_non_empty=expect_non_empty,
                expect_unique=expect_unique,
                allow_partial=allow_partial,
            )

    # --------------------------------------------------------------------------
    # DB write methods (put, update, delete)
    # --------------------------------------------------------------------------
    def put(
        self,
        entry: DB_MODEL,
        condition_expression: ConditionBase | None = None,
        **table_put_item_kwargs,
    ) -> DB_MODEL:
        """Put a new item into the DynamoDB table.

        Args:
            entry (DB_MODEL): The database model entry to be put into the table.
            condition_expression (Optional[ConditionBase], optional): An optional condition
                expression that must be satisfied for the put operation to succeed.
                Defaults to None.
            **table_put_item_kwargs: Additional keyword arguments to be passed to the
                table_put_item function.

        Raises:
            DBWriteException: If there was an error putting the entry.
            DBWriteException: If the HTTP response code indicates a failure.

        Returns:
            The database model entry that was put into the table.
        """
        put_summary = (
            f"(entry: {entry}, condition_expression: {condition_to_str(condition_expression)})"
        )
        self.log.debug(f"{self.table_name} - Putting new entry: {put_summary}")

        e_msg_intro = f"{self.table_name} - Error putting entry: {put_summary}."
        try:
            put_response = table_put_item(
                table_name=self.table_name,
                item=self.build_item(entry),
                condition_expression=condition_expression,
                **table_put_item_kwargs,
            )
        except ClientError as e:
            e_msg_client = f"{e_msg_intro} Details: {get_client_error_code(e)}"
            self.log.error(e_msg_client)
            raise DBWriteException(e_msg_client)
        else:
            if not (200 <= put_response["ResponseMetadata"]["HTTPStatusCode"] < 300):
                e_msg_http = f"{e_msg_intro} Table put_item response: {put_response}"
                raise DBWriteException(e_msg_http)
        self.log.debug(f"{self.table_name} - Put successful: {put_response}")
        return entry

    def update(
        self,
        key: DynamoDBKey,
        new_entry: Mapping[str, Any] | DB_MODEL,
        old_entry: DB_MODEL | None = None,
        **table_update_item_kwargs,
    ) -> DB_MODEL:
        """Update an existing item in the DynamoDB table.

        Args:
            key (DynamoDBKey): The primary key of the item to be updated.
            new_entry (Union[Mapping[str, Any], DB_MODEL]): The new values for the item.
                Can be a dictionary of attribute:value pairs or a DB_MODEL instance.
            old_entry (Optional[DB_MODEL], optional): The existing entry before the update.
                If provided, only attributes that differ from the old_entry will be updated.
                Defaults to None.
            **table_update_item_kwargs: Additional keyword arguments to be passed to the
                table_update_item function.

        Raises:
            DBWriteException: If there was an error updating the entry.
            DBWriteException: If no attributes need to be updated.

        Returns:
            The updated database model entry.
        """
        new_attributes: dict[str, Any] = {}
        if isinstance(new_entry, self.get_db_model_cls()):
            new_attributes = self.build_item(new_entry, partial=True)
        elif new_entry:
            # we still need to convert floats to decimals if new_entry is a dict
            new_attributes = convert_floats_to_decimals(new_entry)  # type: ignore[arg-type]

        for k in key:
            new_attributes.pop(k, None)
        # Add k:v pair from new_attributes if new != old value for a given key
        new_clean_attrs: dict[str, Any] = {}
        if old_entry:
            for k, new_v in new_attributes.items():
                if getattr(old_entry, k) != new_v:
                    new_clean_attrs[k] = new_v
        else:
            new_clean_attrs = new_attributes

        if not new_clean_attrs:
            self.log.debug(
                f"{self.table_name} - No attr_updates to do! Skipping _update_entry call."
            )
            if not old_entry:
                old_entry = self.get(key)
            return old_entry

        update_summary = f"(old_entry: {old_entry}, new_attributes: {new_clean_attrs})"
        self.log.debug(f"{self.table_name} - Updating entry: {update_summary}")
        try:
            updated_item = table_update_item(
                table_name=self.table_name,
                key=key,
                attributes=new_clean_attrs,
                return_values="ALL_NEW",
                **table_update_item_kwargs,
            )
            # table_update_item will always return a dict if ReturnValues != "NONE"
            if updated_item is None:
                raise DBWriteException(
                    f"{self.table_name} - Error updating entry: {update_summary}"
                )
            updated_entry = self.build_entry(updated_item)
        except ClientError as e:
            e_msg = (
                f"{self.table_name} - Error updating entry: {update_summary}. "
                f"Details: {get_client_error_code(e)}"
            )
            self.log.error(e_msg)
            raise DBWriteException(e_msg)
        self.log.debug(f"{self.table_name} - Successfully updated entry: {updated_entry}")
        return updated_entry

    @overload
    def delete(
        self,
        key: DynamoDBKey | DB_MODEL,
        error_on_nonexistent: Literal[True],
    ) -> DB_MODEL: ...

    @overload
    def delete(
        self,
        key: DynamoDBKey | DB_MODEL,
        error_on_nonexistent: Literal[False],
    ) -> DB_MODEL | None: ...

    @overload
    def delete(
        self,
        key: DynamoDBKey | DB_MODEL,
    ) -> DB_MODEL | None: ...

    def delete(
        self,
        key: DynamoDBKey | DB_MODEL,
        error_on_nonexistent: bool = False,
    ) -> DB_MODEL | None:
        """Delete an item from the DynamoDB table.

        Args:
            key (Union[DynamoDBKey, DB_MODEL]): The primary key of the item to be deleted,
                or a DB_MODEL instance representing the item to be deleted.
            error_on_nonexistent (bool, optional): Whether to raise an error if the item
                does not exist. Defaults to False.

        Raises:
            DBWriteException: If there was an error deleting the entry.
            DBWriteException: If error_on_nonexistent is True and the item does not exist.

        Returns:
            The deleted database model entry, or None if the item did not exist and
                error_on_nonexistent is False.
        """
        if isinstance(key, self.get_db_model_cls()):
            key = self.build_key_from_entry(key)
        delete_summary = f"(db_primary_key: {key})"
        self.log.debug(f"{self.table_name} - Deleting entry with: {delete_summary}")
        e_msg = f"{self.table_name} - Delete failed for the following primary key: {key}"
        try:
            deleted_attributes = table_delete_item(
                table_name=self.table_name,
                key=cast(DynamoDBKey, key),
                return_values="ALL_OLD",  # type: ignore[arg-type] # expected type more general than specified here
            )

            if not deleted_attributes:
                self.log.info(f"{self.table_name} - Nothing deleted for primary key: {key}")
                if error_on_nonexistent:
                    raise DBWriteException(e_msg)
                return None
            else:
                deleted_entry = self.build_entry(deleted_attributes)
                self.log.info(f"{self.table_name} - Deleted entry: {deleted_attributes}")
                return deleted_entry
        except ClientError as e:
            detailed_e_msg = f"{e_msg}. Details: {get_client_error_code(e)}"
            raise DBWriteException(detailed_e_msg)

    # --------------------------------------------------------------------------
    # Key and filter utils
    # --------------------------------------------------------------------------
    def _build_key_condition_expression(
        self,
        index: DB_INDEX,
        partition_key: DynamoDBPrimaryKeyItemValue | ConditionBase,
        sort_key_condition_expression: ConditionBase | None = None,
    ) -> ConditionBase:
        partition_key_name = index.key_name
        sort_key_name = index.sort_key_name

        # Build dynamodb key condition expression
        assert partition_key_name is not None
        key_condition_expression: ConditionBase
        if isinstance(partition_key, ConditionBase):
            key_condition_expression = partition_key
        else:
            key_condition_expression = Key(partition_key_name).eq(partition_key)

        if sort_key_condition_expression is not None:
            condition_key_name = sort_key_condition_expression.get_expression()["values"][0].name
            if sort_key_name is not None:
                if sort_key_name == condition_key_name:
                    key_condition_expression &= sort_key_condition_expression
                else:
                    raise DBQueryException(
                        "The sort key specified by the provided sort_key_condition_expression "
                        f"({condition_key_name}) does not match the sort key name of the index "
                        f"to be queried (index: {index}, index_sort_key_name: {sort_key_name})!"
                    )
            else:
                self.log.warning(
                    f"A sort key condition expression was provided "
                    f"({sort_key_condition_expression}) for the query but the specified "
                    f"table index {index} does not support a sort key!"
                )
        return key_condition_expression

    def _build_filter_condition_expression(
        self, filters: list[ConditionBase] | None
    ) -> ConditionBase | None:
        # Build dynamodb attribute condition expression
        filter_expression: ConditionBase | None = None
        if filters:
            filter_expression = functools.reduce(lambda a, b: a & b, filters)
        return filter_expression

    def _fill_values(self, entries: list[DB_MODEL]) -> list[DB_MODEL]:
        entry_index_is_partial = [(_, i, _.is_partial()) for i, _ in enumerate(entries)]
        entry_index_is_partial__complete = [_ for _ in entry_index_is_partial if not _[-1]]
        entry_index_is_partial__partials = [_ for _ in entry_index_is_partial if _[-1]]

        filled_entries = self.batch_get(
            [self.build_key_from_entry(entry) for entry, _, _ in entry_index_is_partial__partials]
        )
        entry_index_is_partial__filled = [
            (filled_entry, i, filled_entry.is_partial())
            for filled_entry, (_, i, _) in zip(filled_entries, entry_index_is_partial__partials)
        ]
        return [
            entry
            for (entry, _, _) in sorted(
                entry_index_is_partial__complete + entry_index_is_partial__filled,
                key=lambda _: _[1],
            )
        ]

    def execute_partiql_statement(self, statement: str) -> Sequence:
        return execute_partiql_statement(statement=statement)

    @classmethod
    def from_env(
        cls: type["DynamoDBTable[DB_MODEL, DB_INDEX]"], *args, **kwargs
    ) -> "DynamoDBTable[DB_MODEL, DB_INDEX]":
        return cls(*args, **kwargs)


@dataclass
class DynamoDBEnvBaseTable(DynamoDBTable[DB_MODEL, DB_INDEX], Generic[DB_MODEL, DB_INDEX]):
    env_base: EnvBase = field(default_factory=EnvBase.from_env)

    @property
    def table_name(self) -> str:
        return self.env_base.get_table_name(super().table_name)

    def get_index_name(self, index: DB_INDEX | None = None) -> str | None:
        if (index_name := super().get_index_name(index)) is not None:
            return self.env_base.prefixed(index_name)
        return index_name
