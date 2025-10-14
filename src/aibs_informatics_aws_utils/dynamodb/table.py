import functools
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
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
    index: Optional[DBIndex],
    query_result: List[Dict[str, Any]],
    key_condition_expression: Optional[ConditionBase] = None,
    filter_expression: Optional[ConditionBase] = None,
):
    if len(query_result) > 1:
        readable_key_expression: Optional[BuiltConditionExpression] = None
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
    index: Optional[DBIndex],
    query_result: List[Dict[str, Any]],
    key_condition_expression: Optional[ConditionBase] = None,
    filter_expression: Optional[ConditionBase] = None,
):
    if len(query_result) == 0:
        readable_key_expression: Optional[BuiltConditionExpression] = None
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


def check_table_name_and_index_match(table_name: str, index: Union[DBIndex, Type[DBIndex]]):
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
    candidate_indexes: Union[Type[DB_INDEX], Sequence[DB_INDEX]],
    *args: Union[DynamoDBKey, ConditionBase],
    **kwargs: Any,
) -> Tuple[
    Optional[DB_INDEX], Optional[ConditionBase], Optional[ConditionBase], List[ConditionBase]
]:
    """Builds an optimized set of conditions for a query or scan


    Args:
        candidate_indexes (Type[DB_INDEX]|Sequence[DB_INDEX]): index class or subset of indexes
            the order of the indexes matters! The first index that matches the provided
            conditions will be used.
        *args (Union[DynamoDBKey, ConditionBase]): varargs of DynamoDBKey or ConditionBase
        **kwargs (Any): kwargs of DynamoDBKey or ConditionBase

    Raises:

    Returns:
        Tuple[
            Optional[DB_INDEX],
            Optional[ConditionBase],
            Optional[ConditionBase],
            List[ConditionBase]
        ]
            * target_index
            * partition_key
            * sort_key_condition_expression
            * filter_expressions
    """
    target_index: Optional[DB_INDEX] = None
    partition_key: Optional[ConditionBase] = None
    sort_key_condition_expression: Optional[ConditionBase] = None
    filter_expressions: List[ConditionBase] = []

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

    candidate_conditions: Dict[
        str,
        Union[
            Equals, GreaterThan, GreaterThanEquals, LessThan, LessThanEquals, BeginsWith, Between
        ],
    ] = {}
    non_candidate_conditions: List[ConditionBase] = []

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

    def get_index_name(self, index: Optional[DB_INDEX] = None) -> Optional[str]:
        return self.index_or_default(index).index_name

    @classmethod
    @functools.cache
    def get_db_model_cls(cls) -> Type[DB_MODEL]:
        return cls.__orig_bases__[0].__args__[0]  # type: ignore

    @classmethod
    @functools.cache
    def get_db_index_cls(cls) -> Type[DB_INDEX]:
        return cls.__orig_bases__[0].__args__[1]  # type: ignore

    @classmethod
    def build_entry(cls, item: Dict[str, Any], **kwargs) -> DB_MODEL:
        return cls.get_db_model_cls().from_dict(item, **kwargs)

    @classmethod
    def build_item(cls, entry: DB_MODEL, **kwargs) -> Dict[str, Any]:
        entry_dict = entry.to_dict(**kwargs)
        return convert_floats_to_decimals(entry_dict)

    @classmethod
    def index_or_default(cls, index: Optional[DB_INDEX] = None) -> DB_INDEX:
        return index if index is not None else cls.get_db_index_cls().get_default_index()

    @classmethod
    def build_key_from_entry(
        cls, entry: DB_MODEL, index: Optional[DB_INDEX] = None
    ) -> DynamoDBKey:
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
        cls, item: Dict[str, Any], index: Optional[DB_INDEX] = None
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
        key: Union[DynamoDBItemValue, Tuple[DynamoDBItemValue, DynamoDBItemValue], DynamoDBKey],
        index: Optional[DB_INDEX] = None,
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
        key: Union[DynamoDBKey, DynamoDBItemValue, Tuple[DynamoDBItemValue, DynamoDBItemValue]],
        partial: bool = False,
    ) -> DB_MODEL:
        """Get a single item from a DynamoDB table by providing a key,

        Args:
            key (DynamoDBKey|DynamoDBItemValue|Tuple[DynamoDBItemValue,DynamoDBItemValue]]):
                The partition key value that should be used to search the table.
                Can be a single partition key value, a tuple of partition and sort key value,
                or a dictionary of attribute:value.
            partial (bool, optional): Whether partial values are allowed. Defaults to False.

        Raises:
            DBReadException: If the item could not be found.

        Returns:
            DB_MODEL: The database entry that was found.
        """
        item_key = self.build_key(key)

        item = table_get_item(table_name=self.table_name, key=item_key)
        if item is None:
            raise DBReadException(f"Could not resolve item from {item_key}")
        return self.build_entry(item, partial=partial)

    def batch_get(
        self,
        keys: Union[
            List[DynamoDBKey],
            List[DynamoDBItemValue],
            List[Tuple[DynamoDBItemValue, DynamoDBItemValue]],
        ],
        partial: bool = False,
        ignore_missing: bool = False,
    ) -> List[DB_MODEL]:
        """Batch get items from a DynamoDB table by providing a list of keys

        Args:
            keys (Union[ List[DynamoDBKey], List[DynamoDBItemValue], List[Tuple[DynamoDBItemValue, DynamoDBItemValue]], ]):
                The partition key values that should be used to search the table.
                Each key can be one of:
                    single partition key value,
                    a tuple of partition and sort key value,
                    or a dictionary of attribute:value.
            partial (bool, optional): Whether partial values are allowed. Defaults to False.
            ignore_missing (bool, optional): If true, suppress errors for keys that are not found.
                Defaults to False.

        Raises:
            DBReadException: If any of the items could not be found.

        Returns:
            List[DB_MODEL]: List of database entries that were found.
        """  # noqa: E501
        if not keys:
            return []

        index = self.index_or_default()
        item_keys = [self.build_key(key, index=index) for key in keys]

        items = table_get_items(table_name=self.table_name, keys=item_keys)
        if len(items) != len(item_keys) and not ignore_missing:
            missing_keys = set(
                [(_[index.key_name], _.get(index.sort_key_name or "")) for _ in item_keys]
            ).difference((_[index.key_name], _.get(index.sort_key_name or "")) for _ in items)

            raise DBReadException(f"Could not find items for {missing_keys}")
        entries = [self.build_entry(_, partial=partial) for _ in items]
        return entries

    def query(
        self,
        index: DB_INDEX,
        partition_key: Union[DynamoDBPrimaryKeyItemValue, ConditionBase],
        sort_key_condition_expression: Optional[ConditionBase] = None,
        filters: Optional[List[ConditionBase]] = None,
        consistent_read: bool = False,
        expect_non_empty: bool = False,
        expect_unique: bool = False,
        allow_partial: bool = False,
    ) -> List[DB_MODEL]:
        """Query a DynamoDB table by providing a DBIndex, partition_key,
        optional sort_key, and optional filter conditions that non-partition-key attributes
        should satisfy.

        Args:
            partition_key (Union[str, ConditionBase]): The partition key value that should be
                used to search the table.
            sort_key_condition_expression (Optional[ConditionBase], optional): The sort key
                condition expression to be used to query the table.

                Example sort_key_condition_expression that specifies a sort key
                ('my_sort_key_name') whose values begin with 'prefix_':

                from boto3.dynamodb.conditions import Key
                Key('my_sort_key_name).begins_with('prefix_')

            index (DBIndex): Specifies the specific table index (e.g. main table, global secondary
                index, or local secondary index) that should be queried. Also 'knows' about the
                appropriate 'table_name' and 'partition_key' name to use for query.
            filters (Optional[Mapping], optional): A dictionary of attribute:value pairs
                where query results must satisfy attribute == value.
            consistent_read (bool, optional): Whether a strongly consistent read should be used
                for the query. By default False which returns **eventually** consistent reads.
            expect_non_empty (bool, optional): Whether the resulting query should return at least
                one result. An error will be raised if expect_non_empty=True and 0 results were
                returned by the query.
            expect_unique (bool, option): Whether the result of the query is expected to
                return AT MOST one result. An error will be raised if expect_unique=True and MORE
                than 1 result was returned for the query.
        Returns:
            Sequence[Dict[str, Any]]: A sequence of dictionaries representing database rows
                where partition_key/sort_key and filter conditions are satisfied.
        """

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
        index: Optional[DB_INDEX] = None,
        filters: Optional[List[ConditionBase]] = None,
        consistent_read: bool = False,
        expect_non_empty: bool = False,
        expect_unique: bool = False,
        allow_partial: bool = False,
    ) -> List[DB_MODEL]:
        """Scan a DynamoDB table by providing a table_name, a DBIndex,
        and optional filter conditions that attributes should satisfy.

        Args:
            index (DBIndex): Specifies the specific table index (e.g. main table, global secondary
                index, or local secondary index) that should be queried. Also 'knows' about the
                appropriate 'table_name' and 'partition_key' name to use for query.
            filters (Optional[Mapping], optional): A dictionary of attribute:value pairs
                where query results must satisfy attribute == value.
            consistent_read (bool, optional): Whether a strongly consistent read should be used
                for the query. By default False which returns **eventually** consistent reads.
            expect_non_empty (bool, optional): Whether the resulting query should return at least
                one result. An error will be raised if expect_non_empty=True and 0 results were
                returned by the query.
            expect_unique (bool, option): Whether the result of the query is expected to
                return AT MOST one result. An error will be raised if expect_unique=True and MORE
                than 1 result was returned for the query.

        Returns:
            Sequence[Dict[str, Any]]: A sequence of dictionaries representing database rows
                where filter conditions are satisfied.
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
        *filters: Union[DynamoDBKey, ConditionBase],
        consistent_read: bool = False,
        expect_non_empty: bool = False,
        expect_unique: bool = False,
        allow_partial: bool = False,
        allow_scan: bool = True,
        **kw_filters: Any,
    ) -> List[DB_MODEL]:
        """
        Constructs a query or scan as necessary from arguments
        (see _query and _scan for arg details). Filters are joined with
        AND into one filter
        """
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
        condition_expression: Optional[ConditionBase] = None,
        **table_put_item_kwargs,
    ) -> DB_MODEL:
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
        new_entry: Union[Mapping[str, Any], DB_MODEL],
        old_entry: Optional[DB_MODEL] = None,
        **table_update_item_kwargs,
    ) -> DB_MODEL:
        new_attributes: Dict[str, Any] = {}
        if isinstance(new_entry, self.get_db_model_cls()):
            new_attributes = self.build_item(new_entry, partial=True)
        elif new_entry:
            # we still need to convert floats to decimals if new_entry is a dict
            new_attributes = convert_floats_to_decimals(new_entry)  # type: ignore[arg-type]

        for k in key:
            new_attributes.pop(k, None)
        # Add k:v pair from new_attributes if new != old value for a given key
        new_clean_attrs: Dict[str, Any] = {}
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
        key: Union[DynamoDBKey, DB_MODEL],
        error_on_nonexistent: Literal[True],
    ) -> DB_MODEL: ...

    @overload
    def delete(
        self,
        key: Union[DynamoDBKey, DB_MODEL],
        error_on_nonexistent: Literal[False],
    ) -> Optional[DB_MODEL]: ...

    @overload
    def delete(
        self,
        key: Union[DynamoDBKey, DB_MODEL],
    ) -> Optional[DB_MODEL]: ...

    def delete(
        self,
        key: Union[DynamoDBKey, DB_MODEL],
        error_on_nonexistent: bool = False,
    ) -> Optional[DB_MODEL]:
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
        partition_key: Union[DynamoDBPrimaryKeyItemValue, ConditionBase],
        sort_key_condition_expression: Optional[ConditionBase] = None,
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
        self, filters: Optional[List[ConditionBase]]
    ) -> Optional[ConditionBase]:
        # Build dynamodb attribute condition expression
        filter_expression: Optional[ConditionBase] = None
        if filters:
            filter_expression = functools.reduce(lambda a, b: a & b, filters)
        return filter_expression

    def _fill_values(self, entries: List[DB_MODEL]) -> List[DB_MODEL]:
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
        cls: Type["DynamoDBTable[DB_MODEL, DB_INDEX]"], *args, **kwargs
    ) -> "DynamoDBTable[DB_MODEL, DB_INDEX]":
        return cls(*args, **kwargs)


@dataclass
class DynamoDBEnvBaseTable(DynamoDBTable[DB_MODEL, DB_INDEX], Generic[DB_MODEL, DB_INDEX]):
    env_base: EnvBase = field(default_factory=EnvBase.from_env)

    @property
    def table_name(self) -> str:
        return self.env_base.get_table_name(super().table_name)

    def get_index_name(self, index: Optional[DB_INDEX] = None) -> Optional[str]:
        if (index_name := super().get_index_name(index)) is not None:
            return self.env_base.prefixed(index_name)
        return index_name
