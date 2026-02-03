from copy import deepcopy
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Mapping, Optional, Union

from aibs_informatics_core.utils.logging import get_logger
from boto3.dynamodb.conditions import ConditionBase
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.dynamodb.conditions import (
    ConditionExpressionComponents,
    UpdateExpressionComponents,
)

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_dynamodb.type_defs import (
        BatchGetItemInputTypeDef,
        GetItemInputTypeDef,
        KeysAndAttributesTypeDef,
        PutItemOutputTableTypeDef,
        QueryInputTableQueryTypeDef,
        QueryInputTypeDef,
        ScanInputTypeDef,
    )
else:
    BatchGetItemInputTypeDef = dict
    GetItemInputRequestTypeDef = dict
    GetItemInputTypeDef = dict
    KeysAndAttributesTypeDef = dict
    PutItemOutputTableTypeDef = dict
    QueryInputTableQueryTypeDef = dict
    QueryInputTypeDef = dict
    ScanInputTypeDef = dict


logger = get_logger(__name__)


get_dynamodb_client = AWSService.DYNAMO_DB.get_client
get_dynamodb_resource = AWSService.DYNAMO_DB.get_resource


# ----------------------------------------------------------------------------
# Dynamo DB Table Methods
# ----------------------------------------------------------------------------
def table_put_item(
    table_name: str,
    item: Dict[str, Any],
    condition_expression: Optional[ConditionBase] = None,
    **kwargs,
) -> PutItemOutputTableTypeDef:
    """Put an item into a DynamoDB table.

    Args:
        table_name: Name of the table.
        item: Dictionary representing the item to put.
        condition_expression: Optional condition that must be satisfied for the put to succeed.
        **kwargs: Additional arguments passed to `table.put_item()`.

    Returns:
        Response from the put_item operation.
    """
    if condition_expression:
        parsed_expression = ConditionExpressionComponents.from_condition(
            condition_expression, False
        )
        kwargs["ConditionExpression"] = parsed_expression.condition_expression
        kwargs["ExpressionAttributeNames"] = parsed_expression.expression_attribute_names
        # Not always necessary to have ExpressionAttributeValues
        if parsed_expression.expression_attribute_values:
            kwargs["ExpressionAttributeValues"] = parsed_expression.expression_attribute_values

    # For more details on additional kwargs for table.put_item see:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
    table = table_as_resource(table_name)
    return table.put_item(Item=item, **kwargs)


def table_get_item(
    table_name: str, key: Mapping[str, Any], attrs: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Get a single item from a DynamoDB table.

    Args:
        table_name: Name of the table.
        key: Dictionary of key attribute(s) identifying the item to get.
        attrs: Optional projection expression specifying which attributes to retrieve.

    Returns:
        The item if found, None otherwise.
    """
    table = table_as_resource(table_name)
    props: GetItemInputRequestTypeDef = {"Key": key, "ReturnConsumedCapacity": "NONE"}  # type: ignore  # we modify use of this type (no table name is needed here)

    if attrs is not None:
        props["ProjectionExpression"] = attrs

    response = table.get_item(**props)  # type: ignore  # pylance complains about extra fields

    logger.info("Response from table.get_item: %s", response)

    return response.get("Item", None)


def table_get_items(
    table_name: str,
    keys: List[Mapping[str, Any]],
    attrs: Optional[str] = None,
    region: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Batch get multiple items from a DynamoDB table.

    Handles pagination automatically when more than 100 keys are provided.

    Args:
        table_name: Name of the table.
        keys: List of key dictionaries identifying the items to get.
        attrs: Optional projection expression specifying which attributes to retrieve.
        region: AWS region. Defaults to None (uses default region).

    Returns:
        List of items found.
    """
    db = get_dynamodb_client(region=region)
    serializer = TypeSerializer()

    items: List[Dict[str, Any]] = []

    # we receive an error if there are more than 100 calls
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_get_item
    MAX_KEYS_PER_API_CALL = 100

    keys_subset_list = [
        keys[i : i + MAX_KEYS_PER_API_CALL] for i in range(0, len(keys), MAX_KEYS_PER_API_CALL)
    ]
    for keys_subset in keys_subset_list:
        serialized_keys = [
            {key: serializer.serialize(value) for key, value in attr_value.items()}
            for attr_value in keys_subset
        ]

        request_items: Mapping[str, KeysAndAttributesTypeDef] = {
            table_name: {
                "Keys": serialized_keys,
            }
        }
        props: BatchGetItemInputTypeDef = {
            "RequestItems": request_items,
            "ReturnConsumedCapacity": "NONE",
        }

        if attrs is not None:
            for _, keys_and_attrs in request_items.items():
                keys_and_attrs["ProjectionExpression"] = attrs

        while True:
            response = db.batch_get_item(**props)
            for table_items in response["Responses"].values():
                items.extend(table_items)
            # Must make subsequent calls for Unprocessed keys if present.
            # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#DDB-BatchGetItem-response-UnprocessedKeys
            if response.get("UnprocessedKeys", None):
                props["RequestItems"] = response["UnprocessedKeys"]
            else:
                # If no more keys to process, break from while loop.
                break

    deserializer = TypeDeserializer()
    return [{k: deserializer.deserialize(v) for k, v in item.items()} for item in items]


def table_update_item(
    table_name: str,
    key: Mapping[str, Any],
    attributes: Mapping[str, Any],
    return_values: Literal["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"] = "NONE",
    **kwargs,
) -> Optional[Dict[str, Any]]:
    """Update an item in a DynamoDB table.

    Args:
        table_name: Name of the table.
        key: Dictionary of key attribute(s) identifying the item to update.
        attributes: Dictionary of attributes to update.
        return_values: What to return after the update. Options:

            - **NONE** - Nothing is returned.
            - **ALL_OLD** - Returns all attributes as they were before the update.
            - **UPDATED_OLD** - Returns only the updated attributes as they were before.
            - **ALL_NEW** - Returns all attributes as they are after the update.
            - **UPDATED_NEW** - Returns only the updated attributes as they are after.

        **kwargs: Additional arguments passed to `table.update_item()`.

    Returns:
        The attributes based on `return_values`, or None if `return_values` is "NONE".
    """
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
    table = table_as_resource(table_name)

    expression_components = UpdateExpressionComponents.from_dict(attributes=attributes)

    response = table.update_item(
        Key=key,
        ExpressionAttributeNames=expression_components.expression_attribute_names,
        ExpressionAttributeValues=expression_components.expression_attribute_values,
        UpdateExpression=expression_components.update_expression,
        ReturnValues=return_values,
        **kwargs,
    )

    logger.info(f"Response from table.update_item: {response}")

    return response.get("Attributes", None)


def table_delete_item(
    table_name: str,
    key: Mapping[str, Any],
    condition_expression: Optional[ConditionBase] = None,
    return_values: Literal["NONE", "ALL_OLD"] = "NONE",
    **kwargs,
) -> Optional[Dict[str, Any]]:
    """Delete an item from a DynamoDB table.

    Args:
        table_name: Name of the table.
        key: Dictionary of key attribute(s) identifying the item to delete.
        condition_expression: Optional condition that must be satisfied for the delete to succeed.
        return_values: What to return after the delete. Options:

            - **NONE** - Nothing is returned.
            - **ALL_OLD** - Returns all attributes of the deleted item.

        **kwargs: Additional arguments passed to `table.delete_item()`.

    Returns:
        The deleted item's attributes if `return_values` is "ALL_OLD", None otherwise.
    """
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/delete_item.html

    if condition_expression:
        parsed_expression = ConditionExpressionComponents.from_condition(
            condition_expression, False
        )
        kwargs["ConditionExpression"] = parsed_expression.condition_expression
        kwargs["ExpressionAttributeNames"] = parsed_expression.expression_attribute_names
        kwargs["ExpressionAttributeValues"] = parsed_expression.expression_attribute_values

    table = table_as_resource(table_name)
    response = table.delete_item(Key=key, ReturnValues=return_values, **kwargs)
    return response.get("Attributes", None)


def table_query(
    table_name: str,
    key_condition_expression: ConditionBase,
    index_name: Optional[str] = None,
    filter_expression: Optional[ConditionBase] = None,
    region: Optional[str] = None,
    consistent_read: bool = False,
) -> List[Dict[str, Any]]:
    """Query a DynamoDB table.

    Args:
        table_name: Name of the table.
        key_condition_expression: Key condition expression for the query.
        index_name: Index name. Defaults to None (query the main table).
        filter_expression: Filter expression to apply after the query. Defaults to None.
        region: AWS region. Defaults to None (uses default region).
        consistent_read: Whether a strongly consistent read should be used.
            Defaults to False.

            Note:
                Strongly consistent reads are not supported for global secondary indexes.

    Returns:
        List of items matching the query.
    """
    db = get_dynamodb_client(region=region)
    table = table_as_resource(table_name)

    key_expr_component = ConditionExpressionComponents.from_condition(
        key_condition_expression, True
    )
    expression_attribute_names = deepcopy(key_expr_component.expression_attribute_names)
    expression_attribute_values = deepcopy(
        key_expr_component.expression_attribute_values__serialized
    )

    db_request: QueryInputTypeDef = {
        "TableName": table.name,
        "KeyConditionExpression": key_expr_component.condition_expression,
    }

    # Handle when filter_expression is provided
    if filter_expression is not None:
        filter_expr_component = ConditionExpressionComponents.from_condition(
            filter_expression, False
        )
        # For queries, there is a possibility that key/sort and filter expression
        # attribute names could collide
        clean_filter_expr_components = key_expr_component.fix_collisions(filter_expr_component)
        expression_attribute_names.update(clean_filter_expr_components.expression_attribute_names)
        expression_attribute_values.update(
            clean_filter_expr_components.expression_attribute_values__serialized
        )
        db_request["FilterExpression"] = clean_filter_expr_components.condition_expression

    # ExpressionAttributeNames/Values should always exist for `query`
    db_request["ExpressionAttributeNames"] = expression_attribute_names
    if expression_attribute_values:
        db_request["ExpressionAttributeValues"] = expression_attribute_values

    # Handle a query/scan involving an index (GSI/LSI)
    if index_name is not None:
        db_request["IndexName"] = index_name
    if index_name is not None and consistent_read is True:
        logger.warning(
            f"Strongly consistent reads are NOT supported for secondary indices "
            f"like '{index_name}' for the table '{table_name}'. Ignoring provided "
            "`consistent_read` value and treating it as False!"
        )
    else:
        db_request["ConsistentRead"] = consistent_read

    items: List[Dict[str, Any]] = []
    paginator = db.get_paginator("query")
    logger.info(f"Performing DB 'query' on {table.name} with following parameters: {db_request}")
    for i, response in enumerate(paginator.paginate(**db_request)):  # type: ignore  # pylance complains about extra fields
        new_items = response.get("Items", [])
        items.extend(new_items)
        logger.debug(f"Iter #{i + 1}: item count from table. Query: {len(new_items)}")
    logger.info(f"Complete item count from table. Query (after any filtering): {len(items)}")

    deserializer = TypeDeserializer()
    return [{k: deserializer.deserialize(v) for k, v in item.items()} for item in items]


def table_scan(
    table_name: str,
    index_name: Optional[str] = None,
    filter_expression: Optional[ConditionBase] = None,
    region: Optional[str] = None,
    consistent_read: bool = False,
) -> List[Dict[str, Any]]:
    """Scan a DynamoDB table.

    Args:
        table_name: Name of the table.
        index_name: Index name. Defaults to None (scan the main table).
        filter_expression: Filter expression to apply. Defaults to None.
        region: AWS region. Defaults to None (uses default region).
        consistent_read: Whether a strongly consistent read should be used.
            Defaults to False.

            Note:
                Strongly consistent reads are not supported for secondary indexes.

    Returns:
        List of items from the scan.
    """

    db = get_dynamodb_client(region=region)
    table = table_as_resource(table_name)

    db_request: ScanInputTypeDef = {"TableName": table.name}

    # Handle when filter_expression is provided
    if filter_expression is not None:
        filter_expr_component = ConditionExpressionComponents.from_condition(
            filter_expression, False
        )
        expression_attribute_names = deepcopy(filter_expr_component.expression_attribute_names)
        expression_attribute_values = deepcopy(
            filter_expr_component.expression_attribute_values__serialized
        )

        db_request["FilterExpression"] = filter_expr_component.condition_expression
        db_request["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values:
            db_request["ExpressionAttributeValues"] = expression_attribute_values

    # Handle a scan involving an index (GSI/LSI)
    if index_name is not None:
        db_request["IndexName"] = index_name
    if index_name is not None and consistent_read is True:
        logger.warning(
            f"Strongly consistent reads are NOT supported for secondary indices "
            f"like '{index_name}' for the table '{table_name}'. Ignoring provided"
            " `consistent_read` value and treating it as False!"
        )
    else:
        db_request["ConsistentRead"] = consistent_read

    items: List[Dict[str, Any]] = []
    paginator = db.get_paginator("scan")
    logger.info(f"Performing DB 'scan' on {table.name} with following parameters: {db_request}")
    for i, response in enumerate(paginator.paginate(**db_request)):  # type: ignore  # pylance complains about extra fields
        new_items = response.get("Items", [])
        items.extend(new_items)
        logger.debug(f"Iter #{i + 1}: item count from table. Scan: {len(new_items)}")
    logger.info(
        f"Complete item count from table. Scan results (after any filtering): {len(items)}"
    )

    deserializer = TypeDeserializer()
    return [{k: deserializer.deserialize(v) for k, v in item.items()} for item in items]


def table_get_key_schema(table_name: str) -> Dict[str, str]:
    """Get the key schema for a DynamoDB table.

    Args:
        table_name: Name of the table.

    Returns:
        Mapping of key type to attribute name.
        Example: `{"HASH": "partition_key_name", "RANGE": "sort_key_name"}`
    """
    table = table_as_resource(table_name)
    return {k["KeyType"]: k["AttributeName"] for k in table.key_schema}


def execute_partiql_statement(
    statement: str, region: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Execute a PartiQL statement against DynamoDB.

    Handles pagination automatically to retrieve all results.

    Args:
        statement: The PartiQL statement to execute.
        region: AWS region. Defaults to None (uses default region).

    Returns:
        List of items returned by the statement.
    """
    db = get_dynamodb_client(region=region)

    response = db.execute_statement(Statement=statement)
    results = response["Items"]

    while response.get("NextToken", None):
        response = db.execute_statement(Statement=statement, NextToken=response["NextToken"])
        results.extend(response["Items"])
    return results


def table_as_resource(table: str, region: Optional[str] = None):
    """Get a DynamoDB Table resource.

    Args:
        table: Name of the table.
        region: AWS region. Defaults to None (uses default region).

    Returns:
        A boto3 DynamoDB Table resource.
    """
    db = get_dynamodb_resource(region=region)
    return db.Table(table)


def convert_decimals_to_floats(item: Dict[str, Any], in_place: bool = True) -> Dict[str, Any]:
    """Convert all Decimal values in a dictionary to floats.

    DynamoDB returns numeric values as Decimal objects. This function recursively
    converts them to standard Python floats.

    Args:
        item: Dictionary potentially containing Decimal values.
        in_place: If True, modify the dictionary in place. If False, create a copy first.
            Defaults to True.

    Returns:
        The dictionary with all Decimals converted to floats.
    """

    def _convert_decimals_to_floats(obj: Union[Dict[str, Any], List[Any]]):
        if isinstance(obj, list):
            for i in range(len(obj)):
                if isinstance(obj[i], Decimal):
                    obj[i] = float(obj[i])
                elif isinstance(obj[i], (dict, list)):
                    _convert_decimals_to_floats(obj[i])
        elif isinstance(obj, dict):
            for k in obj:
                if isinstance(obj[k], Decimal):
                    obj[k] = float(obj[k])
                elif isinstance(obj[k], (dict, list)):
                    _convert_decimals_to_floats(obj[k])

    if not in_place:
        item = deepcopy(item)
    _convert_decimals_to_floats(item)
    return item


def convert_floats_to_decimals(item: Dict[str, Any], in_place: bool = True) -> Dict[str, Any]:
    """Convert all float values in a dictionary to Decimals.

    DynamoDB requires numeric values to be Decimal objects. This function recursively
    converts Python floats to Decimals for storage.

    Args:
        item: Dictionary potentially containing float values.
        in_place: If True, modify the dictionary in place. If False, create a copy first.
            Defaults to True.

    Returns:
        The dictionary with all floats converted to Decimals.
    """

    def _convert_floats_to_decimals(obj: Union[Dict[str, Any], List[Any]]):
        if isinstance(obj, list):
            for i in range(len(obj)):
                if isinstance(obj[i], float):
                    obj[i] = Decimal(str(obj[i]))
                elif isinstance(obj[i], (dict, list)):
                    _convert_floats_to_decimals(obj[i])
        elif isinstance(obj, dict):
            for k in obj:
                if isinstance(obj[k], float):
                    obj[k] = Decimal(str(obj[k]))
                elif isinstance(obj[k], (dict, list)):
                    _convert_floats_to_decimals(obj[k])

    if not in_place:
        item = deepcopy(item)
    _convert_floats_to_decimals(item)
    return item
