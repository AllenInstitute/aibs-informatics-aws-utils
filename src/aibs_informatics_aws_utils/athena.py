import logging
import time

# For Python 3.11+
from typing import TYPE_CHECKING, Literal, Unpack

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_athena.type_defs import (
        GetQueryExecutionOutputTypeDef,
        QueryExecutionStatusTypeDef,
        QueryExecutionTypeDef,
        StartQueryExecutionInputTypeDef,
        StartQueryExecutionOutputTypeDef,
    )
else:
    GetQueryExecutionOutputTypeDef = dict
    QueryExecutionStatusTypeDef = dict
    QueryExecutionTypeDef = dict
    StartQueryExecutionInputTypeDef = dict
    StartQueryExecutionOutputTypeDef = dict


ATHENA_QUERY_WAITER_STATUS = Literal["SUCCEEDED", "FAILED", "CANCELLED", "TIMEOUT"]

logger = logging.getLogger(__name__)

get_athena_client = AWSService.ATHENA.get_client


def start_query_execution(
    query_string: str,
    work_group: str | None = None,
    execution_parameters: list[str] | None = None,
    **kwargs: Unpack[StartQueryExecutionInputTypeDef],
) -> StartQueryExecutionOutputTypeDef:
    """Start an Athena query execution.

    Args:
        query_string (str): The SQL query string to execute.
        work_group (Optional[str]): The name of the workgroup to execute the query in.
        execution_parameters (Optional[List[str]]): Optional list of query execution parameters.
        **kwargs: Additional arguments passed to the Athena start_query_execution API.

    Raises:
        AWSError: If the query execution fails to start.

    Returns:
        The start query execution response containing the QueryExecutionId.
    """
    athena = get_athena_client()

    request = StartQueryExecutionInputTypeDef(QueryString=query_string)
    if work_group:
        request["WorkGroup"] = work_group
    if execution_parameters:
        request["ExecutionParameters"] = execution_parameters
    request.update(kwargs)
    try:
        metadata = athena.start_query_execution(**request)
        return metadata
    except ClientError as e:
        logger.error(f"Error executing : {request} {e}", exc_info=True)
        raise AWSError(f"Error starting query execution: {request} {e}") from e


def get_query_execution(query_execution_id: str) -> GetQueryExecutionOutputTypeDef:
    """Get the status and details of an Athena query execution.

    Args:
        query_execution_id (str): The unique identifier of the query execution.

    Raises:
        AWSError: If the query execution cannot be retrieved.

    Returns:
        The query execution details including status and results location.
    """
    athena = get_athena_client()
    try:
        return athena.get_query_execution(QueryExecutionId=query_execution_id)
    except Exception as e:
        logger.error(f"Error executing : {query_execution_id} {e}", exc_info=True)
        raise AWSError(f"Error starting query execution: {query_execution_id} {e}") from e


def query_waiter(
    query_execution_id: str, timeout: int = 60
) -> tuple[ATHENA_QUERY_WAITER_STATUS, QueryExecutionStatusTypeDef]:
    """Wait for an Athena query to complete.

    Polls the query execution status until it reaches a terminal state
    (SUCCEEDED, FAILED, CANCELLED) or times out.

    Args:
        query_execution_id (str): The unique identifier of the query execution.
        timeout (int): Maximum time to wait in seconds. Defaults to 60.

    Returns:
        A tuple of (status, status_details) where status is one of
            SUCCEEDED, FAILED, CANCELLED, or TIMEOUT.
    """
    start = time.time()
    logger.info(f"Polling for status of query execution: {query_execution_id}")
    while True:
        stats = get_query_execution(query_execution_id=query_execution_id)
        logger.info(f"Query Execution Status: {stats}")
        status = stats["QueryExecution"].get("Status", {})
        state = status.get("State")
        if state in ["SUCCEEDED", "FAILED", "CANCELLED", "TIMEOUT"]:
            return state, status  # type: ignore[return-value]
        time.sleep(0.2)  # 200ms
        # Exit if the time waiting exceed the timeout seconds
        if time.time() > start + timeout:
            return "TIMEOUT", status
