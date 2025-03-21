import logging
import time
from typing import TYPE_CHECKING, List, Literal, Optional, Tuple

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_athena.type_defs import (
        GetQueryExecutionInputRequestTypeDef,
        GetQueryExecutionOutputTypeDef,
        QueryExecutionStatusTypeDef,
        QueryExecutionTypeDef,
        StartQueryExecutionInputRequestTypeDef,
        StartQueryExecutionOutputTypeDef,
    )
else:
    GetQueryExecutionInputRequestTypeDef = dict
    GetQueryExecutionOutputTypeDef = dict
    QueryExecutionStatusTypeDef = dict
    QueryExecutionTypeDef = dict
    StartQueryExecutionInputRequestTypeDef = dict
    StartQueryExecutionOutputTypeDef = dict


ATHENA_QUERY_WAITER_STATUS = Literal["SUCCEEDED", "FAILED", "CANCELLED", "TIMEOUT"]

logger = logging.getLogger(__name__)

get_athena_client = AWSService.ATHENA.get_client


def start_query_execution(
    query_string: str,
    work_group: Optional[str] = None,
    execution_parameters: Optional[List[str]] = None,
    **kwargs,
) -> StartQueryExecutionOutputTypeDef:
    athena = get_athena_client()

    request = StartQueryExecutionInputRequestTypeDef(QueryString=query_string)
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
    athena = get_athena_client()
    try:
        return athena.get_query_execution(QueryExecutionId=query_execution_id)
    except Exception as e:
        logger.error(f"Error executing : {query_execution_id} {e}", exc_info=True)
        raise AWSError(f"Error starting query execution: {query_execution_id} {e}") from e


def query_waiter(
    query_execution_id: str, timeout: int = 60
) -> Tuple[ATHENA_QUERY_WAITER_STATUS, QueryExecutionStatusTypeDef]:
    start = time.time()
    logger.info(f"Polling for status of query execution: {query_execution_id}")
    while True:
        stats = get_query_execution(query_execution_id=query_execution_id)
        logger.info(f"Query Execution Status: {stats}")
        status = stats["QueryExecution"].get("Status", {})
        state = status.get("State")
        if state and state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return state, status
        time.sleep(0.2)  # 200ms
        # Exit if the time waiting exceed the timeout seconds
        if time.time() > start + timeout:
            return "TIMEOUT", status
