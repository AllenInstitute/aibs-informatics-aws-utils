from moto import mock_athena, mock_sts
from pytest import fixture, raises

from aibs_informatics_aws_utils.athena import (
    get_athena_client,
    get_query_execution,
    query_waiter,
    start_query_execution,
)
from aibs_informatics_aws_utils.exceptions import AWSError


@fixture(scope="function")
def athena_client(aws_credentials_fixture):
    with mock_sts(), mock_athena():
        athena_client = get_athena_client()
        yield athena_client


def test__start_query_execution__works(athena_client):
    athena_client.create_work_group(Name="test")

    query_string = "SELECT * FROM table"
    metadata = start_query_execution(
        query_string, work_group="test", execution_parameters=["test"]
    )


def test__start_query_execution__fails(athena_client):
    query_string = "SELECT * FROM table"
    with raises(AWSError):

        start_query_execution(query_string, work_group="test", execution_parameters=["test"])


def test__get_query_execution__works(athena_client):

    query_string = "SELECT * FROM table"
    metadata = start_query_execution(query_string, execution_parameters=["test"])

    assert metadata["QueryExecutionId"] is not None

    query_execution = get_query_execution(metadata["QueryExecutionId"])
    assert (
        query_execution["QueryExecution"].get("QueryExecutionId") == metadata["QueryExecutionId"]
    )


def test__get_query_execution__fails(athena_client):
    with raises(AWSError):
        get_query_execution("non-existent-query-id")


def test__query_waiter__works(athena_client):
    query_string = "SELECT * FROM table"
    metadata = start_query_execution(query_string, execution_parameters=["test"])

    assert metadata["QueryExecutionId"] is not None

    query_waiter(metadata["QueryExecutionId"])
