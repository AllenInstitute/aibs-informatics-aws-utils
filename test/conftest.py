import os
from unittest import mock

import moto
import pytest
from aibs_informatics_core.models.aws.s3 import S3URI

from aibs_informatics_aws_utils.s3 import get_s3_client


@pytest.fixture(scope="function")
def aws_credentials_fixture():
    """Set testing credentials for mocked AWS resources and
    avoid accidentally hitting anything live with boto3.
    """
    # Clear os.environ dict (will be restored after fixture is finished)
    with mock.patch.dict(os.environ, clear=True):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
        os.environ["AWS_REGION"] = "us-west-2"
        yield


@pytest.fixture
def s3_client_fixture(aws_credentials_fixture):
    with moto.mock_s3():
        mock_s3_client = get_s3_client()
        yield mock_s3_client


@pytest.fixture
def s3_object_fixture(s3_client_fixture, request) -> S3URI:
    mock_bucket_name = request.param.get("mock_bucket_name", "test-bucket")
    mock_bucket_location = request.param.get("mock_bucket_location", "us-west-2")
    mock_object_key = request.param.get("mock_object_key", "test_object.txt")
    mock_object_contents = request.param.get("mock_object_contents", "test contents")
    mock_object_storage_class = request.param.get("mock_object_storage_class", "STANDARD")

    mock_s3_client = s3_client_fixture
    mock_s3_client.create_bucket(
        Bucket=mock_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": mock_bucket_location},
    )

    mock_s3_client.put_object(
        Bucket=mock_bucket_name,
        Key=mock_object_key,
        Body=mock_object_contents,
        StorageClass=mock_object_storage_class,
    )

    yield S3URI.build(bucket_name=mock_bucket_name, key=mock_object_key)
