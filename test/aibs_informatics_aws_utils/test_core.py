from unittest import mock

import moto
import pytest
from botocore.config import Config

from aibs_informatics_aws_utils.core import (
    AWSService,
    get_account_id,
    get_client,
    get_iam_arn,
    get_region,
    get_resource,
    get_user_id,
)
from aibs_informatics_aws_utils.exceptions import AWSError
from test.aibs_informatics_aws_utils.base import AwsBaseTest


class AWSGetterTests(AwsBaseTest):
    @mock.patch("aibs_informatics_aws_utils.core.Session")
    def test__get_region__valid_user_input_and_returns_valid_str(self, mock_session):
        mock_session.return_value.region_name = None
        self.assertEqual(get_region("us-east-2"), "us-east-2")

    @mock.patch("aibs_informatics_aws_utils.core.Session")
    def test__get_region__falls_back_to_boto3_Session_and_returns_valid_str(self, mock_session):
        mock_session.return_value.region_name = "us-east-2"
        self.assertEqual(get_region(), "us-east-2")

    @mock.patch("aibs_informatics_aws_utils.core.Session")
    def test__get_region__falls_back_to_env_var_AWS_REGION_and_returns_valid_str(
        self, mock_session
    ):
        mock_session.return_value.region_name = None
        self.set_env_vars(
            ("AWS_REGION", "us-west-2"), ("AWS_DEFAULT_REGION", None), ("REGION", None)
        )
        self.assertEqual(get_region(), "us-west-2")
        # Show that AWS_REGION superscedes any other value
        self.set_env_vars(("REGION", "us-west-1"))
        self.assertEqual(get_region(), "us-west-2")

    @mock.patch("aibs_informatics_aws_utils.core.Session")
    def test__get_region__falls_back_to_env_var_REGION_and_returns_valid_str(self, mock_session):
        mock_session.return_value.region_name = None
        self.set_env_vars(
            ("REGION", "us-west-2"), ("AWS_DEFAULT_REGION", None), ("AWS_REGION", None)
        )
        self.assertEqual(get_region(), "us-west-2")
        # Show that AWS_REGION superscedes any other value
        self.set_env_vars(("AWS_REGION", "us-west-1"))
        self.assertEqual(get_region(), "us-west-1")

    @mock.patch("aibs_informatics_aws_utils.core.Session")
    def test__get_region__invalid_user_input_raises_error(self, mock_session):
        mock_session.return_value.region_name = None
        self.assertRaises(AWSError, get_region, "not-a-region")

    @mock.patch("aibs_informatics_aws_utils.core.Session")
    def test__get_region__user_input_and_no_fallbacks_raises_error(self, mock_session):
        mock_session.return_value.region_name = None
        self.set_env_vars(("AWS_REGION", None), ("AWS_DEFAULT_REGION", None), ("REGION", None))
        self.assertRaises(AWSError, get_region)

    def test__get_account_id__succeeds(self):
        # moto sets account to be 123456789012
        with moto.mock_aws():
            self.assertEqual(get_account_id(), self.ACCOUNT_ID)

    def test__get_user_id__succeeds(self):
        # moto sets user to be AKIAIOSFODNN7EXAMPLE
        with moto.mock_aws():
            self.assertEqual(get_user_id(), "AKIAIOSFODNN7EXAMPLE")

    def test__get_iam_arn__succeeds(self):
        # moto sets user to be arn:aws:sts::123456789012:user/moto
        with moto.mock_aws():
            self.assertEqual(get_iam_arn(), "arn:aws:sts::123456789012:user/moto")


class AWSServiceTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region()
        self.get_client = self.create_patch("aibs_informatics_aws_utils.core.get_client")
        self.get_resource = self.create_patch("aibs_informatics_aws_utils.core.get_resource")

    def test__get_client__gets_service_clients(self):
        AWSService.API_GATEWAY.get_client()
        AWSService.DYNAMO_DB.get_client()
        AWSService.ECR.get_client()
        AWSService.S3.get_client()
        AWSService.STEPFUNCTIONS.get_client()
        AWSService.SQS.get_client()

        self.get_client.assert_has_calls(
            [
                mock.call("apigateway", region=None),
                mock.call("dynamodb", region=None),
                mock.call("ecr", region=None),
                mock.call("s3", region=None),
                mock.call("stepfunctions", region=None),
                mock.call("sqs", region=None),
            ]
        )

    def test__get_resource__gets_service_resources(self):
        AWSService.S3.get_resource()
        AWSService.DYNAMO_DB.get_resource()
        AWSService.SQS.get_resource()

        self.get_resource.assert_has_calls(
            [
                mock.call("s3", region=None),
                mock.call("dynamodb", region=None),
                mock.call("sqs", region=None),
            ]
        )


@pytest.mark.parametrize(
    "service, preexisting_config, expected_retries_config",
    [
        pytest.param(
            # service
            "s3",
            # preexisting_config
            None,
            # expected_retries_config
            {"total_max_attempts": 7, "mode": "standard"},
            id="Basic test case (no preexisting_config provided)",
        ),
        pytest.param(
            # service
            "dynamodb",
            # preexisting_config
            Config(retries={"max_attempts": 8, "mode": "adaptive"}),
            # expected_retries_config
            {"total_max_attempts": 9, "mode": "adaptive"},
            id="Test preexisting_config doesn't get overridden by default",
        ),
    ],
)
def test___core__get_client__config_setup_properly(
    aws_credentials_fixture, service, preexisting_config, expected_retries_config
):
    if preexisting_config:
        client = get_client(service=service, config=preexisting_config)
    else:
        client = get_client(service=service)

    assert expected_retries_config == client._client_config.retries


@pytest.mark.parametrize(
    "service, preexisting_config, expected_retries_config",
    [
        pytest.param(
            # service
            "s3",
            # preexisting_config
            None,
            # expected_retries_config
            {"total_max_attempts": 7, "mode": "standard"},
            id="Basic test case (no preexisting_config provided)",
        ),
        pytest.param(
            # service
            "dynamodb",
            # preexisting_config
            Config(retries={"max_attempts": 8, "mode": "adaptive"}),
            # expected_retries_config
            {"total_max_attempts": 9, "mode": "adaptive"},
            id="Test preexisting_config doesn't get overridden by default",
        ),
    ],
)
def test___core__get_resource__config_setup_properly(
    aws_credentials_fixture, service, preexisting_config, expected_retries_config
):
    if preexisting_config:
        resource = get_resource(service=service, config=preexisting_config)
    else:
        resource = get_resource(service=service)

    assert expected_retries_config == resource.meta.client._client_config.retries
