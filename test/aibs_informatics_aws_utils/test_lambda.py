import json

import boto3
import moto
from pytest import raises

from aibs_informatics_aws_utils.lambda_ import (
    get_lambda_function_file_systems,
    get_lambda_function_url,
)
from test.aibs_informatics_aws_utils.base import AwsBaseTest


@moto.mock_aws
class LambdaTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_aws_credentials()

    def get_role_arn(self) -> str:
        return boto3.client("iam").create_role(
            RoleName="foo",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "lambda.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )["Role"]["Arn"]

    @moto.mock_aws(config={"lambda": {"use_docker": False}})
    def test__get_lambda_function_file_systems__no_file_systems(self):
        # Set up lambda
        lambda_client = boto3.client("lambda")
        file_system_configs = [
            {
                "Arn": "arn:aws:elasticfilesystem:us-west-2:123456789012:access-point/fsap-1234abcd",  # noqa: E501
                "LocalMountPath": "/mnt/efs1",
            },
            {
                "Arn": "arn:aws:elasticfilesystem:us-west-2:123456789012:access-point/fsap-1234abcd2",  # noqa: E501
                "LocalMountPath": "/mnt/efs2",
            },
        ]
        lambda_client.create_function(
            FunctionName="test",
            Runtime="python3.8",
            Handler="test",
            Role=self.get_role_arn(),
            Code={"ZipFile": b"bar"},
            Environment={"Variables": {"TEST": "test"}},
            # NOTE: FileSystemConfigs is not supported by moto yet, so this is meaningless
            FileSystemConfigs=file_system_configs,
        )
        # HACK: moto doesn't support FileSystemConfigs yet, so we have to patch it in
        #       here we will fetch the actual response and then add the FileSystemConfigs
        response = lambda_client.get_function_configuration(FunctionName="test")
        with self.stub(lambda_client) as lambda_stubber:
            lambda_stubber.add_response(
                "get_function_configuration",
                {
                    **response,
                    **{"FileSystemConfigs": file_system_configs},
                },
                expected_params={"FunctionName": "test"},
            )
            self.create_patch(
                "aibs_informatics_aws_utils.lambda_.get_lambda_client", return_value=lambda_client
            )

            actual_file_system_configs = get_lambda_function_file_systems("test")
            self.assertListEqual(actual_file_system_configs, file_system_configs)

    @moto.mock_aws(config={"lambda": {"use_docker": False}})
    def test__get_lambda_function_url__with_url(self):
        lambda_client = boto3.client("lambda")
        lambda_client.create_function(
            FunctionName="test",
            Runtime="python3.8",
            Handler="test",
            Role=self.get_role_arn(),
            Code={"ZipFile": b"bar"},
            Environment={"Variables": {"TEST": "test"}},
        )
        response = lambda_client.create_function_url_config(
            FunctionName="test", AuthType="AWS_IAM"
        )

        assert get_lambda_function_url("test") == response["FunctionUrl"]

    @moto.mock_aws(config={"lambda": {"use_docker": False}})
    def test__get_lambda_function_url__handles_no_url(self):
        lambda_client = boto3.client("lambda")
        lambda_client.create_function(
            FunctionName="test",
            Runtime="python3.8",
            Handler="test",
            Role=self.get_role_arn(),
            Code={"ZipFile": b"bar"},
            Environment={"Variables": {"TEST": "test"}},
        )

    @moto.mock_aws(config={"lambda": {"use_docker": False}})
    def test__get_lambda_function_url__handles_invalid_function_name(self):
        with raises(ValueError):
            get_lambda_function_url("@#$#$@#$@#$")
