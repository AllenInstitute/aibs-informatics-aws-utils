import json
from dataclasses import dataclass
from unittest import mock

import boto3
import moto
import requests
from aibs_informatics_core.models.base import DataClassModel
from pytest import raises

from aibs_informatics_aws_utils.lambda_ import (
    call_lambda_function_url,
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


@dataclass
class _DummyModel(DataClassModel):
    str_value: str
    int_value: int


@mock.patch("aibs_informatics_aws_utils.lambda_.requests.request")
@mock.patch("aibs_informatics_aws_utils.lambda_.IamAWSRequestsAuth")
@mock.patch("aibs_informatics_aws_utils.lambda_.get_lambda_function_url")
def test__call_lambda_function_url__dict_payload_returns_json(
    mock_get_url, mock_auth_cls, mock_request
):
    function_name = "my-function"
    mock_get_url.return_value = "https://abc.lambda-url.us-west-2.on.aws/my-path"
    mock_auth = mock.Mock(name="auth")
    mock_auth_cls.return_value = mock_auth
    mock_response = mock.Mock(
        ok=True,
        headers={"Content-Type": "application/json"},
    )
    mock_response.json.return_value = {"result": "ok"}
    mock_response.text = "ignored"
    mock_request.return_value = mock_response

    result = call_lambda_function_url(function_name, payload={"key": "value"}, region="us-west-2")

    assert result == {"result": "ok"}
    assert str(mock_get_url.call_args[0][0]) == function_name
    mock_auth_cls.assert_called_once_with(service_name="lambda")
    expected_payload = json.dumps({"key": "value"})
    mock_request.assert_called_once_with(
        method="POST",
        url="https://abc.lambda-url.us-west-2.on.aws/my-path",
        json=expected_payload,
        params={},
        headers={},
        auth=mock_auth,
    )


def test__call_lambda_function_url__direct_url_with_custom_headers_and_query():
    function_url = "https://abc.lambda-url.us-west-2.on.aws/my-path?foo=bar&foo=baz&single=value"
    mock_response = mock.Mock(ok=True, headers={"Content-Type": "text/plain"}, text="done")
    headers = {"X-Test": "1"}
    auth = object()
    with (
        mock.patch("aibs_informatics_aws_utils.lambda_.get_lambda_function_url") as mock_get_url,
        mock.patch(
            "aibs_informatics_aws_utils.lambda_.requests.request", return_value=mock_response
        ) as mock_request,
    ):
        result = call_lambda_function_url(function_url, headers=headers, auth=auth, timeout=3)

    assert result == "done"
    mock_get_url.assert_not_called()
    kwargs = mock_request.call_args.kwargs
    assert kwargs["method"] == "GET"
    assert kwargs["url"] == "https://abc.lambda-url.us-west-2.on.aws/my-path"
    assert kwargs["params"] == {"foo": ["bar", "baz"], "single": ["value"]}
    assert kwargs["headers"] is headers
    assert kwargs["auth"] is auth
    assert kwargs["timeout"] == 3


@mock.patch("aibs_informatics_aws_utils.lambda_.requests.request")
@mock.patch("aibs_informatics_aws_utils.lambda_.IamAWSRequestsAuth")
@mock.patch("aibs_informatics_aws_utils.lambda_.get_lambda_function_url")
def test__call_lambda_function_url__model_and_bytes_payload(
    mock_get_url, mock_auth_cls, mock_request
):
    mock_get_url.return_value = "https://abc.lambda-url.us-west-2.on.aws/"
    mock_auth_cls.return_value = mock.Mock()
    mock_response = mock.Mock(ok=True, headers={"Content-Type": "application/json"})
    mock_response.json.return_value = {"status": "ok"}
    mock_response.text = "ignored"
    mock_request.return_value = mock_response
    payload = _DummyModel(str_value="value", int_value=7)

    result = call_lambda_function_url("test-function", payload=payload)

    assert result == {"status": "ok"}
    first_call_kwargs = mock_request.call_args.kwargs
    assert first_call_kwargs["method"] == "POST"
    assert first_call_kwargs["json"] == json.dumps({"str_value": "value", "int_value": 7})

    mock_response.headers["Content-Type"] = "text/plain"
    mock_response.text = "text response"

    result_text = call_lambda_function_url(
        "https://abc.lambda-url.us-west-2.on.aws/", payload=b"hi"
    )

    assert result_text == "text response"
    second_call_kwargs = mock_request.call_args.kwargs
    assert second_call_kwargs["json"] == "hi"


@mock.patch("aibs_informatics_aws_utils.lambda_.get_lambda_function_url", return_value=None)
def test__call_lambda_function_url__missing_function_raises(mock_get_url):
    with raises(ValueError) as excinfo:
        call_lambda_function_url("missing-function")
    assert "Function missing-function not found" in str(excinfo.value)
    mock_get_url.assert_called_once()


@mock.patch(
    "aibs_informatics_aws_utils.lambda_.get_lambda_function_url",
    return_value="https://abc.lambda-url.us-west-2.on.aws/",
)
def test__call_lambda_function_url__invalid_payload_raises(mock_get_url):
    with raises(ValueError) as excinfo:
        call_lambda_function_url("test-function", payload=object())
    assert "Invalid payload type" in str(excinfo.value)
    mock_get_url.assert_called_once()


def test__call_lambda_function_url__invalid_function_identifier():
    with raises(ValueError) as excinfo:
        call_lambda_function_url("not a valid value")
    assert "Invalid function name or url" in str(excinfo.value)


@mock.patch("aibs_informatics_aws_utils.lambda_.requests.request")
@mock.patch("aibs_informatics_aws_utils.lambda_.IamAWSRequestsAuth")
@mock.patch(
    "aibs_informatics_aws_utils.lambda_.get_lambda_function_url",
    return_value="https://abc.lambda-url.us-west-2.on.aws/",
)
def test__call_lambda_function_url__propagates_http_error(
    mock_get_url, mock_auth_cls, mock_request
):
    mock_auth_cls.return_value = mock.Mock()
    http_error = requests.HTTPError("boom")
    mock_response = mock.Mock(ok=False)
    mock_response.raise_for_status.side_effect = http_error
    mock_request.return_value = mock_response

    with raises(requests.HTTPError) as excinfo:
        call_lambda_function_url("test-function")
    assert excinfo.value is http_error
    mock_request.assert_called_once()
