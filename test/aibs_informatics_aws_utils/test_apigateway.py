from test.aibs_informatics_aws_utils.base import AwsBaseTest

import moto
from pytest import raises

from aibs_informatics_aws_utils import apigateway
from aibs_informatics_aws_utils.apigateway import get_rest_api, get_rest_api_endpoint
from aibs_informatics_aws_utils.exceptions import ResourceNotFoundError


class ApiGatewayTests(AwsBaseTest):
    def setUp(self) -> None:
        return super().setUp()


def test__get_rest_api__fails_if_no_api_found(aws_credentials_fixture):
    with moto.mock_aws():
        with raises(ResourceNotFoundError):
            get_rest_api("non-existent-api")


def test__get_rest_api__works(aws_credentials_fixture):
    with moto.mock_aws():
        apigateway_client = apigateway.get_apigateway_client()
        apigateway_client.create_rest_api(name="test-api")
        api = get_rest_api("test-api")
        assert api["name"] == "test-api"


def test__get_rest_api_endpoint__works(aws_credentials_fixture):
    with moto.mock_aws():
        apigateway_client = apigateway.get_apigateway_client()
        apigateway_client.create_rest_api(name="test-api")
        api = get_rest_api("test-api")
        endpoint = get_rest_api_endpoint(api)
        assert endpoint == f"https://{api['id']}.execute-api.us-west-2.amazonaws.com/prod"
