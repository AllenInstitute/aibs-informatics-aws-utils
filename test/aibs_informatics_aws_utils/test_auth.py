import requests
from moto import mock_apigateway, mock_sts

from aibs_informatics_aws_utils.auth import IamAWSRequestsAuth


def test__IamAWSRequestsAuth__call__works(aws_credentials_fixture):
    with mock_sts(), mock_apigateway():
        auth = IamAWSRequestsAuth()
        request = auth(
            requests.Request(
                method="GET",
                url="https://test.execute-api.us-west-2.amazonaws.com/prod",
                auth=auth,
            )
        )

        request = auth(
            requests.Request(
                method="GET",
                url="https://test.execute-api.us-west-2.amazonaws.com/prod",
                json={"test": "test"},
                auth=auth,
                headers={"test": "test"},
            )
        )
