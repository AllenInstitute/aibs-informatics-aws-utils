import moto
import requests

from aibs_informatics_aws_utils.auth import IamAWSRequestsAuth


def test__IamAWSRequestsAuth__call__works(aws_credentials_fixture):
    with moto.mock_aws():
        auth = IamAWSRequestsAuth()
        auth(
            requests.Request(
                method="GET",
                url="https://test.execute-api.us-west-2.amazonaws.com/prod",
                auth=auth,
            )
        )

        auth(
            requests.Request(
                method="GET",
                url="https://test.execute-api.us-west-2.amazonaws.com/prod",
                json={"test": "test"},
                auth=auth,
                headers={"test": "test"},
            )
        )
