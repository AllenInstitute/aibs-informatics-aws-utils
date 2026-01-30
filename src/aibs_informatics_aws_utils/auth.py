from typing import Optional

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.compat import parse_qsl, urlparse
from botocore.session import Session
from requests.auth import AuthBase

from aibs_informatics_aws_utils.core import get_session


class IamAWSRequestsAuth(AuthBase):
    """IAM authorizer for signing HTTP requests with AWS SigV4.

    This class can be used with the `requests` library to automatically sign
    HTTP requests using AWS IAM credentials.

    Args:
        session (Optional[Session]): Optional botocore Session object for credentials.
        service_name (str): The AWS service name for signing. Defaults to "execute-api".

    Example:
        ```python
        auth = IamAWSRequestsAuth()
        response = requests.get(url, auth=auth)

        # With custom session and service
        auth = IamAWSRequestsAuth(boto3.Session(), 'execute-api')
        ```
    """

    def __init__(self, session: Optional[Session] = None, service_name: str = "execute-api"):
        self.boto3_session = get_session(session)
        credentials = self.boto3_session.get_credentials()
        if not credentials:
            raise ValueError("No AWS credentials found")

        self.sigv4 = SigV4Auth(
            credentials=credentials.get_frozen_credentials(),
            service_name=service_name,
            region_name=self.boto3_session.region_name,
        )

    def __call__(self, request):
        # Parse request URL
        url = urlparse(request.url)

        # Prepare AWS request
        awsrequest = AWSRequest(
            method=request.method,
            url=f"{url.scheme}://{url.netloc}{url.path}",
            data=request.body if hasattr(request, "body") else (request.json or request.data),
            params=dict(parse_qsl(url.query)),
        )

        # Sign request
        self.sigv4.add_auth(awsrequest)

        # Re-add original headers
        for key, val in request.headers.items():
            if key not in awsrequest.headers:
                awsrequest.headers[key] = val

        # Return prepared request
        return awsrequest.prepare()
