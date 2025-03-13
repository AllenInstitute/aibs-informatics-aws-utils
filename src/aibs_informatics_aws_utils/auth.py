from typing import Optional

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.compat import parse_qsl, urlparse
from botocore.session import Session
from requests.auth import AuthBase

from aibs_informatics_aws_utils.core import get_session


class IamAWSRequestsAuth(AuthBase):
    """
    IAM authorizer.

    :param boto3.Session session: Optional boto3 Session object
    :param str service_name: Optional AWS service name

    :Example:

    >>> IAMAuth()
    >>> IAMAuth(boto3.Session(), 'execute-api')
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
