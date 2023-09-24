from test.base import BaseTest

from botocore.client import BaseClient, ClientError
from botocore.stub import Stubber


class AwsBaseTest(BaseTest):
    ACCOUNT_ID = "123456789012"
    US_EAST_1 = "us-east-1"
    US_WEST_2 = "us-west-2"

    @property
    def DEFAULT_REGION(self) -> str:
        return self.US_WEST_2

    @property
    def DEFAULT_SECRET_KEY(self) -> str:
        return "A" * 20

    @property
    def DEFAULT_ACCESS_KEY(self) -> str:
        return "A" * 20

    def set_region(self, region: str = None):
        self.set_env_vars(
            ("AWS_REGION", region or self.DEFAULT_REGION),
            ("AWS_DEFAULT_REGION", region or self.DEFAULT_REGION),
        )

    def set_credentials(self, access_key: str = None, secret_key: str = None):
        self.set_env_vars(
            ("AWS_ACCESS_KEY_ID", access_key or self.DEFAULT_ACCESS_KEY),
            ("AWS_SECRET_ACCESS_KEY", secret_key or self.DEFAULT_SECRET_KEY),
            ("AWS_SECURITY_TOKEN", "testing"),
            ("AWS_SESSION_TOKEN", "testing"),
        )

    def set_account_id(self, account_id: str = None):
        self.set_env_vars(("AWS_ACCOUNT_ID", account_id or self.ACCOUNT_ID))

    def set_aws_credentials(self):
        self.set_credentials(access_key="testing", secret_key="testing")
        self.set_region()
        self.set_account_id()

    def stub(self, client: BaseClient) -> Stubber:
        return Stubber(client=client)
