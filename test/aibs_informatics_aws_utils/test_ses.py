from test.aibs_informatics_aws_utils.base import AwsBaseTest

import moto
from aibs_informatics_core.models.email_address import EmailAddress
from pytest import mark, param, raises

from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_aws_utils.ses import (
    SendEmailRequestTypeDef,
    get_ses_client,
    is_verified,
    send_email,
    send_simple_email,
    verify_email_identity,
)


class SesTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.US_WEST_2)
        self.source = "test_email@fake_address.com"
        self.destination1 = "test_email1@fake_address.com"

    @property
    def ses_client(self):
        return get_ses_client()

    def test__is_verified_true(self):
        with moto.mock_ses():
            self.verify_email(self.source)
            assert is_verified(self.source)

    def test__is_verified_false(self):
        with moto.mock_ses():
            assert not is_verified(self.source)

    def test__send_email__succeeds(self):
        with moto.mock_ses():
            self.verify_email(self.source)
            self.verify_email(self.destination1)
            response = send_email(
                SendEmailRequestTypeDef(
                    Source=self.source,
                    Destination={"ToAddresses": [self.destination1]},
                    Message={
                        "Subject": {"Data": "subject_line"},
                        "Body": {"Text": {"Data": "body_paragraph"}},
                    },
                )
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test__send_simple_email__succeeds(self):
        with moto.mock_ses():
            self.verify_email(self.source)
            self.verify_email(self.destination1)
            response = send_simple_email(
                source=self.source, to_addresses=[self.destination1], subject="subject_line"
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test__send_simple_email__fails_unverfied(self):
        with moto.mock_ses():
            with raises(AWSError):
                response = send_simple_email(
                    source=self.source, to_addresses=[self.destination1], subject="subject_line"
                )

    def test__verify_email_identity__succeeds(self):
        with moto.mock_ses():
            response = verify_email_identity(self.source)
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def verify_email(self, email_address: EmailAddress) -> None:
        self.ses_client.verify_email_identity(EmailAddress=email_address)
