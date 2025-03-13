import tempfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import moto
from aibs_informatics_core.models.email_address import EmailAddress
from pytest import raises

from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_aws_utils.ses import (
    _construct_mime_attachment_from_path,
    get_ses_client,
    is_verified,
    send_email,
    send_email_with_attachment,
    send_raw_email,
    send_simple_email,
    verify_email_identity,
)
from test.aibs_informatics_aws_utils.base import AwsBaseTest

if TYPE_CHECKING:
    from mypy_boto3_ses.type_defs import (
        SendEmailRequestRequestTypeDef,
        SendRawEmailRequestRequestTypeDef,
    )
else:
    SendEmailRequestRequestTypeDef = dict
    SendRawEmailRequestRequestTypeDef = dict


class SesTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.US_WEST_2)
        self.source = EmailAddress("test_email@fake_address.com")
        self.destination1 = EmailAddress("test_email1@fake_address.com")

    @property
    def ses_client(self):
        return get_ses_client()

    def test__is_verified_true(self):
        with moto.mock_aws():
            self.verify_email(self.source)
            assert is_verified(self.source)

    def test__is_verified_false(self):
        with moto.mock_aws():
            assert not is_verified(self.source)

    def test__send_email__succeeds(self):
        with moto.mock_aws():
            self.verify_email(self.source)
            self.verify_email(self.destination1)
            response = send_email(
                SendEmailRequestRequestTypeDef(
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
        with moto.mock_aws():
            self.verify_email(self.source)
            self.verify_email(self.destination1)
            response = send_simple_email(
                source=self.source, to_addresses=[self.destination1], subject="subject_line"
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test__send_simple_email__fails_unverfied(self):
        with moto.mock_aws():
            with raises(AWSError):
                send_simple_email(
                    source=self.source, to_addresses=[self.destination1], subject="subject_line"
                )

    def test__verify_email_identity__succeeds(self):
        with moto.mock_aws():
            response = verify_email_identity(self.source)
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def verify_email(self, email_address: EmailAddress) -> None:
        self.ses_client.verify_email_identity(EmailAddress=email_address)

    def test__send_raw_email__succeeds(self):
        with moto.mock_aws():
            self.verify_email(self.source)
            self.verify_email(self.destination1)

            msg = MIMEMultipart("mixed")
            msg["Subject"] = "subject_line"
            msg["From"] = EmailAddress(self.source)
            msg["To"] = ", ".join([EmailAddress(self.destination1)])
            msg_body = MIMEMultipart("alternative")
            msg_body.attach(MIMEText("body_paragraph"))

            response = send_raw_email(
                request=SendRawEmailRequestRequestTypeDef(RawMessage={"Data": msg.as_string()})
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @patch("aibs_informatics_aws_utils.ses._construct_mime_attachment_from_path")
    def test__send_email_with_attachment__succeeds(self, mock_construct_mime_attachment_obj):
        mime_obj = MIMEText("foo", "csv")
        mime_obj.add_header("Content-Disposition", "attachment", filename="foo.csv")
        mock_construct_mime_attachment_obj.return_value = mime_obj

        with moto.mock_aws():
            self.verify_email(self.source)
            self.verify_email(self.destination1)
            response = send_email_with_attachment(
                source=EmailAddress(self.source),
                to_addresses=[EmailAddress(self.destination1)],
                subject="subject_line",
                body="body_paragraph",
                attachments_paths=[Path("foo.csv")],
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    @moto.mock_aws
    def test__construct_mime_attachment_from_path__csv(self):
        filename = "foo.csv"
        payload = """foo,bar
        1,2
        """

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = Path(tmp_dir) / filename
            with open(tmp_file, "w") as f:
                f.write(payload)
            mime_obj = _construct_mime_attachment_from_path(path=tmp_file)

        assert mime_obj.get_content_type() == "text/csv"
        assert mime_obj.get_payload() == payload
        assert mime_obj.get_filename() == filename
        assert mime_obj.get_content_disposition() == "attachment"
