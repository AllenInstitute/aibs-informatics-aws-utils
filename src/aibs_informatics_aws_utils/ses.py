import logging
import mimetypes
from collections.abc import Sequence
from email.mime.multipart import MIMEMultipart
from email.mime.nonmultipart import MIMENonMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import TYPE_CHECKING, Any

from aibs_informatics_core.models.email_address import EmailAddress
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService, get_region
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:
    # TypeDefs can be used as kwargs:
    # Example 1: SendEmailRequestTypeDef = {}, ses.send_email(**kwargs)
    # Example 2: SendRawEmailRequestTypeDef = {}, ses.send_raw_email(**kwargs)
    from mypy_boto3_ses.type_defs import (
        DestinationTypeDef,
        MessageTagTypeDef,
        MessageTypeDef,
        SendEmailRequestTypeDef,
        SendEmailResponseTypeDef,
        SendRawEmailRequestTypeDef,
        SendRawEmailResponseTypeDef,
    )

    # 'Request' portion of name for RequestRequestTypeDefs is not accidentally repeated
    # See: https://youtype.github.io/boto3_stubs_docs/mypy_boto3_ses/type_defs/#sendrawemailrequestrequesttypedef
else:
    (
        SendEmailRequestTypeDef,
        SendEmailResponseTypeDef,
        SendRawEmailRequestTypeDef,
        SendRawEmailResponseTypeDef,
        DestinationTypeDef,
        MessageTagTypeDef,
        MessageTypeDef,
    ) = (dict, dict, dict, dict, dict, dict, dict)


logger = logging.getLogger(__name__)


get_ses_client = AWSService.SES.get_client


def verify_email_identity(email_address: str) -> dict[str, Any]:  # no type def?
    """Send a verification email to an email address.

    Initiates the email verification process for SES. The recipient will
    receive an email with a verification link.

    Args:
        email_address (str): The email address to verify.

    Returns:
        The SES verify email identity response.
    """
    ses = get_ses_client(region=get_region())
    response = ses.verify_email_identity(EmailAddress=email_address)
    return response


def is_verified(identity: str) -> bool:
    """Check if an email address or domain identity is verified in SES.

    Args:
        identity (str): The email address or domain to check.

    Raises:
        AWSError: If the verification status cannot be checked.

    Returns:
        True if the identity is verified, False otherwise.

    Example:
        ```python
        # Check email identity
        is_verified('myemail@subdomain.domain.com')

        # Check subdomain identity
        is_verified('subdomain.domain.com')

        # Check domain identity
        is_verified('domain.com')
        ```
    """
    ses = get_ses_client(region=get_region())
    try:
        response = ses.get_identity_verification_attributes(Identities=[identity])
        v_attrs = response["VerificationAttributes"]
        if identity not in v_attrs:
            return False
        return v_attrs[identity]["VerificationStatus"] == "Success"
    except ClientError as e:
        logger.exception(e)
        raise AWSError(f"Could not check verification status, error: {e}")


def send_email(request: SendEmailRequestTypeDef) -> SendEmailResponseTypeDef:
    """Send an email using SES.

    Args:
        request (SendEmailRequestTypeDef): The SES send email request configuration.

    Raises:
        AWSError: If the email cannot be sent.

    Returns:
        The send email response containing the message ID.
    """
    logger.info(f"Sending email request: {request}")
    ses = get_ses_client(region=get_region())

    try:
        response = ses.send_email(**request)
    except ClientError as e:
        logger.exception(e.response)
        raise AWSError(f"Could not send email, error: {e}, {e.response}")

    return response


def send_simple_email(
    source: str | EmailAddress,
    to_addresses: Sequence[str | EmailAddress],
    subject: str,
    body: str = "",
) -> SendEmailResponseTypeDef:
    """Send a simple text email using SES.

    Args:
        source (Union[str, EmailAddress]): The sender email address.
        to_addresses (Sequence[Union[str, EmailAddress]]): List of recipient addresses.
        subject (str): The email subject line.
        body (str): The email body text. Defaults to empty string.

    Returns:
        The send email response containing the message ID.
    """
    return send_email(
        SendEmailRequestTypeDef(
            Source=source,
            Destination={"ToAddresses": to_addresses},
            Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
        )
    )


def send_raw_email(request: SendRawEmailRequestTypeDef) -> SendRawEmailResponseTypeDef:
    """Send a raw MIME email using SES.

    Args:
        request (SendRawEmailRequestTypeDef): The SES send raw email request.

    Raises:
        AWSError: If the email cannot be sent.

    Returns:
        The send raw email response containing the message ID.
    """
    logger.info(f"Sending email request: {request}")
    ses = get_ses_client(region=get_region())

    try:
        response = ses.send_raw_email(**request)
    except ClientError as e:
        logger.exception(e.response)
        raise AWSError(f"Could not send email, error: {e}, {e.response}")

    return response


def send_email_with_attachment(
    source: str | EmailAddress,
    to_addresses: Sequence[str | EmailAddress],
    subject: str,
    body: str | MIMEText = "",
    attachments_paths: list[Path] | None = None,
) -> SendRawEmailResponseTypeDef:
    """Send an email with attachments using SES.

    Args:
        source (Union[str, EmailAddress]): The sender email address.
        to_addresses (Sequence[Union[str, EmailAddress]]): List of recipient addresses.
        subject (str): The email subject line.
        body (Union[str, MIMEText]): The email body (plain text or MIMEText for HTML).
        attachments_paths (Optional[List[Path]]): Optional list of file paths to attach.

    Returns:
        The send raw email response containing the message ID.
    """
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = source
    msg["To"] = ", ".join(to_addresses)

    msg_body = MIMEMultipart("alternative")
    if isinstance(body, str):
        msg_body.attach(MIMEText(body))
    else:
        msg_body.attach(body)
    msg.attach(msg_body)

    if attachments_paths is not None:
        for attachments_path in attachments_paths:
            attachment_obj = _construct_mime_attachment_from_path(path=attachments_path)
            msg.attach(attachment_obj)

    return send_raw_email(SendRawEmailRequestTypeDef(RawMessage={"Data": msg.as_string()}))


def _construct_mime_attachment_from_path(path: Path) -> MIMENonMultipart:
    """Constructs a MIME attachment from a `Path`"""
    mimetype, _ = mimetypes.guess_type(url=path)

    if mimetype is None:
        raise RuntimeError(f"Could not guess the MIME type for the file/object at: {path}")

    maintype, subtype = mimetype.split("/")

    filename = Path(path).name

    with open(path) as f:
        data = f.read()

    mime_obj = MIMENonMultipart(maintype, subtype)
    mime_obj.set_payload(data)
    mime_obj["Content-Type"] = mimetype
    mime_obj.add_header("Content-Disposition", "attachment", filename=filename)

    return mime_obj
