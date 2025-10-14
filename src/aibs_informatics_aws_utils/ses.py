import logging
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.nonmultipart import MIMENonMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

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


def verify_email_identity(email_address: str) -> Dict[str, Any]:  # no type def?
    """
    TODO: replace with send_custom_verification_email-> SendCustomVerificationEmailResponseTypeDef:
    """
    ses = get_ses_client(region=get_region())
    response = ses.verify_email_identity(EmailAddress=email_address)
    return response


def is_verified(identity: str) -> bool:
    """Checks if email address or domain identity is valid

    Examples:
        email identitiy:    `is_verified(myemail@subdomain.domain.com)`
        subdomain identity  `is_verified(subdomain.domain.com)`
        domain idenitity    `is_verified(domain.com)`

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
    logger.info(f"Sending email request: {request}")
    ses = get_ses_client(region=get_region())

    try:
        response = ses.send_email(**request)
    except ClientError as e:
        logger.exception(e.response)
        raise AWSError(f"Could not send email, error: {e}, {e.response}")

    return response


def send_simple_email(
    source: Union[str, EmailAddress],
    to_addresses: Sequence[Union[str, EmailAddress]],
    subject: str,
    body: str = "",
) -> SendEmailResponseTypeDef:
    return send_email(
        SendEmailRequestTypeDef(
            Source=source,
            Destination={"ToAddresses": to_addresses},
            Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
        )
    )


def send_raw_email(request: SendRawEmailRequestTypeDef) -> SendRawEmailResponseTypeDef:
    logger.info(f"Sending email request: {request}")
    ses = get_ses_client(region=get_region())

    try:
        response = ses.send_raw_email(**request)
    except ClientError as e:
        logger.exception(e.response)
        raise AWSError(f"Could not send email, error: {e}, {e.response}")

    return response


def send_email_with_attachment(
    source: Union[str, EmailAddress],
    to_addresses: Sequence[Union[str, EmailAddress]],
    subject: str,
    body: Union[str, MIMEText] = "",
    attachments_paths: Optional[List[Path]] = None,
) -> SendRawEmailResponseTypeDef:
    """
    Args:
        source: Source email address
        to_addresses: List of recipient email addresses
        subject: Email subject
        body: Email body which can be either basic str or MIMEText, which can
            allow html with hyperlinks.
        attachments_paths: List of optional paths to read contents from and attach to the email
    Returns: `SendEmailResponseTypeDef`
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
