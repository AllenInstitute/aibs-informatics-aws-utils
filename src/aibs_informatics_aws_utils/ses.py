import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService, get_region
from aibs_informatics_aws_utils.exceptions import AWSError

# from aibs_informatics_aws_utils.models.email_address import EmailAddress
EmailAddress = str

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ses.type_defs import (
        SendEmailRequestTypeDef,  # can be used as kwargs: SendEmailRequestTypeDef = {}, ses.send_email(**kwargs)
    )
    from mypy_boto3_ses.type_defs import (
        DestinationTypeDef,
        MessageTagTypeDef,
        MessageTypeDef,
        SendEmailResponseTypeDef,
        SourceTypeDef,
    )
else:
    (
        DestinationTypeDef,
        MessageTagTypeDef,
        MessageTypeDef,
        SendEmailRequestTypeDef,
        SendEmailResponseTypeDef,
    ) = (dict, dict, dict, dict, dict)


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
    source: EmailAddress,
    to_addresses: Sequence[EmailAddress],
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
