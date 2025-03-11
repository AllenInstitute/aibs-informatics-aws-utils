__all__ = [
    "get_sns_client",
    "get_sns_resource",
    "publish_to_topic",
    "publish",
    "PublishInputRequestTypeDef",
]

import logging
from typing import TYPE_CHECKING, Mapping, Optional

from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_sns.type_defs import (
        MessageAttributeValueTypeDef,
        PublishInputRequestTypeDef,
        PublishResponseTypeDef,
    )
else:
    PublishInputRequestTypeDef, PublishResponseTypeDef, MessageAttributeValueTypeDef = (
        dict,
        dict,
        dict,
    )


logger = logging.getLogger(__name__)


get_sns_client = AWSService.SNS.get_client
get_sns_resource = AWSService.SNS.get_resource


def publish(
    message: str,
    topic_arn: Optional[str] = None,
    target_arn: Optional[str] = None,
    phone_number: Optional[str] = None,
    subject: Optional[str] = None,
    message_structure: Optional[str] = None,
    message_attributes: Optional[Mapping[str, MessageAttributeValueTypeDef]] = None,
    message_deduplication_id: Optional[str] = None,
    message_group_id: Optional[str] = None,
    region: Optional[str] = None,
) -> PublishResponseTypeDef:
    if topic_arn is None and target_arn is None and phone_number is None:
        raise AWSError("Must provide either a topic_arn, target_arn, or phone_number")
    sns = get_sns_client(region=region)
    logger.info(
        f"Publishing message: {message} with subject: {subject}, "
        f"to topic: {topic_arn}, target: {target_arn}, phone_number: {phone_number}"
    )
    request = dict(
        TopicArn=topic_arn,
        TargetArn=target_arn,
        PhoneNumber=phone_number,
        Message=message,
        Subject=subject,
        MessageStructure=message_structure,
        MessageAttributes=message_attributes,
        MessageDeduplicationId=message_deduplication_id,
        MessageGroupId=message_group_id,
    )
    try:
        publish_response = sns.publish(**remove_null_values(request))
    except ClientError as e:
        logger.exception(e)
        raise AWSError(f"Could not publish message using request parameters: {request}")
    return publish_response


def publish_to_topic(
    message: str,
    topic_arn: str,
    subject: Optional[str] = None,
    message_structure: Optional[str] = None,
    message_attributes: Optional[Mapping[str, MessageAttributeValueTypeDef]] = None,
    message_deduplication_id: Optional[str] = None,
    message_group_id: Optional[str] = None,
    region: Optional[str] = None,
) -> PublishResponseTypeDef:
    return publish(
        message=message,
        topic_arn=topic_arn,
        subject=subject,
        message_structure=message_structure,
        message_attributes=message_attributes,
        message_deduplication_id=message_deduplication_id,
        message_group_id=message_group_id,
        region=region,
    )
