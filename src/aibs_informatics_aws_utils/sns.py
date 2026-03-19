__all__ = [
    "get_sns_client",
    "get_sns_resource",
    "publish_to_topic",
    "publish",
]

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_sns.type_defs import (
        MessageAttributeValueTypeDef,
        PublishInputTypeDef,
        PublishResponseTypeDef,
    )
else:
    PublishResponseTypeDef = dict
    MessageAttributeValueTypeDef = dict
    PublishInputTypeDef = dict


logger = logging.getLogger(__name__)


get_sns_client = AWSService.SNS.get_client
get_sns_resource = AWSService.SNS.get_resource


def publish(
    message: str,
    topic_arn: str | None = None,
    target_arn: str | None = None,
    phone_number: str | None = None,
    subject: str | None = None,
    message_structure: str | None = None,
    message_attributes: Mapping[str, MessageAttributeValueTypeDef] | None = None,
    message_deduplication_id: str | None = None,
    message_group_id: str | None = None,
    region: str | None = None,
) -> PublishResponseTypeDef:
    """Publish a message to an SNS topic, endpoint, or phone number.

    At least one of topic_arn, target_arn, or phone_number must be provided.

    Args:
        message (str): The message to publish.
        topic_arn (Optional[str]): Optional SNS topic ARN to publish to.
        target_arn (Optional[str]): Optional endpoint ARN (e.g., mobile push).
        phone_number (Optional[str]): Optional phone number to send SMS to.
        subject (Optional[str]): Optional subject for email endpoints.
        message_structure (Optional[str]): Message structure for multi-format messages.
        message_attributes (Optional[Mapping]): Optional message attributes.
        message_deduplication_id (Optional[str]): Deduplication ID for FIFO topics.
        message_group_id (Optional[str]): Message group ID for FIFO topics.
        region (Optional[str]): AWS region. Defaults to None.

    Raises:
        AWSError: If none of topic_arn, target_arn, or phone_number is provided,
            or if publishing fails.

    Returns:
        The publish response containing the message ID.
    """
    if topic_arn is None and target_arn is None and phone_number is None:
        raise AWSError("Must provide either a topic_arn, target_arn, or phone_number")
    sns = get_sns_client(region=region)
    logger.info(
        f"Publishing message: {message} with subject: {subject}, "
        f"to topic: {topic_arn}, target: {target_arn}, phone_number: {phone_number}"
    )
    request: PublishInputTypeDef = PublishInputTypeDef(Message=message)
    if topic_arn:
        request["TopicArn"] = topic_arn
    if target_arn:
        request["TargetArn"] = target_arn
    if phone_number:
        request["PhoneNumber"] = phone_number
    if message:
        request["Message"] = message
    if subject:
        request["Subject"] = subject
    if message_structure:
        request["MessageStructure"] = message_structure
    if message_attributes:
        request["MessageAttributes"] = message_attributes
    if message_deduplication_id:
        request["MessageDeduplicationId"] = message_deduplication_id
    if message_group_id:
        request["MessageGroupId"] = message_group_id
    try:
        publish_response = sns.publish(**request)
    except ClientError as e:
        logger.exception(e)
        raise AWSError(f"Could not publish message using request parameters: {request}")
    return publish_response


def publish_to_topic(
    message: str,
    topic_arn: str,
    subject: str | None = None,
    message_structure: str | None = None,
    message_attributes: Mapping[str, MessageAttributeValueTypeDef] | None = None,
    message_deduplication_id: str | None = None,
    message_group_id: str | None = None,
    region: str | None = None,
) -> PublishResponseTypeDef:
    """Publish a message to an SNS topic.

    This is a convenience wrapper around publish() for topic-based publishing.

    Args:
        message (str): The message to publish.
        topic_arn (str): The SNS topic ARN to publish to.
        subject (Optional[str]): Optional subject for email endpoints.
        message_structure (Optional[str]): Message structure for multi-format messages.
        message_attributes (Optional[Mapping]): Optional message attributes.
        message_deduplication_id (Optional[str]): Deduplication ID for FIFO topics.
        message_group_id (Optional[str]): Message group ID for FIFO topics.
        region (Optional[str]): AWS region. Defaults to None.

    Returns:
        The publish response containing the message ID.
    """
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
