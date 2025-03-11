import json
import logging
from typing import Optional, Type

from aibs_informatics_core.utils.json import DecimalEncoder
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService, get_region
from aibs_informatics_aws_utils.exceptions import AWSError

logger = logging.getLogger(__name__)


get_sqs_client = AWSService.SQS.get_client
get_sqs_resource = AWSService.SQS.get_resource


def delete_from_queue(queue_name: str, receipt_handle: str, region: Optional[str] = None):
    sqs = get_sqs_client()
    queue_url_response = sqs.get_queue_url(QueueName=queue_name)
    queue_url = queue_url_response["QueueUrl"]

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    logger.info("Deleted message %s form queue %s", receipt_handle, queue_name)

    return True


def send_to_dispatch_queue(payload: dict, env_base: str):
    sqs = get_sqs_client(region=get_region())
    queue_name = "-".join([env_base, "demand_request_queue"])
    logger.info("Queue name: %s", queue_name)

    try:
        queue_url_response = sqs.get_queue_url(QueueName=queue_name)
    except ClientError as e:
        logger.exception(e)
        raise AWSError(f"Could not find SQS queue with name {queue_name}")

    queue_url = queue_url_response["QueueUrl"]

    response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload))

    return response["MD5OfMessageBody"]


def send_sqs_message(
    queue_name: str,
    payload: dict,
    message_deduplication_id: Optional[str] = None,
    message_group_id: Optional[str] = None,
    payload_json_encoder: Type[json.JSONEncoder] = DecimalEncoder,
) -> str:
    """Send a message to an SQS queue by providing a queue name

    Args:
        queue_name (str): The name of the queue that you want to send a message to.
            (e.g. 'aps-sync-request-queue.fifo')
        payload (dict): A dictionary representing the message payload you would like to send.
        message_deduplication_id (Optional[str], optional): An ID that can be used by SQS
            to remove messages that have the same deduplication_id. Do not set if your
            SQS queue already uses content based deduplication. Defaults to None.
        message_group_id (Optional[str], optional): Required for FIFO queues.
            Messages sent with the same message_group_id will obey FIFO rules. Messages with
            different message_group_ids may be interleaved. Defaults to None.
        payload_json_encoder (Type[json.JSONEncoder], optional): The JSONEncoder
            class that should be used to covert the input `payload` dictionary into
            a json string. By default uses a DecimalEncoder which can handle decimal.Decimal types.

    Raises:
        AWSError: If the provided queue_name cannot be resolved to an SQS url.
            HINT: Does the code calling this function have the correct SQS permissions?
        RuntimeError: If the destination queue is a FIFO queue, then `message_group_id` MUST
            be provided.

    Returns:
        str: Returns an MD5 digest of the send message body.
    """
    sqs = get_sqs_client(region=get_region())
    try:
        queue_url_response = sqs.get_queue_url(QueueName=queue_name)
    except ClientError:
        raise AWSError(
            f"Could not find SQS queue with name: {queue_name}. "
            "Does the code calling send_sqs_message() have sqs:GetQueueUrl permissions?"
        )

    send_sqs_message_args = {
        "QueueUrl": queue_url_response["QueueUrl"],
        "MessageBody": json.dumps(payload, cls=payload_json_encoder),
    }

    if message_group_id is not None:
        send_sqs_message_args["MessageGroupId"] = message_group_id
    else:
        if queue_name.endswith(".fifo"):
            raise RuntimeError("SQS messages for a FIFO queue *must* include a message_group_id!")

    if message_deduplication_id is not None:
        send_sqs_message_args["MessageDeduplicationId"] = message_deduplication_id

    response = sqs.send_message(**send_sqs_message_args)  # type: ignore  # complains about valid kwargs

    return response["MD5OfMessageBody"]
