import json
import logging

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService, get_region
from aibs_informatics_aws_utils.exceptions import AWSError

logger = logging.getLogger(__name__)


get_sqs_client = AWSService.SQS.get_client
get_sqs_resource = AWSService.SQS.get_resource


def delete_from_queue(queue_name: str, receipt_handle: str, region: str = None):
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
