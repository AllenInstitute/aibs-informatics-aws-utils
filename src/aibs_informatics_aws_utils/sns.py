"""
gcsutils.sqs
Contains utility functions for interacting with SQS
"""
import logging
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService, get_region
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_sns.type_defs import PublishInputRequestTypeDef, PublishResponseTypeDef
else:
    PublishInputRequestTypeDef, PublishResponseTypeDef = dict, dict


logger = logging.getLogger(__name__)


get_sns_client = AWSService.SNS.get_client
get_sns_resource = AWSService.SNS.get_resource


def publish_to_topic(request: PublishInputRequestTypeDef) -> PublishResponseTypeDef:
    sns = get_sns_client(region=get_region())
    logger.info("PublishInputRequest: %s", request)

    try:
        publish_response = sns.publish(**request)
    except ClientError as e:
        logger.exception(e)
        raise AWSError(f"Could not publish message using request parameters: {request}")

    return publish_response
