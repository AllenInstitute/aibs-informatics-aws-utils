__all__ = [
    "get_account_id",
    "get_client",
    "get_client_error_code",
    "get_client_error_message",
    "get_region",
    "get_resource",
    "get_session",
    "AWSService",
    "AWS_REGION_VAR",
]
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Literal, Optional, TypeVar, Union, cast

import boto3
from aibs_informatics_core.models.aws.core import AWSRegion
from aibs_informatics_core.models.aws.iam import IAMArn, UserId
from aibs_informatics_core.utils.decorators import cache
from boto3 import Session
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient, ClientError
from botocore.config import Config
from botocore.session import Session as BotocoreSession

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_apigateway import APIGatewayClient
    from mypy_boto3_athena import AthenaClient
    from mypy_boto3_batch import BatchClient
    from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
    from mypy_boto3_ec2 import EC2Client, EC2ServiceResource
    from mypy_boto3_ecr import ECRClient
    from mypy_boto3_ecs import ECSClient
    from mypy_boto3_efs import EFSClient
    from mypy_boto3_fsx import FSxClient
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_logs import CloudWatchLogsClient
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_secretsmanager import SecretsManagerClient
    from mypy_boto3_ses import SESClient
    from mypy_boto3_sns import SNSClient, SNSServiceResource
    from mypy_boto3_sqs import SQSClient, SQSServiceResource
    from mypy_boto3_ssm import SSMClient
    from mypy_boto3_stepfunctions import SFNClient
    from mypy_boto3_sts import STSClient
    from mypy_boto3_sts.type_defs import GetCallerIdentityResponseTypeDef
else:
    AthenaClient = object
    APIGatewayClient = object
    BatchClient = object
    CloudWatchLogsClient = object
    DynamoDBClient, DynamoDBServiceResource = object, object
    EC2Client, EC2ServiceResource = object, object
    ECRClient = object
    ECSClient = object
    EFSClient = object
    FSxClient = object
    GetCallerIdentityResponseTypeDef = dict
    LambdaClient = object
    S3Client, S3ServiceResource = object, object
    SecretsManagerClient = object
    SESClient = object
    SNSClient, SNSServiceResource = object, object
    SQSClient, SQSServiceResource = object, object
    SSMClient = object
    STSClient = object
    SFNClient = object


from aibs_informatics_aws_utils.exceptions import AWSError

logger = logging.getLogger(__name__)  # type: logging.Logger


# ----------------------------------------------------------------------------
# AWS Session / Account / Region utilties
# ----------------------------------------------------------------------------


AWS_REGION_VAR = "AWS_REGION"


def get_session(session: Optional[Union[Session, BotocoreSession]] = None) -> Session:
    if not session:
        return Session()
    elif isinstance(session, BotocoreSession):
        return Session(botocore_session=session)
    else:
        return session


def get_region(region: Optional[str] = None) -> str:
    """Get and sanitize region value

    Will retrieve the current region from a newly created boto3 session.
    This is helpful for getting the region dynamically within a Lambda
    function or from the environment if that is not available.

    Logic of priority for resolving region:
        * User input
        * boto3.Session
        * ENV VAR "AWS_REGION"
        * ENV VAR "REGION"

    Subsequently, the region value is validated against a regex

    Args:
        region (str, optional): user provided region. Defaults to None.

    Raises:
        ApplicationException:
            - If region cannot be resolved
            - If region is not correct format

    Returns:
        str: AWS Region
    """

    # If not provided, check session
    if not region:
        session = Session()
        region = session.region_name
    # If session does not resolve, check environment variables
    if not region:
        # Attempt to get it from an environment variable.
        ENV_VARS = ["AWS_REGION", "REGION"]
        logger.debug(
            "Could not resolve region from session. "
            f"Looking for following env variables: {ENV_VARS}"
        )
        region = next((os.environ.get(key) for key in ENV_VARS if os.environ.get(key)), None)

    if not region:
        error_msg = "Could not determine region from default session or environment"
        logger.error(error_msg)
        raise AWSError(error_msg)
    try:
        region = AWSRegion(region)
        assert region is not None  # mollify mypy
    except Exception as e:
        raise AWSError from e
    return region


def get_account_id() -> str:
    """Will get the account id from the current credentials/identity"""
    return get_caller_identity()["Account"]


def get_user_id() -> UserId:
    return UserId(get_caller_identity()["UserId"])


def get_iam_arn() -> IAMArn:
    return IAMArn(get_caller_identity()["Arn"])


def get_caller_identity() -> GetCallerIdentityResponseTypeDef:
    return boto3.client("sts").get_caller_identity()


# ----------------------------------------------------------------------------
# Common utilities
# ----------------------------------------------------------------------------


def get_client_error_code(client_error: ClientError) -> str:
    return client_error.response.get("Error", {}).get("Code", "Unknown")


def get_client_error_message(client_error: ClientError) -> str:
    return client_error.response.get("Error", {}).get("Message", "Unknown")


def client_error_code_check(client_error: ClientError, *error_codes: str) -> bool:
    return get_client_error_code(client_error) in error_codes


# ----------------------------------------------------------------------------
# AWS Client / Resource Utilities
# ----------------------------------------------------------------------------

Resources = Literal[
    "dynamodb",
    "ec2",
    "s3",
    "sns",
    "sqs",
]
Clients = Literal[
    "athena",
    "apigateway",
    "batch",
    "dynamodb",
    "ec2",
    "ecr",
    "ecs",
    "efs",
    "fsx",
    "lambda",
    "logs",
    "s3",
    "secretsmanager",
    "ses",
    "sns",
    "sqs",
    "ssm",
    "sts",
    "stepfunctions",
]
Services = Literal[Clients, Resources]


ClientType = TypeVar("ClientType", bound=BaseClient)
ResourceType = TypeVar("ResourceType", bound=ServiceResource)


@cache
def get_client(
    service: Clients,
    session: Optional[boto3.Session] = None,
    region: Optional[str] = None,
    **kwargs,
):
    """Get a boto3 client object
    Notes:

    - If session is not provided, then the default session is used
    - Order of priority for region is
      1. region parameter
      2. "region_name" in **kwargs
      3. fallback to whatever is fetched in get_region

    Args:
        service (Clients): The intended service to build client for
        session (boto3.Session, optional): A customized session object. Defaults to None.
        region (Optional[str], optional): An explicit region. Defaults to None.

    Returns:
        A boto3 Client object
    """
    region_name = get_region(region=region or kwargs.get("region_name"))
    if region_name:
        kwargs["region_name"] = region_name

    # If config for our client is not set, we want to set it to use "standard" mode
    # (default is "legacy") and increase the number of retries to 5 (default is 3)
    # See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#available-retry-modes
    config: Optional[Config] = kwargs.pop("config", None)
    default_config = Config(
        connect_timeout=120, read_timeout=120, retries={"max_attempts": 6, "mode": "standard"}
    )
    if config is None:
        config = default_config
    else:
        # Have values in pre-existing config (if it exists) take precedence over default_config
        config = default_config.merge(other_config=config)

    session = session or boto3.Session()
    return session.client(service, config=config, **kwargs)


@cache
def get_resource(
    service: Resources,
    session: Optional[boto3.Session] = None,
    region: Optional[str] = None,
    **kwargs,
):
    """Get a boto3 resource object
    Notes:

    - If session is not provided, then the default session is used
    - Order of priority for region is
      1. region parameter
      2. "region_name" in **kwargs
      3. fallback to whatever is fetched in get_region

    Args:
        service (Resources): The intended service to build resource from
        session (boto3.Session, optional): A customized session object. Defaults to None.
        region (Optional[str], optional): An explicit region. Defaults to None.

    Returns:
        A ServiceResource object
    """
    region_name = get_region(region=region or kwargs.get("region_name"))
    if region_name:
        kwargs["region_name"] = region_name

    # If config for our client is not set, we want to set it to use "standard" mode
    # (default is "legacy") and increase the number of retries to 5 (default is 3)
    # See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#available-retry-modes
    config: Optional[Config] = kwargs.pop("config", None)
    default_config = Config(
        connect_timeout=120, read_timeout=120, retries={"max_attempts": 6, "mode": "standard"}
    )
    if config is None:
        config = default_config
    else:
        # Have values in pre-existing config (if it exists) take precedence over default_config
        config = default_config.merge(other_config=config)

    session = session or boto3.Session()
    return session.resource(service, config=config, **kwargs)


@dataclass
class AWSServiceProvider(Generic[ClientType]):
    service_name: Services

    def get_client(self, region: Optional[str] = None, **kwargs) -> ClientType:
        return cast(ClientType, get_client(self.service_name, region=region, **kwargs))


@dataclass
class AWSServiceAndResourceProvider(
    AWSServiceProvider[ClientType], Generic[ClientType, ResourceType]
):
    service_name: Resources

    def get_resource(self, region: Optional[str] = None, **kwargs) -> ResourceType:
        return cast(ResourceType, get_resource(self.service_name, region=region, **kwargs))


class AWSService:
    ATHENA = AWSServiceProvider[AthenaClient]("athena")
    API_GATEWAY = AWSServiceProvider[APIGatewayClient]("apigateway")
    BATCH = AWSServiceProvider[BatchClient]("batch")
    DYNAMO_DB = AWSServiceAndResourceProvider[DynamoDBClient, DynamoDBServiceResource]("dynamodb")
    EC2 = AWSServiceAndResourceProvider[EC2Client, EC2ServiceResource]("ec2")
    ECR = AWSServiceProvider[ECRClient]("ecr")
    ECS = AWSServiceProvider[ECSClient]("ecs")
    EFS = AWSServiceProvider[EFSClient]("efs")
    FSX = AWSServiceProvider[FSxClient]("fsx")
    LAMBDA = AWSServiceProvider[LambdaClient]("lambda")
    LOGS = AWSServiceProvider[CloudWatchLogsClient]("logs")
    S3 = AWSServiceAndResourceProvider[S3Client, S3ServiceResource]("s3")
    SECRETSMANAGER = AWSServiceProvider[SecretsManagerClient]("secretsmanager")
    SES = AWSServiceProvider[SESClient]("ses")
    SNS = AWSServiceAndResourceProvider[SNSClient, SNSServiceResource]("sns")
    SQS = AWSServiceAndResourceProvider[SQSClient, SQSServiceResource]("sqs")
    SSM = AWSServiceProvider[SSMClient]("ssm")
    STS = AWSServiceProvider[STSClient]("sts")
    STEPFUNCTIONS = AWSServiceProvider[SFNClient]("stepfunctions")
