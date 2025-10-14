import logging
from typing import TYPE_CHECKING, List

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ecs.type_defs import (
        DescribeContainerInstancesRequestTypeDef,
        DescribeContainerInstancesResponseTypeDef,
    )
else:
    DescribeContainerInstancesRequestTypeDef = dict
    DescribeContainerInstancesResponseTypeDef = dict


logger = logging.getLogger(__name__)

get_ecs_client = AWSService.ECS.get_client


def ecs_describe_container_instances(
    container_instances: List[str], cluster: str, **kwargs
) -> DescribeContainerInstancesResponseTypeDef:
    ecs = get_ecs_client()
    try:
        return ecs.describe_container_instances(
            containerInstances=container_instances, cluster=cluster, **kwargs
        )
    except ClientError as e:
        msg = (
            "Error retrieving container instance metadata for container "
            f"instances={container_instances}, cluster={cluster}: {e}"
        )
        logger.error(msg, exc_info=True)
        raise AWSError(msg)
