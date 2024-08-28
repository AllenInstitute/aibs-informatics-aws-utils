import moto
from pytest import raises

from aibs_informatics_aws_utils.ecs import ecs_describe_container_instances
from aibs_informatics_aws_utils.exceptions import AWSError


@moto.mock_aws
def test__ecs_describe_container_instances__fails(aws_credentials_fixture):
    with raises(AWSError):
        ecs_describe_container_instances(cluster="test", container_instances=["test"])
