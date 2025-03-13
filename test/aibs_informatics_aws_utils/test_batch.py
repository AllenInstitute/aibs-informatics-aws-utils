from typing import TYPE_CHECKING, Dict, List

from aibs_informatics_aws_utils.batch import (
    ContainerPropertiesTypeDef,
    JobDefinitionTypeDef,
    RetryStrategyTypeDef,
    build_retry_strategy,
    get_batch_client,
    register_job_definition,
    to_key_value_pairs,
    to_mount_point,
    to_resource_requirements,
    to_volume,
)
from test.aibs_informatics_aws_utils.base import AwsBaseTest

DESCRIBE_JOB_DEFINITIONS = "describe_job_definitions"
REGISTER_JOB_DEFINITION = "register_job_definition"


if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_batch.client import BatchClient
else:
    BatchClient = object


class BatchTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.DEFAULT_REGION)
        self._batch_client = get_batch_client(region=self.DEFAULT_REGION)

        self._get_batch_client = self.create_patch(
            "aibs_informatics_aws_utils.batch.get_batch_client", autospec=BatchClient
        )
        self._get_batch_client.return_value = self._batch_client

    @property
    def batch_client(self) -> BatchClient:
        return self._batch_client

    def test__register_job_definition__no_job_definitions__registers_new_definition(self):
        with self.stub(self.batch_client) as batch_stubber:
            job_def_name = "test"

            batch_stubber.add_response(
                DESCRIBE_JOB_DEFINITIONS,
                {
                    "jobDefinitions": [],
                },
            )
            batch_stubber.add_response(
                REGISTER_JOB_DEFINITION, self.get_register_job_def_response("test", 1)
            )
            register_job_definition(
                job_definition_name=job_def_name, container_properties=self.get_container_props()
            )

            batch_stubber.assert_no_pending_responses()

    def test__register_job_definition__most_recent_out_of_date__registers_new_def(self):
        with self.stub(self.batch_client) as batch_stubber:
            job_def_name = "test"
            v2_container_props = self.get_container_props(image="test2")
            v1_container_props = self.get_container_props(image="test")
            batch_stubber.add_response(
                DESCRIBE_JOB_DEFINITIONS,
                {
                    "jobDefinitions": [
                        self.get_job_def_type(
                            job_def_name, 1, containerProperties=v1_container_props
                        ),
                        self.get_job_def_type(
                            job_def_name, 3, containerProperties=v2_container_props
                        ),
                        self.get_job_def_type(
                            job_def_name, 2, containerProperties=v1_container_props
                        ),
                    ],
                },
            )
            batch_stubber.add_response(
                REGISTER_JOB_DEFINITION, self.get_register_job_def_response("test", 4)
            )
            register_job_definition(
                job_definition_name=job_def_name, container_properties=v1_container_props
            )

            batch_stubber.assert_no_pending_responses()

    def test__register_job_definition__most_recent_current__skips_new_register_call(self):
        with self.stub(self.batch_client) as batch_stubber:
            job_def_name = "test"
            v2_container_props = self.get_container_props(image="test2")
            v1_container_props = self.get_container_props(image="test")
            batch_stubber.add_response(
                DESCRIBE_JOB_DEFINITIONS,
                {
                    "jobDefinitions": [
                        self.get_job_def_type(
                            job_def_name, 2, containerProperties=v2_container_props
                        ),
                        self.get_job_def_type(
                            job_def_name, 1, containerProperties=v1_container_props
                        ),
                    ],
                },
            )
            register_job_definition(
                job_definition_name=job_def_name, container_properties=v2_container_props
            )

            batch_stubber.assert_no_pending_responses()

    def test__register_job_definition__most_recent_with_retries__skips_new_register_call(self):
        with self.stub(self.batch_client) as batch_stubber:
            job_def_name = "test"
            v2_container_props = self.get_container_props(image="test")
            batch_stubber.add_response(
                DESCRIBE_JOB_DEFINITIONS,
                {
                    "jobDefinitions": [
                        self.get_job_def_type(
                            job_def_name,
                            2,
                            v2_container_props,
                            retryStrategy=build_retry_strategy(),
                        ),
                    ],
                },
            )
            register_job_definition(
                job_definition_name=job_def_name,
                container_properties=v2_container_props,
                retry_strategy=build_retry_strategy(),
            )
            batch_stubber.assert_no_pending_responses()

    def test__build_retry_strategy__builds_with_no_args(self):
        self.assertDictEqual(
            build_retry_strategy(),
            {
                "attempts": 5,
                "evaluateOnExit": [
                    {
                        "action": "RETRY",
                        "onStatusReason": "Task failed to start",
                        "onReason": "DockerTimeoutError*",
                    },
                    {"action": "RETRY", "onStatusReason": "Host EC2*"},
                    {"action": "EXIT", "onStatusReason": "*"},
                ],
            },
        )

    def test__build_retry_strategy__builds_with_default_and_custom_retry_configs(self):
        self.assertDictEqual(
            build_retry_strategy(
                evaluate_on_exit_configs=[{"action": "RETRY", "onStatusReason": "I failed"}]
            ),
            {
                "attempts": 5,
                "evaluateOnExit": [
                    {"action": "RETRY", "onStatusReason": "I failed"},
                    {
                        "action": "RETRY",
                        "onStatusReason": "Task failed to start",
                        "onReason": "DockerTimeoutError*",
                    },
                    {"action": "RETRY", "onStatusReason": "Host EC2*"},
                    {"action": "EXIT", "onStatusReason": "*"},
                ],
            },
        )

    def test__build_retry_strategy__builds_without_default_and_custom_retry_configs(self):
        self.assertDictEqual(
            build_retry_strategy(
                evaluate_on_exit_configs=[{"action": "RETRY", "onStatusReason": "I failed"}],
                include_default_evaluate_on_exit_configs=False,
            ),
            {
                "attempts": 5,
                "evaluateOnExit": [
                    {"action": "RETRY", "onStatusReason": "I failed"},
                ],
            },
        )

    def get_container_props(
        self,
        command: List[str] = [],
        image: str = "test",
        environment: Dict[str, str] = {},
        vcpus: int = 64,
        memory: int = 128000,
    ) -> ContainerPropertiesTypeDef:
        return ContainerPropertiesTypeDef(
            command=command,
            image=image,
            environment=[dict(name=k, value=v) for k, v in environment.items()],
            resourceRequirements=[
                dict(value=str(vcpus), type="VCPU"),
                dict(value=str(memory), type="MEMORY"),
            ],
        )

    def get_job_def_type(
        self,
        job_def_name: str,
        revision: int,
        containerProperties: ContainerPropertiesTypeDef,
        retryStrategy: RetryStrategyTypeDef = None,
        status: str = "ACTIVE",
        schedulingPriority: int = None,
        parameters: Dict[str, str] = None,
        tags: Dict[str, str] = None,
    ) -> JobDefinitionTypeDef:
        kwargs = dict(
            jobDefinitionName=job_def_name,
            jobDefinitionArn=self.get_job_def_arn(job_def_name, revision),
            containerProperties=containerProperties,
            revision=revision,
            type="container",
        )
        if retryStrategy:
            kwargs["retryStrategy"] = retryStrategy
        if status:
            kwargs["status"] = status
        if schedulingPriority:
            kwargs["schedulingPriority"] = schedulingPriority
        if parameters:
            kwargs["parameters"] = parameters
        if tags:
            kwargs["tags"] = tags
        return JobDefinitionTypeDef(**kwargs)

    def get_register_job_def_response(self, job_def_name: str, revision: int):
        return {
            "jobDefinitionName": job_def_name,
            "jobDefinitionArn": self.get_job_def_arn(job_def_name, revision),
            "revision": revision,
        }

    def get_job_def_arn(self, job_def_name: str, revision: int) -> str:
        return f"arn:aws:batch:us-west-2:051791135335:job-definition/{job_def_name}:{revision}"


def test__to_volume__works():
    volume = to_volume("source", "name", None)
    expected = {
        "name": "name",
        "host": {"sourcePath": "source"},
    }
    assert volume == expected


def test__to_mount_point__works():
    mount_point = to_mount_point("/test", False, None)
    expected = {
        "containerPath": "/test",
        "readOnly": False,
    }
    assert mount_point == expected


def test__to_resource_requirements__works():
    resource_requirements = to_resource_requirements(None, 1, 2)
    expected = [
        {"value": "1", "type": "MEMORY"},
        {"value": "2", "type": "VCPU"},
    ]
    assert resource_requirements == expected


def test__to_key_value_pairs__works():
    key_value_pairs = to_key_value_pairs(environment=dict(a="a", b=None), remove_null_values=True)

    expected = [{"name": "a", "value": "a"}]
    assert key_value_pairs == expected

    key_value_pairs = to_key_value_pairs(environment=dict(a="a", b=None), remove_null_values=False)

    expected = [{"name": "a", "value": "a"}, {"name": "b", "value": None}]
    assert key_value_pairs == expected
