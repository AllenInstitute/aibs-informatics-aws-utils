from typing import TYPE_CHECKING, Dict, List
from unittest import mock

from aibs_informatics_core.env import ENV_BASE_KEY_ALIAS, EnvBase, EnvType
from aibs_informatics_core.models.aws.batch import ResourceRequirements

from aibs_informatics_aws_utils.batch import (
    BatchJobBuilder,
    ContainerPropertiesTypeDef,
    JobDefinitionTypeDef,
    RetryStrategyTypeDef,
    batch_log_stream_name_to_url,
    build_retry_strategy,
    describe_jobs,
    get_batch_client,
    register_job_definition,
    submit_job,
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

    @mock.patch("aibs_informatics_aws_utils.batch.sha256_hexdigest", return_value="hashvalue")
    def test__submit_job__submits_with_minimal_args(self, mock_sha: mock.MagicMock):
        with self.stub(self.batch_client) as batch_stubber:
            mock_sha.return_value = (
                "1ee55eb8c7f4cee6a644c1346db610ba2306547a695a7a76ff28b9a47b829fac"  # noqa
            )
            job_def_name = "test-job-def-name"
            job_queue = "test-queue"
            expected_job_name = (
                "dev-marmotdev-1ee55eb8c7f4cee6a644c1346db610ba2306547a695a7a76ff28b9a47b829fac"  # noqa
            )
            batch_stubber.add_response(
                "submit_job",
                {
                    "jobName": expected_job_name,
                    "jobId": "01234567-89ab-cdef-0123-456789abcdef",
                },
                {
                    "jobName": expected_job_name,
                    "jobQueue": job_queue,
                    "jobDefinition": job_def_name,
                },
            )
            submit_response = submit_job(
                job_definition=job_def_name,
                job_queue=job_queue,
                env_base=self.env_base,
                region=self.DEFAULT_REGION,
            )
            self.assertEqual(
                submit_response,
                {
                    "jobName": expected_job_name,
                    "jobId": "01234567-89ab-cdef-0123-456789abcdef",
                },
            )

            batch_stubber.assert_no_pending_responses()

    @mock.patch("aibs_informatics_aws_utils.batch.sha256_hexdigest", return_value="hashvalue")
    def test__submit_job__submits_with_all_args_specified(self, mock_sha: mock.MagicMock):
        with self.stub(self.batch_client) as batch_stubber:
            mock_sha.return_value = (
                "1ee55eb8c7f4cee6a644c1346db610ba2306547a695a7a76ff28b9a47b829fac"  # noqa
            )
            job_def_name = "test-job-def-name"
            job_queue = "test-queue"
            expected_job_name = "test-job-name"
            batch_stubber.add_response(
                "submit_job",
                {
                    "jobName": expected_job_name,
                    "jobId": "01234567-89ab-cdef-0123-456789abcdef",
                },
                {
                    "jobName": expected_job_name,
                    "jobQueue": job_queue,
                    "jobDefinition": job_def_name,
                },
            )
            submit_response = submit_job(
                job_definition=job_def_name,
                job_queue=job_queue,
                job_name=expected_job_name,
                env_base=self.env_base,
                region=self.DEFAULT_REGION,
            )
            self.assertEqual(
                submit_response,
                {
                    "jobName": expected_job_name,
                    "jobId": "01234567-89ab-cdef-0123-456789abcdef",
                },
            )
            batch_stubber.assert_no_pending_responses()
            mock_sha.assert_not_called()

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


@mock.patch("aibs_informatics_aws_utils.batch.get_region", return_value="us-east-1")
def test__batch_job_builder__container_properties_include_optional_fields(_mock_get_region):
    env_base = EnvBase.from_type_and_label(EnvType.DEV, "builder")
    resource_requirements = [
        {"type": "MEMORY", "value": "8192"},
        {"type": "GPU", "value": "1"},
        {"type": "VCPU", "value": "2"},
    ]
    builder = BatchJobBuilder(
        image="example:latest",
        job_definition_name="definition",
        job_name="job",
        command=["python", "script.py"],
        environment={"EXTRA": "value"},
        resource_requirements=resource_requirements,
        mount_points=[{"containerPath": "/data", "readOnly": False, "sourceVolume": "data"}],
        volumes=[{"name": "data", "host": {"sourcePath": "/mnt/data"}}],
        job_role_arn="arn:aws:iam::123456789012:role/BatchRole",
        privileged=True,
        linux_parameters={"initProcessEnabled": True},
        env_base=env_base,
    )

    assert builder.environment[ENV_BASE_KEY_ALIAS] == env_base
    assert builder.environment["AWS_REGION"] == "us-east-1"
    assert builder.environment["EXTRA"] == "value"

    container_props = builder.container_properties
    expected_environment = [
        {"name": "AWS_REGION", "value": "us-east-1"},
        {"name": ENV_BASE_KEY_ALIAS, "value": env_base},
        {"name": "EXTRA", "value": "value"},
    ]
    expected_resource_requirements = [
        {"type": "GPU", "value": "1"},
        {"type": "MEMORY", "value": "8192"},
        {"type": "VCPU", "value": "2"},
    ]

    assert container_props["image"] == "example:latest"
    assert container_props["command"] == ["python", "script.py"]
    assert container_props["privileged"] is True
    assert container_props["mountPoints"] == [
        {"containerPath": "/data", "readOnly": False, "sourceVolume": "data"}
    ]
    assert container_props["volumes"] == [{"name": "data", "host": {"sourcePath": "/mnt/data"}}]
    assert container_props["environment"] == expected_environment
    assert container_props["resourceRequirements"] == expected_resource_requirements
    assert container_props["linuxParameters"] == {"initProcessEnabled": True}
    assert container_props["jobRoleArn"] == "arn:aws:iam::123456789012:role/BatchRole"
    assert builder._normalized_resource_requirements() == expected_resource_requirements


@mock.patch("aibs_informatics_aws_utils.batch.get_region", return_value="us-west-2")
def test__batch_job_builder__container_overrides_and_pascal_case(_mock_get_region):
    env_base = EnvBase.from_type_and_label(EnvType.TEST, "builder")
    builder = BatchJobBuilder(
        image="example:latest",
        job_definition_name="definition",
        job_name="job",
        environment={"EXTRA": "value", "NULL": None},
        resource_requirements=ResourceRequirements(GPU=2, MEMORY=4096, VCPU=16),
        env_base=env_base,
    )

    expected_resource_requirements = [
        {"type": "GPU", "value": "2"},
        {"type": "MEMORY", "value": "4096"},
        {"type": "VCPU", "value": "16"},
    ]
    expected_environment = [
        {"name": "AWS_REGION", "value": "us-west-2"},
        {"name": ENV_BASE_KEY_ALIAS, "value": env_base},
        {"name": "EXTRA", "value": "value"},
    ]

    container_overrides = builder.container_overrides
    assert builder.environment["NULL"] is None
    assert container_overrides["resourceRequirements"] == expected_resource_requirements
    assert container_overrides["environment"] == expected_environment
    assert builder.container_overrides__sfn == {
        "Environment": [
            {"Name": "AWS_REGION", "Value": "us-west-2"},
            {"Name": ENV_BASE_KEY_ALIAS, "Value": env_base},
            {"Name": "EXTRA", "Value": "value"},
        ],
        "ResourceRequirements": [
            {"Type": "GPU", "Value": "2"},
            {"Type": "MEMORY", "Value": "4096"},
            {"Type": "VCPU", "Value": "16"},
        ],
    }
    assert builder._normalized_resource_requirements() == expected_resource_requirements


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


@mock.patch("aibs_informatics_aws_utils.batch.get_batch_client")
def test__describe_jobs__works(mock_get_batch_client):
    mock_client = mock.MagicMock()
    mock_get_batch_client.return_value = mock_client
    mock_client.describe_jobs.return_value = {"jobs": []}

    describe_jobs(job_ids=["job1", "job2"])
    mock_client.describe_jobs.assert_called_once_with(jobs=["job1", "job2"])


@mock.patch("aibs_informatics_aws_utils.batch.build_log_stream_url")
def test__batch_log_stream_name_to_url__works(mock_build_log_stream_url):
    mock_build_log_stream_url.return_value = "http://example.com"
    batch_log_stream_name_to_url(log_stream_name="stream", region="us-west-2")
    mock_build_log_stream_url.assert_called_once_with(
        log_group_name="/aws/batch/job",
        log_stream_name="stream",
        region="us-west-2",
    )
