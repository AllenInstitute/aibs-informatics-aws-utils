from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Mapping, Optional, Union

from aibs_informatics_core.env import ENV_BASE_KEY_ALIAS, EnvBase, get_env_base
from aibs_informatics_core.models.aws.batch import JobName, ResourceRequirements
from aibs_informatics_core.utils.decorators import retry
from aibs_informatics_core.utils.hashing import sha256_hexdigest
from aibs_informatics_core.utils.logging import get_logger
from aibs_informatics_core.utils.tools.dicttools import convert_key_case
from aibs_informatics_core.utils.tools.strtools import pascalcase
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWS_REGION_VAR, AWSService, get_region
from aibs_informatics_aws_utils.logs import build_log_stream_url

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_batch.literals import JobDefinitionTypeType
    from mypy_boto3_batch.type_defs import (
        ContainerOverridesTypeDef,
        ContainerPropertiesTypeDef,
        DescribeJobsResponseTypeDef,
        EFSVolumeConfigurationTypeDef,
        EvaluateOnExitTypeDef,
        HostTypeDef,
        JobDefinitionTypeDef,
        KeyValuePairTypeDef,
        LinuxParametersTypeDef,
        MountPointTypeDef,
        RegisterJobDefinitionRequestTypeDef,
        RegisterJobDefinitionResponseTypeDef,
        ResourceRequirementTypeDef,
        RetryStrategyTypeDef,
        SubmitJobRequestTypeDef,
        SubmitJobResponseTypeDef,
        VolumeTypeDef,
    )
else:
    JobDefinitionTypeType = str
    RegisterJobDefinitionRequestTypeDef = dict
    ContainerOverridesTypeDef = dict
    ContainerPropertiesTypeDef = dict
    EFSVolumeConfigurationTypeDef = dict
    EvaluateOnExitTypeDef = dict
    HostTypeDef = dict
    JobDefinitionTypeDef = dict
    DescribeJobsResponseTypeDef = dict
    KeyValuePairTypeDef = dict
    LinuxParametersTypeDef = dict
    MountPointTypeDef = dict
    RegisterJobDefinitionResponseTypeDef = dict
    ResourceRequirementTypeDef = dict
    RetryStrategyTypeDef = dict
    SubmitJobRequestTypeDef = dict
    SubmitJobResponseTypeDef = dict
    VolumeTypeDef = dict


logger = get_logger(__name__)


get_batch_client = AWSService.BATCH.get_client


def to_volume(
    source_path: Optional[str],
    name: Optional[str],
    efs_volume_configuration: Optional[EFSVolumeConfigurationTypeDef],
) -> VolumeTypeDef:
    volume_dict = VolumeTypeDef()
    if source_path:
        volume_dict["host"] = HostTypeDef(sourcePath=source_path)
    if name:
        volume_dict["name"] = name
    if efs_volume_configuration:
        volume_dict["efsVolumeConfiguration"] = efs_volume_configuration
    return volume_dict


def to_mount_point(
    container_path: Optional[str],
    read_only: bool,
    source_volume: Optional[str],
) -> MountPointTypeDef:
    mount_point_dict = MountPointTypeDef(readOnly=read_only)
    if container_path:
        mount_point_dict["containerPath"] = container_path
    if source_volume:
        mount_point_dict["sourceVolume"] = source_volume
    return mount_point_dict


def to_key_value_pairs(
    environment: Dict[str, str],
    remove_null_values: bool = True,
) -> List[KeyValuePairTypeDef]:
    """Converts a map style of environment variables into a list of key-value pairs

    Args:
        environment (Dict[str, str]): map of environment variable keys and values
        remove_null_values (bool): Whether to withhold pairs where value is None. Defaults to True

    Returns:
        List[KeyValuePairTypeDef]: List of name,value json blobs representing env variables
    """

    return sorted(
        [
            KeyValuePairTypeDef(name=k, value=v)
            for k, v in environment.items()
            if not remove_null_values or v is not None
        ],
        key=lambda _: _.get("name", ""),
    )


def to_resource_requirements(
    gpu: Optional[int] = None,
    memory: Optional[int] = None,
    vcpus: Optional[int] = None,
) -> List[ResourceRequirementTypeDef]:
    """Converts Batch resource requirement parameters into a list of ResourceRequirement objects

    The returned list only includes dictionary entries for resources that specify
    an explicit value. Anything unset will be dropped.

    Args:
        gpu (Optional[int], optional): number of GPUs to use. Defaults to None.
        memory (Optional[int], optional): amount of memory in MiB. Defaults to None.
        vcpus (Optional[int], optional): Number of VCPUs to use. Defaults to None.

    Returns:
        List[ResourceRequirementTypeDef]: list of resource requirements
    """

    pairs: list[tuple[Literal["GPU", "MEMORY", "VCPU"], Optional[int]]] = [
        ("GPU", gpu),
        ("MEMORY", memory),
        ("VCPU", vcpus),
    ]
    return [ResourceRequirementTypeDef(type=t, value=str(v)) for t, v in pairs if v is not None]


def build_retry_strategy(
    num_retries: int = 5,
    evaluate_on_exit_configs: Optional[List[EvaluateOnExitTypeDef]] = None,
    include_default_evaluate_on_exit_configs: bool = True,
) -> RetryStrategyTypeDef:
    """Build a Retry Strategy for a Job definition

    By default, SPOT Termination retries are included. These can be excluded if desired.

    https://aws.amazon.com/blogs/compute/introducing-retry-strategies-for-aws-batch/

    Args:
        num_retries (int, optional): number of times to retry. Defaults to 5.
        evaluate_on_exit_list (list, optional): list of EvaluateOnExit configs.
        include_default_evaluate_on_exit_configs (bool, optional): Whether to exclude default
            evaluate on exit configuraitons
    Returns:
        RetryStrategy
    """
    all_evaluate_on_exit_configs: List[EvaluateOnExitTypeDef] = []
    if evaluate_on_exit_configs:
        all_evaluate_on_exit_configs.extend(evaluate_on_exit_configs)
    if include_default_evaluate_on_exit_configs:
        all_evaluate_on_exit_configs.extend(
            [
                EvaluateOnExitTypeDef(
                    action="RETRY",
                    onStatusReason="Task failed to start",
                    onReason="DockerTimeoutError*",
                ),
                EvaluateOnExitTypeDef(action="RETRY", onStatusReason="Host EC2*"),
                EvaluateOnExitTypeDef(action="EXIT", onStatusReason="*"),
            ]
        )
    return RetryStrategyTypeDef(attempts=num_retries, evaluateOnExit=all_evaluate_on_exit_configs)


# TODO: need better way of checking when parallel register job def calls collide
@retry(ClientError)
def register_job_definition(
    job_definition_name: str,
    container_properties: ContainerPropertiesTypeDef,
    parameters: Optional[Mapping[str, str]] = None,
    job_definition_type: JobDefinitionTypeType = "container",
    retry_strategy: Optional[RetryStrategyTypeDef] = None,
    tags: Optional[Mapping[str, str]] = None,
    propagate_tags: bool = False,
    region: Optional[str] = None,
) -> Union[JobDefinitionTypeDef, RegisterJobDefinitionResponseTypeDef]:
    batch = get_batch_client(region=region)

    # First we check to make sure that we aren't crearting unnecessary revisions
    # of the same job definition.
    latest = get_latest_job_definition(job_definition_name=job_definition_name, region=region)
    logger.info(f"Previously registered batch job definition: {latest}")
    if latest:
        latest_container_properties = latest.get("containerProperties", {})
        if (
            latest_container_properties.get("command") == container_properties.get("command")
            and latest_container_properties.get("image") == container_properties.get("image")
            and latest_container_properties.get("jobRoleArn")
            == container_properties.get("jobRoleArn")
            and latest.get("parameters") == parameters
            and latest.get("type") == job_definition_type
            and latest.get("tags") == tags
            and latest.get("retryStrategy") == retry_strategy
        ):
            logger.info(
                f"Latest job definition (name={job_definition_name}) matches expected. "
                "Skipping register new job definition call"
            )
            return latest
    register_job_definition_kwargs = RegisterJobDefinitionRequestTypeDef(
        jobDefinitionName=job_definition_name,
        type=job_definition_type,
        parameters=parameters or {},
        containerProperties=container_properties,
        propagateTags=propagate_tags,
        retryStrategy=retry_strategy or {},
        tags=tags or {},
    )
    logger.info(
        f"Registering job definition with following properties: {register_job_definition_kwargs}"
    )
    response = batch.register_job_definition(**register_job_definition_kwargs)  # type: ignore[arg-type]
    return response


def get_latest_job_definition(
    job_definition_name: str, region: Optional[str] = None
) -> Optional[JobDefinitionTypeDef]:
    batch = get_batch_client(region=region)
    response = batch.describe_job_definitions(
        jobDefinitionName=job_definition_name,
        maxResults=1,
        status="ACTIVE",
    )
    if len(response["jobDefinitions"]) == 1:
        return response["jobDefinitions"][0]
    elif len(response["jobDefinitions"]) > 1:
        return sorted(response["jobDefinitions"], key=lambda _: _["revision"], reverse=True)[0]
    return None


def submit_job(
    job_definition: str,
    job_queue: str,
    job_name: Optional[Union[JobName, str]] = None,
    env_base: Optional[EnvBase] = None,
    region: Optional[str] = None,
) -> SubmitJobResponseTypeDef:
    batch_client = get_batch_client(region=region)
    env_base = env_base or get_env_base()
    if job_name is None:
        job_name = JobName(f"{env_base}-{sha256_hexdigest()}")
    else:
        job_name = JobName(job_name)
    submit_job_kwargs = SubmitJobRequestTypeDef(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition,
    )
    response = batch_client.submit_job(**submit_job_kwargs)
    return response


@dataclass
class BatchJobBuilder:
    image: str
    job_definition_name: str
    job_name: str
    command: List[str] = field(default_factory=list)
    environment: Dict[str, str] = field(default_factory=dict)
    job_definition_tags: Dict[str, str] = field(default_factory=dict)
    resource_requirements: Union[List[ResourceRequirementTypeDef], ResourceRequirements] = field(
        default_factory=list
    )
    mount_points: List[MountPointTypeDef] = field(default_factory=list)
    volumes: List[VolumeTypeDef] = field(default_factory=list)
    job_role_arn: Optional[str] = field(default=None)
    privileged: bool = field(default=False)
    linux_parameters: Optional[LinuxParametersTypeDef] = field(default=None)
    env_base: EnvBase = field(default_factory=EnvBase.from_env)

    def __post_init__(self):
        self.environment[ENV_BASE_KEY_ALIAS] = self.env_base
        self.environment[AWS_REGION_VAR] = get_region()
        # TODO: Should add EFS mount point if present as env var

    @property
    def container_properties(self) -> ContainerPropertiesTypeDef:
        container_props = ContainerPropertiesTypeDef(
            image=self.image,
            command=self.command,
            environment=to_key_value_pairs(self.environment),
            resourceRequirements=self._normalized_resource_requirements(),
            mountPoints=self.mount_points,
            volumes=self.volumes,
            privileged=self.privileged,
        )
        if self.linux_parameters:
            container_props["linuxParameters"] = self.linux_parameters
        if self.job_role_arn:
            container_props["jobRoleArn"] = self.job_role_arn
        return container_props

    @property
    def container_overrides(self) -> ContainerOverridesTypeDef:
        return ContainerOverridesTypeDef(
            environment=to_key_value_pairs(self.environment),
            resourceRequirements=self._normalized_resource_requirements(),
        )

    @property
    def container_overrides__sfn(self) -> Dict[str, Any]:
        return convert_key_case(self.container_overrides, pascalcase)  # type: ignore[arg-type]

    def _normalized_resource_requirements(self) -> List[ResourceRequirementTypeDef]:
        if isinstance(self.resource_requirements, list):
            return sorted(self.resource_requirements, key=lambda _: _["type"])
        else:
            return to_resource_requirements(
                gpu=self.resource_requirements.GPU,
                memory=self.resource_requirements.MEMORY,
                vcpus=self.resource_requirements.VCPU,
            )


def describe_jobs(
    job_ids: List[str],
    region: Optional[str] = None,
) -> DescribeJobsResponseTypeDef:
    batch = get_batch_client(region=region)
    response = batch.describe_jobs(jobs=job_ids)
    return response


def batch_log_stream_name_to_url(log_stream_name: str, region: Optional[str] = None) -> str:
    log_group_name = "/aws/batch/job"
    return build_log_stream_url(
        log_group_name=log_group_name, log_stream_name=log_stream_name, region=region
    )
