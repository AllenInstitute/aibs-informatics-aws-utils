import json
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple, Union

import boto3
import moto
from moto.core.config import DefaultConfig

from aibs_informatics_aws_utils.constants.efs import (
    EFS_MOUNT_POINT_ID_VAR,
    EFS_MOUNT_POINT_PATH_VAR,
)
from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.efs import (
    MountPointConfiguration,
    deduplicate_mount_points,
    detect_mount_points,
)
from test.aibs_informatics_aws_utils.efs.base import EFSTestsBase

if TYPE_CHECKING:
    from mypy_boto3_lambda.type_defs import FileSystemConfigTypeDef
else:  # pragma: no cover
    FileSystemConfigTypeDef = dict


class MountPointConfigurationTests(EFSTestsBase):
    def setUp(self) -> None:
        super().setUp()
        detect_mount_points.cache_clear()

    @property
    def mock_aws_config(self) -> Optional[DefaultConfig]:
        return {"batch": {"use_docker": False}, "core": {"reset_boto3_session": True}}

    def setUpEFS(self, *access_points: Tuple[str, Path], file_system_name: Optional[str] = None):
        self.create_file_system(file_system_name)
        for access_point_name, access_point_path in access_points:
            self.create_access_point(
                access_point_name=access_point_name,
                access_point_path=access_point_path,
                file_system_name=file_system_name,
            )

    def test__as_env_vars__happy_case(self):
        c = self.get_mount_point("/opt/A", "ap1", "/a")

        assert c.access_point is not None
        assert "AccessPointId" in c.access_point
        self.assertEqual(
            c.as_env_vars(name="TEST"),
            {
                EFS_MOUNT_POINT_ID_VAR + "_TEST": c.access_point["AccessPointId"],
                EFS_MOUNT_POINT_PATH_VAR + "_TEST": "/opt/A",
            },
        )

    def test__translate_mounted_path__with_nested_access_point(self):
        c1 = self.get_mount_point(Path("/opt/A"), "ap1", Path("/a"))
        c2 = self.get_mount_point(Path("/mnt/B"), "ap2", Path("/a/b"))

        # Absolute path should work
        path = Path("b/file.txt")
        full_path = c1.mount_point / path
        self.assertEqual(c2.mount_point / "file.txt", c2.translate_mounted_path(full_path, c1))
        # Relative path should work too
        self.assertEqual(c2.mount_point / "file.txt", c2.translate_mounted_path(path, c1))

        # Should work in reverse too if we use the right level of nesting
        path = Path("file.txt")
        # access point is
        full_path = c2.mount_point / path
        self.assertEqual(c1.mount_point / "b/file.txt", c1.translate_mounted_path(full_path, c2))
        self.assertEqual(c1.mount_point / "b/file.txt", c1.translate_mounted_path(path, c2))

        with self.assertRaises(ValueError):
            path = Path("c") / "file.txt"
            c2.translate_mounted_path(path, c1)

    def test__translate_mounted_path__fails_for_non_overlapping_access_points(self):
        c1 = self.get_mount_point("/opt/A", "ap1", "/a")
        c2 = self.get_mount_point("/mnt/B", "ap2", "/b")

        path = Path("b") / "file.txt"

        with self.assertRaises(ValueError):
            c2.translate_mounted_path(c1.mount_point / path, c1)

    def test__detect_mount_points__no_env_vars(self):
        actual = detect_mount_points()
        self.assertEqual(len(actual), 0)

    def test__detect_mount_points__single_env_vars(self):
        c = self.get_mount_point(self.tmp_path(), None, None, "fs1")

        self.set_env_vars(*c.as_env_vars(name="A").items())

        actual = detect_mount_points()
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual[0].file_system["FileSystemId"], c.file_system["FileSystemId"])
        self.assertEqual(actual[0].mount_point, c.mount_point)
        self.assertEqual(actual[0].access_point_path, c.access_point_path)

    def test__detect_mount_points__multiple_env_vars(self):
        c1 = self.get_mount_point(self.tmp_path(), None, None, "fs1")
        c2 = self.get_mount_point(self.tmp_path(), "ap2", "/b", "fs1")

        self.set_env_vars(*c1.as_env_vars(name="A").items())
        self.set_env_vars(*c2.as_env_vars(name="B").items())

        actual = detect_mount_points()
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual[0].file_system["FileSystemId"], c1.file_system["FileSystemId"])
        self.assertEqual(actual[0].mount_point, c1.mount_point)
        self.assertEqual(actual[0].access_point_path, c1.access_point_path)
        self.assertEqual(actual[1].file_system["FileSystemId"], c2.file_system["FileSystemId"])
        self.assertEqual(actual[1].mount_point, c2.mount_point)
        self.assertEqual(actual[1].access_point_path, c2.access_point_path)

    def test__detect_mount_points__multiple_env_vars_only_some_available(self):
        c1 = self.get_mount_point(self.tmp_path(), None, None, "fs1")

        self.set_env_vars(*c1.as_env_vars(name="A").items())
        self.set_env_vars((f"{EFS_MOUNT_POINT_ID_VAR}_B", "fs-12345678"))
        self.set_env_vars((f"{EFS_MOUNT_POINT_PATH_VAR}_B", "relative/path"))
        self.set_env_vars((f"{EFS_MOUNT_POINT_ID_VAR}_C", "fs-12345678"))
        self.set_env_vars((f"{EFS_MOUNT_POINT_PATH_VAR}_C", "/invalid/path"))
        self.set_env_vars((f"{EFS_MOUNT_POINT_ID_VAR}_D", "fs-invalid"))
        self.set_env_vars((f"{EFS_MOUNT_POINT_PATH_VAR}_D", c1.mount_point_path.as_posix()))

        actual = detect_mount_points()
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual[0].file_system["FileSystemId"], c1.file_system["FileSystemId"])
        self.assertEqual(actual[0].mount_point, c1.mount_point)
        self.assertEqual(actual[0].access_point_path, c1.access_point_path)

    @moto.mock_aws(config={"lambda": {"use_docker": False}})
    def test__detect_mount_points__lambda_config_overrides(self):
        c1 = self.get_mount_point(self.tmp_path(), "ap1", "/", "fs1")
        c2 = self.get_mount_point(self.tmp_path(), "ap2", "/b", "fs1")
        c3 = self.get_mount_point(self.tmp_path(), "ap3", "/c", "fs1")

        self.set_env_vars(*c2.as_env_vars(name="B").items())
        self.set_env_vars(*c3.as_env_vars(name="C").items())
        self.set_env_vars(("AWS_LAMBDA_FUNCTION_NAME", "test"))

        # Set up lambda
        lambda_client = boto3.client("lambda")
        file_system_configs: list[FileSystemConfigTypeDef] = [
            {
                "Arn": (c1.access_point or {}).get("AccessPointArn"),  # type: ignore
                "LocalMountPath": c1.mount_point.as_posix(),
            },
            {
                "Arn": (c2.access_point or {}).get("AccessPointArn"),  # type: ignore
                "LocalMountPath": c2.mount_point.as_posix(),
            },
        ]
        lambda_client.create_function(
            FunctionName="test",
            Runtime="python3.8",
            Handler="test",
            Role=boto3.client("iam").create_role(
                RoleName="foo",
                AssumeRolePolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    }
                ),
            )["Role"]["Arn"],
            Code={"ZipFile": b"bar"},
            Environment={"Variables": {"TEST": "test"}},
            # NOTE: FileSystemConfigs is not supported by moto yet, so this is meaningless
            FileSystemConfigs=file_system_configs,
        )
        # HACK: moto doesn't support FileSystemConfigs yet, so we have to patch it in
        #       here we will fetch the actual response and then add the FileSystemConfigs
        response = lambda_client.get_function_configuration(FunctionName="test")
        with self.stub(lambda_client) as lambda_stubber:
            lambda_stubber.add_response(
                "get_function_configuration",
                {
                    **response,
                    **{"FileSystemConfigs": file_system_configs},
                },
                expected_params={"FunctionName": "test"},
            )
            self.create_patch(
                "aibs_informatics_aws_utils.efs.mount_point.get_lambda_client",
                return_value=lambda_client,
            )

            # Act
            actual = detect_mount_points()
            lambda_stubber.assert_no_pending_responses()
        # Assert
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual[0].file_system["FileSystemId"], c1.file_system["FileSystemId"])
        self.assertEqual(actual[0].mount_point, c1.mount_point)
        self.assertEqual(actual[0].access_point_path, c1.access_point_path)
        self.assertEqual(actual[1].file_system["FileSystemId"], c2.file_system["FileSystemId"])
        self.assertEqual(actual[1].mount_point, c2.mount_point)
        self.assertEqual(actual[1].access_point_path, c2.access_point_path)

    def test__detect_mount_points__batch_job_config_overrides(self):
        c1 = self.get_mount_point(self.tmp_path(), None, None, "fs1")
        c2 = self.get_mount_point(self.tmp_path(), "ap2", "/b", "fs1")
        c3 = self.get_mount_point(self.tmp_path(), "ap3", "/c", "fs1")

        # This will validate that the batch client is being used over env vars
        self.set_env_vars(*c2.as_env_vars(name="B").items())
        self.set_env_vars(*c3.as_env_vars(name="C").items())

        # EC2 Resources: VPC, Subnets, Security Groups
        ec2_client = boto3.client("ec2")
        vpc_id = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]  # type: ignore
        subnet_ids = [
            ec2_client.create_subnet(VpcId=vpc_id, CidrBlock="10.0.0.0/20")["Subnet"]["SubnetId"]  # type: ignore
        ]
        security_group_ids = [
            ec2_client.create_security_group(GroupName="test", Description="test", VpcId=vpc_id)[
                "GroupId"
            ]
        ]

        # IAM Resources: Role, Instance Profile
        iam_client = boto3.client("iam")
        iam_role_arn = iam_client.create_role(
            RoleName="foo",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "batch.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        },
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "ec2.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        },
                    ],
                }
            ),
        )["Role"]["Arn"]
        instance_profile_arn = iam_client.create_instance_profile(InstanceProfileName="foo")[
            "InstanceProfile"
        ]["Arn"]
        iam_client.add_role_to_instance_profile(InstanceProfileName="foo", RoleName="foo")

        # Batch Resources: Job Definition, Compute Environment, Job Queue
        batch_client = AWSService.BATCH.get_client()

        assert c2.access_point is not None
        assert "AccessPointId" in c2.access_point
        job_definition_arn = batch_client.register_job_definition(
            jobDefinitionName="test",
            type="container",
            containerProperties={
                "resourceRequirements": [
                    {"type": "MEMORY", "value": "1024"},
                    {"type": "VCPU", "value": "1"},
                ],
                "image": "test",
                "mountPoints": (
                    batch_mount_point_configs := [
                        {
                            "containerPath": c1.mount_point.as_posix(),
                            "sourceVolume": "test1",
                        },
                        {
                            "containerPath": c2.mount_point.as_posix(),
                            "sourceVolume": "test2",
                        },
                    ]
                ),
                "volumes": (
                    _batch_volume_configs := [
                        {
                            "name": "test1",
                            "efsVolumeConfiguration": {
                                "fileSystemId": c1.file_system["FileSystemId"],
                                "rootDirectory": "/",
                            },
                        },
                        {
                            "name": "test2",
                            "efsVolumeConfiguration": {
                                "fileSystemId": c2.file_system["FileSystemId"],
                                "rootDirectory": "/b",
                                "transitEncryption": "ENABLED",
                                "authorizationConfig": {
                                    "accessPointId": c2.access_point["AccessPointId"]
                                },
                            },
                        },
                    ]
                ),
            },
        )["jobDefinitionArn"]

        ce_arn = batch_client.create_compute_environment(
            computeEnvironmentName="test",
            type="MANAGED",
            state="ENABLED",
            computeResources={
                "type": "EC2",
                "minvCpus": 0,
                "maxvCpus": 256,
                "desiredvCpus": 0,
                "instanceTypes": ["optimal"],
                "subnets": subnet_ids,
                "securityGroupIds": security_group_ids,
                "instanceRole": instance_profile_arn,
                "tags": {"Name": "test"},
            },
            serviceRole=iam_role_arn,
        )["computeEnvironmentArn"]
        job_queue_arn = batch_client.create_job_queue(
            state="ENABLED",
            jobQueueName="test",
            priority=1,
            computeEnvironmentOrder=[
                {
                    "order": 1,
                    "computeEnvironment": ce_arn,
                }
            ],
        )["jobQueueArn"]
        job_id = batch_client.submit_job(
            jobName="test",
            jobQueue=job_queue_arn,
            jobDefinition=job_definition_arn,
        )["jobId"]
        self.set_env_vars(("AWS_BATCH_JOB_ID", job_id))

        describe_job_response = batch_client.describe_jobs(jobs=[job_id])
        with self.stub(batch_client) as batch_stubber:
            describe_job_response["jobs"][0]["container"][  # type: ignore
                "mountPoints"
            ] = batch_mount_point_configs
            batch_stubber.add_response(
                "describe_jobs",
                describe_job_response,
                expected_params={"jobs": [job_id]},
            )

            self.create_patch(
                "aibs_informatics_aws_utils.efs.mount_point.get_batch_client",
                return_value=batch_client,
            )
            # Act
            actual = detect_mount_points()
            batch_stubber.assert_no_pending_responses()

        # Assert
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual[0].file_system["FileSystemId"], c1.file_system["FileSystemId"])
        self.assertEqual(actual[0].mount_point, c1.mount_point)
        self.assertEqual(actual[0].access_point_path, c1.access_point_path)
        self.assertEqual(actual[1].file_system["FileSystemId"], c2.file_system["FileSystemId"])
        self.assertEqual(actual[1].mount_point, c2.mount_point)
        self.assertEqual(actual[1].access_point_path, c2.access_point_path)

    def test__deduplicate_mount_points__no_overlap(self):
        c1 = self.get_mount_point(Path("/opt/A"), "ap1", Path("/a"), "fs1")
        c2 = self.get_mount_point(Path("/opt/B"), "ap2", Path("/b"), "fs2")

        self.assertListEqual(deduplicate_mount_points([c1, c2]), [c1, c2])
        self.assertListEqual(deduplicate_mount_points([c2, c1]), [c2, c1])

    def test__deduplicate_mount_points__duplicate_mount_points_removed(self):
        c1 = self.get_mount_point(Path("/opt/A"), "ap1", Path("/a"), "fs1")
        c2 = self.get_mount_point(Path("/opt/B"), "ap2", Path("/b"), "fs2")

        self.assertListEqual(deduplicate_mount_points([c2, c1, c2]), [c2, c1])

    def test__deduplicate_mount_points__errors_on_differing_file_system_ids(self):
        c1 = self.get_mount_point(Path("/opt/A"), "ap1", Path("/a"), "fs1")
        c2 = self.get_mount_point(Path("/opt/A"), "ap1", Path("/a"), "fs2")

        with self.assertRaises(ValueError):
            deduplicate_mount_points([c1, c2])

    def test__deduplicate_mount_points__raises_value_error_if_access_points_overlap(self):
        c1 = self.get_mount_point(Path("/opt/A"), "ap1", Path("/a"), "fs1")
        c2 = self.get_mount_point(Path("/opt/A"), "ap2", Path("/b"), "fs2")

        with self.assertRaises(ValueError):
            deduplicate_mount_points([c1, c2])

    def get_mount_point(
        self,
        mount_point: Union[Path, str],
        access_point_name: Optional[str],
        access_point_path: Optional[Union[Path, str]],
        file_system_name: Optional[str] = None,
        create: bool = True,
    ) -> MountPointConfiguration:
        file_system = self.create_file_system(file_system_name)
        if access_point_name:
            access_point = self.create_access_point(
                access_point_name=access_point_name,
                access_point_path=access_point_path,
                file_system_name=file_system_name,
            )
        else:
            access_point = None
        return MountPointConfiguration.build(
            mount_point=mount_point, file_system=file_system, access_point=access_point
        )
