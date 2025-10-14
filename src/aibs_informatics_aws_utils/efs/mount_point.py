from __future__ import annotations

import re
from functools import cache

__all__ = [
    "MountPointConfiguration",
    "detect_mount_points",
    "deduplicate_mount_points",
]

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union, cast

from aibs_informatics_core.models.aws.efs import AccessPointId, EFSPath, FileSystemId
from aibs_informatics_core.utils.decorators import retry
from aibs_informatics_core.utils.hashing import sha256_hexdigest
from aibs_informatics_core.utils.os_operations import get_env_var
from botocore.exceptions import NoCredentialsError

from aibs_informatics_aws_utils.constants.efs import (
    EFS_MOUNT_POINT_ID_VAR,
    EFS_MOUNT_POINT_PATH_VAR,
)
from aibs_informatics_aws_utils.core import AWSService
from aibs_informatics_aws_utils.efs.core import get_efs_access_point, get_efs_file_system

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_batch.type_defs import JobDetailTypeDef
    from mypy_boto3_efs.type_defs import (
        AccessPointDescriptionTypeDef,
        FileSystemDescriptionTypeDef,
    )
else:
    AccessPointDescriptionTypeDef = dict
    FileSystemDescriptionTypeDef = dict
    JobDetailTypeDef = dict

logger = logging.getLogger(__name__)

StrPath = Union[Path, str]


get_lambda_client = AWSService.LAMBDA.get_client
get_batch_client = AWSService.BATCH.get_client


@dataclass
class MountPointConfiguration:
    """
    Adapter for translating and mapping paths between AWS Elastic File System
    (EFS) mount points and local file system paths.

    local file systems are most often used by:
    - EC2 instances
    - AWS Lambda functions

    This class provides functionality to adapt file paths for applications
    that interact with AWS EFS. It allows for the conversion of paths from the
    EFS perspective to the local file system perspective and vice versa,
    considering the mount point and access point configurations.

    Attributes:
        file_system (FileSystemDescriptionTypeDef): The description of the EFS file system.
        access_point (Optional[AccessPointDescriptionTypeDef]): The description
            of the EFS access point, if used.
        mount_point (Path): The local file system path where the EFS is mounted.
    """

    file_system: FileSystemDescriptionTypeDef
    access_point: Optional[AccessPointDescriptionTypeDef]
    mount_point: Path

    # TODO: make hashable

    @property
    def access_point_path(self) -> Path:
        """This is the access point on file system mounted to host"""
        path: str = "/"
        if self.access_point:
            path = self.access_point.get("RootDirectory", {}).get("Path", path)
        return Path(path)

    @property
    def mount_point_path(self) -> Path:
        """This is the path on the host where the access point will be mounted"""
        return self.mount_point

    @property
    def is_writeable(self) -> bool:
        """Returns true, if the mount point is writeable"""
        # TODO: need to figure out if it is writeable
        return True

    def translate_mounted_path(self, path: StrPath, other: MountPointConfiguration) -> Path:
        """
        Translates the location of a path described by another mount point to
        a location on this mount point

        Args:
            path (StrPath): The path to translate. should be relative path
                or absolute relative to the other mount point/access point.
            other (T): The config of other mount point/access point to translate from.

        Raises:
            ValueError: If file system of other mount point/access point is
                not the same as this mount point/access point.

        Returns:
            Path: The translated absolute path on this mount point.
        """
        path = Path(path)
        if self.file_system["FileSystemId"] != other.file_system["FileSystemId"]:
            raise ValueError(
                f"Cannot resolve path {path} from file system "
                f"{other.file_system['FileSystemId']} to file system "
                f"{self.file_system['FileSystemId']}. They are not the same file system!"
            )

        # First, convert the path to a global path on EFS File System
        global_path = other.as_efs_path(path)
        # Next, convert the global path to the path on the container
        return self.as_mounted_path(global_path)

    def as_relative_path(self, path: StrPath) -> Path:
        """
        Converts a path to a relative path from the mount point or access point.

        Behavior:
        - If the path is absolute and is relative to the mount point or access point,
            it is returned as a relative path.
        - If the path is already relative,
            it is returned as is.
        - If the path is absolute and is not relative to the mount point or access point,
            a ValueError is raised.


        Args:
            path (StrPath): The path to convert to a relative path.

        Returns:
            Path: The relative path.

        """
        path = Path(path)
        if path.is_absolute():
            # Check longer path first. This is to avoid the case where the mount point
            # is a subdirectory of the access point.
            check_mount_path_first = len(self.mount_point_path.as_posix()) > len(
                self.access_point_path.as_posix()
            )
            if check_mount_path_first and self.is_mounted_path(path):
                path = path.relative_to(self.mount_point)
            elif self.is_efs_path(path):
                path = path.relative_to(self.access_point_path)
            elif self.is_mounted_path(path):
                path = path.relative_to(self.mount_point)
            else:
                raise ValueError(
                    f"Path {path} is not relative to either the mount point {self.mount_point} "
                    f"or the access point {self.access_point_path}"
                )
        return path

    def as_efs_path(self, path: StrPath) -> Path:
        """
        Converts a path to a path on the EFS file system.

        Behavior:
        - If the path is absolute, the path is made relative to either the
            mount point or access point first. examples:
            - "/efs/accesspoint/path/to/file" -> "path/to/file"
            - "/mnt/efs/path/to/file" -> "path/to/file"
        - If the path is absolute and does not start with the mount point or access point,
            a ValueError is raised.
        - If the path is relative, It is assumed to be the relative path from the access point.
            e.g. "path/to/file" -> "/efs/accesspoint/path/to/file"

        Args:
            path (StrPath): The path to convert to a path on the EFS file system.

        Returns:
            Path: absolute path on the EFS file system.
        """
        path = self.as_relative_path(path)
        return (self.access_point_path / path).resolve()

    def as_efs_uri(self, path: StrPath) -> EFSPath:
        """
        Converts a path to a customized URI EFSPath describing the location on an EFS file system.

        Args:
            path (StrPath): The path to convert to a path on the EFS file system.

        Returns:
            EFSPath: absolute URI path on the EFS file system.
        """
        return EFSPath.build(
            resource_id=self.file_system["FileSystemId"],
            path=self.as_efs_path(path),
        )

    def as_mounted_path(self, path: StrPath) -> Path:
        """
        Converts a path to a path on the host where the EFS is mounted.

        Behavior:
        - If the path is absolute, the path is made relative to either the mount point
            or access point first. examples:
            - "/efs/accesspoint/path/to/file" -> "path/to/file"
            - "/mnt/efs/path/to/file" -> "path/to/file"
        - If the path is absolute and does not start with the mount point or access point,
            a ValueError is raised.
        - If the path is relative, It is assumed to be the relative path from the mount point.
            e.g. "path/to/file" -> "/mnt/efs/path/to/file"

        Args:
            path (StrPath): The path to convert to a path on the host where the EFS is mounted.

        Returns:
            Path: absolute path on the host where the EFS is mounted.
        """
        path = self.as_relative_path(path)
        return (self.mount_point / path).resolve()

    def is_efs_path(self, path: StrPath) -> bool:
        """Checks if the path is relative to the access point"""
        return Path(path).is_relative_to(self.access_point_path)

    def is_mounted_path(self, path: StrPath) -> bool:
        """Checks if the path is relative to the mount point"""
        return Path(path).is_relative_to(self.mount_point)

    def as_env_vars(self, name: Optional[str] = None) -> Dict[str, str]:
        """Converts the mount point configuration to environment variables."""
        if self.access_point and self.access_point.get("AccessPointId"):
            mount_point_id = self.access_point["AccessPointId"]
        else:
            mount_point_id = self.file_system["FileSystemId"]

        return self.to_env_vars(self.mount_point, mount_point_id, name)

    @classmethod
    def build(
        cls,
        mount_point: StrPath,
        access_point: Optional[Union[str, AccessPointDescriptionTypeDef]] = None,
        file_system: Optional[Union[str, FileSystemDescriptionTypeDef]] = None,
        access_point_tags: Optional[Dict[str, str]] = None,
        file_system_tags: Optional[Dict[str, str]] = None,
    ) -> MountPointConfiguration:
        """Creates a new config from the given mount point and access point or file system.

        Important: Must provide either access point or file system.

        Args:
            mount_point (StrPath): Intended mount point of the EFS file system on the host.
            access_point (Optional[Union[str, AccessPointDescriptionTypeDef]]):
                Identifier of the access point or the access point description.
                If specified as a string, can be either the access point id or name.
                Defaults to None.
            file_system (Optional[Union[str, FileSystemDescriptionTypeDef]]):
                Identifier of the file system or the file system description.
                If specified as a string, can be either the file system id or name.
                Defaults to None.
            access_point_tags (Optional[Dict[str, str]]): Tags to filter the access point.
                Defaults to None.
            file_system_tags (Optional[Dict[str, str]]): Tags to filter the file system.
                Defaults to None.
        Raises:
            ValueError: if neither access point nor file system is provided.

        Returns:
            The config
        """

        if access_point is None and file_system is None:
            raise ValueError("Either access point or file system must be provided")

        file_system_id = None
        if isinstance(file_system, str):
            logger.info(f"Resolving file system config from name {file_system}")
            fs_id, fs_name = (
                (FileSystemId(file_system).normalized, None)
                if FileSystemId.is_valid(file_system)
                else (None, file_system)
            )
            file_system = get_efs_file_system(
                name=fs_name, file_system_id=fs_id, tags=file_system_tags
            )
            file_system_id = file_system["FileSystemId"]
            logger.info(f"Resolved file system id {file_system_id}")

        access_point_id = None
        if isinstance(access_point, str):
            logger.info(f"Resolving access point config from name {access_point}")
            ap_id, ap_name = (
                (AccessPointId(access_point).normalized, None)
                if AccessPointId.is_valid(access_point)
                else (None, access_point)
            )
            access_point = get_efs_access_point(
                access_point_name=ap_name,
                access_point_id=ap_id,
                file_system_id=file_system_id,
                access_point_tags=access_point_tags,
            )
            access_point_id = access_point.get("AccessPointId")
            logger.info(f"Resolved access point id {access_point_id}")

        if file_system is None:
            assert access_point is not None
            file_system_id = access_point.get("FileSystemId")
            logger.info(f"Resolving file system config from access point id {access_point_id}")
            file_system = get_efs_file_system(file_system_id=access_point.get("FileSystemId"))
            logger.info(f"Resolved file system id {file_system_id}")

        if not isinstance(mount_point, Path):
            mount_point = Path(mount_point)

        logger.info(
            f"Generating EFS Mount Point Connection using "
            f"file system {file_system}, "
            f"access point {access_point}, "
            f"with target {mount_point} mount point"
        )
        return cls(
            file_system=file_system,
            access_point=access_point,
            mount_point=mount_point,
        )

    @classmethod
    def to_env_vars(
        cls,
        mount_point: StrPath,
        mount_point_id: Union[str, AccessPointId, FileSystemId],
        name: Optional[str] = None,
    ) -> Dict[str, str]:
        """Converts the mount point configuration to environment variables.

        Args:
            mount_point (StrPath): The mount point.
            access_point (Optional[str]): The access point. Defaults to None.

        Returns:
            Dict[str, str]: The environment variables.
        """

        if not isinstance(mount_point, Path):
            mount_point = Path(mount_point)

        if not isinstance(mount_point_id, AccessPointId) and not isinstance(
            mount_point_id, FileSystemId
        ):
            if AccessPointId.is_valid(mount_point_id):
                mount_point_id = AccessPointId(mount_point_id).normalized
            elif FileSystemId.is_valid(mount_point_id):
                mount_point_id = FileSystemId(mount_point_id).normalized
            else:
                raise ValueError(
                    f"Invalid mount point id {mount_point_id}. "
                    "Must be either an access point id or file system id."
                )
        else:
            mount_point_id = mount_point_id.normalized

        if name and not re.match(r"[a-zA-Z][\w]{0,11}", name):
            raise ValueError(
                f"Invalid mount point name {name}. Must be a valid environment variable name."
            )
        else:
            name = name or sha256_hexdigest([mount_point.as_posix(), mount_point_id])[:6]

        return {
            f"{EFS_MOUNT_POINT_PATH_VAR}_{name}": mount_point.as_posix(),
            f"{EFS_MOUNT_POINT_ID_VAR}_{name}": mount_point_id,
        }

    def __repr__(self) -> str:
        access_point = self.access_point.get("AccessPointId") if self.access_point else None
        return (
            f"{self.__class__.__name__}(file_system={self.file_system['FileSystemId']}, "
            f"access_point={access_point},"
            f"access_point_path={self.access_point_path}, mount_point={self.mount_point})"
        )


@cache
@retry(retryable_exceptions=(NoCredentialsError), tries=5, backoff=2.0)
def detect_mount_points() -> List[MountPointConfiguration]:
    mount_points: List[MountPointConfiguration] = []

    if batch_job_id := get_env_var("AWS_BATCH_JOB_ID"):
        logger.info(f"Detected Batch job {batch_job_id}")
        batch_mp_configs = _detect_mount_points_from_batch_job(batch_job_id)
        logger.info(f"Detected {len(batch_mp_configs)} EFS mount points from Batch")
        mount_points.extend(batch_mp_configs)
    elif lambda_function_name := get_env_var("AWS_LAMBDA_FUNCTION_NAME"):
        logger.info(f"Detected Lambda function {lambda_function_name}")
        lambda_mp_configs = _detect_mount_points_from_lambda(lambda_function_name)
        logger.info(f"Detected {len(lambda_mp_configs)} EFS mount points from Lambda")
        mount_points.extend(lambda_mp_configs)
    else:
        logger.info("No Lambda or Batch environment detected. Using environment variables.")
        env_mount_points = _detect_mount_points_from_env()
        logger.info(
            f"Detected {len(env_mount_points)} EFS mount points from environment variables"
        )
        mount_points.extend(env_mount_points)

    logger.info(f"Detected {len(mount_points)} EFS mount points. Deuplicating...")
    mount_points = deduplicate_mount_points(mount_points)
    return mount_points


def deduplicate_mount_points(
    mount_points: List[MountPointConfiguration],
) -> List[MountPointConfiguration]:
    """
    Deduplicates a list of MountPointConfiguration objects based on the file
    system id and access point id.
    """

    unique_configs: Dict[str, MountPointConfiguration] = {}
    for mp_config in mount_points:
        key = mp_config.mount_point.as_posix()

        if key in unique_configs:
            other = unique_configs[key]
            if mp_config.access_point_path != other.access_point_path:
                raise ValueError(
                    f"Found conflicting mount points for {key}: "
                    f"{mp_config.access_point_path} and {unique_configs[key].access_point_path}"
                )
            elif mp_config.file_system["FileSystemId"] != other.file_system["FileSystemId"]:
                raise ValueError(
                    f"Found conflicting file systems for {key}: "
                    f"{mp_config.file_system['FileSystemId']} "
                    f"and {unique_configs[key].file_system['FileSystemId']}"
                )
            elif mp_config.access_point != other.access_point:
                raise ValueError(
                    f"Found conflicting access points for {key}: "
                    f"{mp_config.access_point} and {unique_configs[key].access_point}"
                )
            continue
        unique_configs[key] = mp_config
    return list(unique_configs.values())


# ------------------------------------
# Private Helpers
# ------------------------------------


def _detect_mount_points_from_lambda(lambda_function_name: str) -> List[MountPointConfiguration]:
    mount_points: List[MountPointConfiguration] = []
    lambda_ = get_lambda_client()
    response = lambda_.get_function_configuration(FunctionName=lambda_function_name)

    fs_configs = response.get("FileSystemConfigs")
    if fs_configs:
        for fs_config in fs_configs:
            mount_points.append(
                MountPointConfiguration.build(
                    mount_point=fs_config["LocalMountPath"],
                    access_point=fs_config["Arn"],
                )
            )
    return _remove_invalid_mount_points(mount_points)


def _detect_mount_points_from_batch_job(batch_job_id: str) -> List[MountPointConfiguration]:
    mount_points: List[MountPointConfiguration] = []
    batch = get_batch_client()
    response = batch.describe_jobs(jobs=[batch_job_id])
    job_container = response.get("jobs", [cast(JobDetailTypeDef, {})])[0].get("container", {})
    batch_mount_points = job_container.get("mountPoints")
    batch_volumes = job_container.get("volumes")
    if batch_mount_points and batch_volumes:
        volume_mapping = {
            v_config["name"]: v_config
            for v_config in batch_volumes
            if "efsVolumeConfiguration" in v_config and "name" in v_config
        }

        for batch_mount_point in batch_mount_points:
            assert "containerPath" in batch_mount_point, "containerPath is required"
            assert "sourceVolume" in batch_mount_point, "sourceVolume is required"

            if (batch_volume := volume_mapping.get(batch_mount_point["sourceVolume"])) is None or (
                efs_vol_config := batch_volume.get("efsVolumeConfiguration")
            ) is None:
                continue

            mount_path = batch_mount_point["containerPath"]
            access_point = efs_vol_config.get("authorizationConfig", {}).get("accessPointId")
            file_system = efs_vol_config["fileSystemId"]

            mount_points.append(
                MountPointConfiguration.build(
                    mount_point=mount_path,
                    access_point=access_point,
                    file_system=file_system,
                )
            )

    return _remove_invalid_mount_points(mount_points)


def _detect_mount_points_from_env() -> List[MountPointConfiguration]:
    mount_points: List[MountPointConfiguration] = []

    for k, v in os.environ.items():
        if k.startswith(EFS_MOUNT_POINT_PATH_VAR):
            try:
                name = k[len(EFS_MOUNT_POINT_PATH_VAR) :]
                mount_point = Path(v)

                # if not mount_point.absolute():
                #     logger.warning(f"Mount point {v} is not an absolute path. Skipping...")
                #     continue
                # if not mount_point.is_dir():
                #     logger.warning(f"Mount point {v} is not a valid directory. Skipping...")
                #     continue

                if (efs_id := get_env_var(f"{EFS_MOUNT_POINT_ID_VAR}{name}")) is None:
                    logger.warning(f"Couldn't resolve EFS ID associated with {k} (at {v})")
                    continue

                file_system_id = FileSystemId.find_suffix(efs_id)
                access_point_id = AccessPointId.find_suffix(efs_id)

                if file_system_id is None and access_point_id is None:
                    logger.warning(f"Could not resolve file system or access point from {efs_id}")
                    continue

                mount_points.append(
                    MountPointConfiguration.build(
                        mount_point=mount_point,
                        access_point=access_point_id,
                        file_system=file_system_id,
                    )
                )
            except Exception as e:
                logger.warning(f"Error building config for EFS mount point {k}={v}: {e}")
    return _remove_invalid_mount_points(mount_points)


def _remove_invalid_mount_points(
    mount_points: List[MountPointConfiguration],
) -> List[MountPointConfiguration]:
    valid_mount_points: List[MountPointConfiguration] = []
    for mp in mount_points:
        try:
            if not mp.mount_point.absolute():
                raise ValueError(
                    f"Mount point {mp.mount_point} is not an absolute path. Skipping..."
                )
            if not mp.mount_point.is_dir():
                raise ValueError(
                    f"Mount point {mp.mount_point} is not a valid directory. Skipping..."
                )
            valid_mount_points.append(mp)
        except Exception as e:
            logger.warning(f"Error validating EFS mount point {mp.mount_point}: {e}")
    return valid_mount_points
