from __future__ import annotations

__all__ = [
    "get_local_path",
]

import logging
from pathlib import Path
from typing import TYPE_CHECKING, List, Literal, Optional, Union, overload

from aibs_informatics_core.models.aws.efs import EFSPath

from aibs_informatics_aws_utils.efs.mount_point import MountPointConfiguration, detect_mount_points

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_efs.type_defs import (
        AccessPointDescriptionTypeDef,
        DescribeAccessPointsRequestPaginateTypeDef,
        DescribeAccessPointsResponseTypeDef,
        DescribeFileSystemsResponseTypeDef,
        FileSystemDescriptionTypeDef,
        TagTypeDef,
    )
else:
    AccessPointDescriptionTypeDef = dict
    DescribeFileSystemsResponseTypeDef = dict
    DescribeAccessPointsRequestPaginateTypeDef = dict
    DescribeAccessPointsResponseTypeDef = dict
    FileSystemDescriptionTypeDef = dict
    TagTypeDef = dict

logger = logging.getLogger(__name__)

StrPath = Union[Path, str]


@overload
def get_efs_path(
    local_path: Path,
    raise_if_unresolved: Literal[False],
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Optional[EFSPath]: ...


@overload
def get_efs_path(
    local_path: Path,
    raise_if_unresolved: Literal[True] = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> EFSPath: ...


def get_efs_path(
    local_path: Path,
    raise_if_unresolved: bool = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Optional[EFSPath]:
    """Converts a local path assumed to be on a mount point to the EFS path

    Args:
        local_path (Path): Local path
        raise_if_unresolved (bool): If True, raises an error if the local path is not
            under an identifiable mount point. Defaults to True.
        mount_points (List[MountPointConfiguration] | None): Optionally can override
            list of mount_points. If None, mount points are detected. Defaults to None.

    Returns:
        EFSPath: Corresponding EFS URI or None if the path cannot be resolved and
            raise_if_unresolved is False
    """
    mount_points = mount_points if mount_points is not None else detect_mount_points()

    for mp in mount_points:
        if mp.is_mounted_path(local_path):
            logger.debug(f"Found mount point {mp} that matches path {local_path}")
            return mp.as_efs_uri(local_path)
    else:
        message = (
            f"Local path {local_path} is not relative to any of the "
            f"{len(mount_points)} mount point mount_points. Adapters: {mount_points}"
        )
        if raise_if_unresolved:
            logger.error(message)
            raise ValueError(message)
        logger.warning(message)
        return None


@overload
def get_local_path(
    efs_path: EFSPath,
    raise_if_unmounted: Literal[False],
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Optional[Path]: ...


@overload
def get_local_path(
    efs_path: EFSPath,
    raise_if_unmounted: Literal[True] = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Path: ...


def get_local_path(
    efs_path: EFSPath,
    raise_if_unmounted: bool = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Optional[Path]:
    """Gets a valid locally mounted path for the given EFS path.

    Args:
        efs_path (EFSPath): The EFS path. e.g., "efs://fs-12345678:/path/to/file.txt"
        raise_if_unmounted (bool): If True, raises an error if the EFS path is
            not mounted locally. Defaults to True.
        mount_points (List[MountPointConfiguration] | None): Optionally can override
            list of mount points. If None, mount points are detected. Defaults to None.

    Returns:
        Path: The local path. e.g., "/mnt/efs/path/to/file.txt" or None if the path
            cannot be resolved and raise_if_unmounted is False
    """
    mount_points = mount_points if mount_points is not None else detect_mount_points()
    for mount_point in mount_points:
        if mount_point.file_system["FileSystemId"] == efs_path.file_system_id:
            logger.debug(
                f"Found {mount_point} with matching file system id for efs path {efs_path}"
            )

            if not efs_path.path.is_relative_to(mount_point.access_point_path):
                logger.debug(
                    f"EFS Path {efs_path.path} is not relative "
                    f"to mount point access point {mount_point.access_point_path}. Skipping"
                )
                continue
            logger.info(f"Found matching mount point {mount_point} for efs path {efs_path}")
            return mount_point.as_mounted_path(efs_path.path)
    else:
        message = (
            f"Could not resolve local path for EFS path {efs_path} from "
            f"{len(mount_points)} mount points detected on host."
        )
        if raise_if_unmounted:
            logger.error(message)
            raise ValueError(message)
        logger.warning(message)
        return None
