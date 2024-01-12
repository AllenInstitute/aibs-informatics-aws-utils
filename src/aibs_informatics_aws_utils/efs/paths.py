from __future__ import annotations

__all__ = [
    "EFSLocalPath",
    "EFSCloudPath",
    "get_efs_path",
    "get_local_path",
]

import logging
import os
import pathlib
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Literal, Optional, Tuple, Union, overload

from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.utils.file_operations import (
    copy_path,
    find_all_paths,
    move_path,
    remove_path,
)

from aibs_informatics_aws_utils.efs.core import get_efs_client
from aibs_informatics_aws_utils.efs.mount_point import MountPointConfiguration, detect_mount_points

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_efs import EFSClient
else:
    EFSClient = object

logger = logging.getLogger(__name__)

StrPath = Union[Path, str]


@overload
def get_efs_path(
    local_path: Path,
    raise_if_unresolved: Literal[False],
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Optional[EFSPath]:
    ...


@overload
def get_efs_path(
    local_path: Path,
    raise_if_unresolved: Literal[True] = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> EFSPath:
    ...


def get_efs_path(
    local_path: Path,
    raise_if_unresolved: bool = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Optional[EFSPath]:
    """Converts a local path assumed to be on a mount point to the EFS path

    Args:
        local_path (Path): Local path
        raise_if_unresolved (bool): If True, raises an error if the local path is not under an identifiable mount point. Defaults to True.
        mount_points (List[MountPointConfiguration] | None): Optionally can override list of mount_points.
            If None, mount points are detected. Defaults to None.

    Returns:
        EFSPath: Corresponding EFS URI or None if the path cannot be resolved and raise_if_unresolved is False
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
) -> Optional[Path]:
    ...


@overload
def get_local_path(
    efs_path: EFSPath,
    raise_if_unmounted: Literal[True] = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Path:
    ...


def get_local_path(
    efs_path: EFSPath,
    raise_if_unmounted: bool = True,
    mount_points: Optional[List[MountPointConfiguration]] = None,
) -> Optional[Path]:
    """Gets a valid locally mounted path for the given EFS path.

    Args:
        efs_path (EFSPath): The EFS path. e.g., "efs://fs-12345678:/path/to/file.txt"
        raise_if_unmounted (bool): If True, raises an error if the EFS path is not mounted locally. Defaults to True.
        mount_points (List[MountPointConfiguration] | None): Optionally can override list of mount points.
            If None, mount points are detected. Defaults to None.

    Returns:
        Path: The local path. e.g., "/mnt/efs/path/to/file.txt" or None if the path cannot be resolved and raise_if_unmounted is False
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


from cloudpathlib.client import Client, register_client_class
from cloudpathlib.cloudpath import CloudPath, register_path_class


@register_client_class("efs")
class EFSCloudClient(Client):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._client: EFSClient = get_efs_client(**kwargs)

    def _download_file(self, cloud_path: EFSCloudPath, local_path: Union[str, PathLike]) -> Path:
        copy_path(cloud_path.as_local_path(), new_local_path := Path(local_path))
        return new_local_path

    def _exists(self, cloud_path: EFSCloudPath) -> bool:
        return cloud_path.as_local_path().exists()

    def _list_dir(
        self, cloud_path: EFSCloudPath, recursive: bool
    ) -> Iterable[Tuple[EFSCloudPath, bool]]:
        local_path = cloud_path.as_local_path()
        if not recursive:
            return [(EFSCloudPath(path), path.is_dir()) for path in local_path.iterdir()]
        paths = find_all_paths(local_path, include_dirs=True, include_files=True)
        return [
            (EFSCloudPath(full_path := local_path / path), full_path.is_dir()) for path in paths
        ]

    def _move_file(
        self, src: EFSCloudPath, dst: EFSCloudPath, remove_src: bool = True
    ) -> EFSCloudPath:
        if remove_src:
            move_path(src.as_local_path(), dst.as_local_path())
        else:
            copy_path(src.as_local_path(), dst.as_local_path())

    def _remove(self, path: EFSCloudPath, missing_ok: bool = True) -> None:
        remove_path(path.as_local_path(), missing_ok=missing_ok)

    def _upload_file(self, local_path: str | PathLike, cloud_path: EFSCloudPath) -> EFSCloudPath:
        copy_path(local_path, cloud_path.as_local_path())
        return cloud_path


@register_path_class("efs")
class EFSCloudPath(CloudPath):
    """Class for representing and operating on AWS S3 URIs, in the style of the Python standard
    library's [`pathlib` module](https://docs.python.org/3/library/pathlib.html). Instances
    represent a path in S3 with filesystem path semantics, and convenient methods allow for basic
    operations like joining, reading, writing, iterating over contents, etc. This class almost
    entirely mimics the [`pathlib.Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path)
    interface, so most familiar properties and methods should be available and behave in the
    expected way.

    The [`S3Client`](../s3client/) class handles authentication with AWS. If a client instance is
    not explicitly specified on `S3Path` instantiation, a default client is used. See `S3Client`'s
    documentation for more details.
    """

    cloud_prefix: str = "efs://"
    client: EFSCloudClient

    def __init__(
        self,
        cloud_path: Union[str, CloudPath, EFSLocalPath],
        client: Optional[Client] = None,
        **kwargs,
    ) -> None:
        if isinstance(cloud_path, EFSLocalPath):
            self._efs_path = cloud_path.efs_path
            self._local_path = cloud_path
            cloud_path = self._efs_path.as_uri(False)
        if isinstance(cloud_path, str) and EFSPath.is_valid(cloud_path):
            self._efs_path = EFSPath(cloud_path)
            self._local_path = get_local_path(self._efs_path)
            cloud_path = self._efs_path.as_uri(False)
        elif isinstance(cloud_path, (str, Path)):
            self._local_path = Path(cloud_path)
            self._efs_path = get_efs_path(self._local_path)
            cloud_path = self._efs_path.as_uri(False)
        else:
            self._efs_path = EFSPath(cloud_path._str)
            self._local_path = get_local_path(self._efs_path)
        super().__init__(cloud_path, client)

    @property
    def drive(self) -> str:
        return self._efs_path.file_system_id

    def as_uri(self) -> EFSPath:
        return EFSPath(super().as_uri())

    def as_local_path(self) -> EFSLocalPath:
        return self._local_path

    def is_dir(self) -> bool:
        return self.as_local_path().is_dir()

    def is_file(self) -> bool:
        return self.as_local_path().is_file()

    def mkdir(self, parents: bool = False, exist_ok: bool = False):
        return self.as_local_path().mkdir(parents=parents, exist_ok=exist_ok)

    def touch(self, exist_ok: bool = True):
        self.as_local_path().touch(exist_ok=exist_ok)

    def stat(self):
        return self.as_local_path().stat()


# Determine the correct class to inherit from based on the OS
PathBase = pathlib.WindowsPath if os.name == "nt" else pathlib.PosixPath


class EFSLocalPath(PathBase):
    def __init__(
        self,
        *args,
        raise_if_unmounted: bool = True,
        raise_if_unresolved: bool = True,
        mount_points: Optional[List[MountPointConfiguration]] = None,
        **kwargs,
    ):
        raw_path = args[0]
        if EFSPath.is_valid(raw_path):
            self._efs_path = EFSPath(raw_path)
            local_path = get_local_path(
                self._efs_path,
                raise_if_unmounted=raise_if_unmounted,
                mount_points=mount_points,
            )
            args = (local_path, *args[1:])
        else:
            local_path = Path(raw_path)
            self._efs_path = get_efs_path(
                local_path,
                raise_if_unresolved=raise_if_unresolved,
                mount_points=mount_points,
            )
        super().__init__(*args, **kwargs)

    @property
    def efs_path(self) -> EFSPath:
        return self._efs_path
