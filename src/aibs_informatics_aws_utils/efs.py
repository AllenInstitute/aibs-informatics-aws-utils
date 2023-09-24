import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from aibs_informatics_core.utils.os_operations import get_env_var
from aibs_informatics_core.utils.tools.dict_helpers import remove_null_values

from aibs_informatics_aws_utils.core import AWSService

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_efs.type_defs import (
        AccessPointDescriptionTypeDef,
        DescribeAccessPointsRequestRequestTypeDef,
        DescribeAccessPointsResponseTypeDef,
        DescribeFileSystemsResponseTypeDef,
        FileSystemDescriptionTypeDef,
        TagTypeDef,
    )
else:
    AccessPointDescriptionTypeDef = dict
    DescribeFileSystemsResponseTypeDef = dict
    DescribeAccessPointsRequestRequestTypeDef = dict
    DescribeAccessPointsResponseTypeDef = dict
    FileSystemDescriptionTypeDef = dict
    TagTypeDef = dict


logger = logging.getLogger(__name__)

get_efs_client = AWSService.EFS.get_client


EFS_MOUNT_PATH_VAR = "EFS_MOUNT_PATH"


def get_efs_mount_path(default_root: Optional[Union[str, Path]] = None) -> str:
    root = get_env_var(EFS_MOUNT_PATH_VAR) or str(default_root)
    if root is None:
        raise ValueError("No EFS mounted file system could be resolved from env var")
    return root


def get_efs_file_systems(
    file_system_id: Optional[str] = None,
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> List[FileSystemDescriptionTypeDef]:
    efs = get_efs_client()
    paginator = efs.get_paginator("describe_file_systems")

    file_systems: List[FileSystemDescriptionTypeDef] = []
    paginator_kwargs = remove_null_values(dict(FileSystemId=file_system_id))
    for results in paginator.paginate(**paginator_kwargs):
        for fs in results["FileSystems"]:
            if name and fs.get("Name") != name:
                continue
            if tags:
                fs_tags = {tag["Key"]: tag["Value"] for tag in fs["Tags"]}
                if not all([tags[k] == fs_tags.get(k) for k in tags]):
                    continue
            file_systems.append(fs)
    return file_systems


def get_efs_file_system(
    file_system_id: Optional[str] = None,
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> FileSystemDescriptionTypeDef:
    file_systems = get_efs_file_systems(file_system_id=file_system_id, name=name, tags=tags)
    if len(file_systems) > 1:
        raise ValueError(
            f"Found more than one file systems ({len(file_systems)}) "
            f"based on id={file_system_id}, name={name}, tags={tags}"
        )
    elif len(file_systems) == 0:
        raise ValueError(
            f"Found no file systems based on id={file_system_id}, name={name}, tags={tags}"
        )
    return file_systems[0]


def get_efs_access_points(
    access_point_id: Optional[str] = None,
    access_point_name: Optional[str] = None,
    access_point_tags: Optional[Dict[str, str]] = None,
    file_system_id: Optional[str] = None,
    file_system_name: Optional[str] = None,
    file_system_tags: Optional[Dict[str, str]] = None,
) -> List[AccessPointDescriptionTypeDef]:
    efs = get_efs_client()

    file_system_ids: List[Optional[str]] = []
    if file_system_id:
        file_system_ids.append(file_system_id)
    elif file_system_name or file_system_tags:
        file_systems = get_efs_file_systems(
            file_system_id=file_system_id, name=file_system_name, tags=file_system_tags
        )
        file_system_ids.extend(map(lambda _: _["FileSystemId"], file_systems))
    else:
        file_system_ids.append(None)

    access_points: List[AccessPointDescriptionTypeDef] = []

    for fs_id in file_system_ids:
        response = efs.describe_access_points(
            **remove_null_values(dict(AccessPointId=access_point_id, FileSystemId=fs_id))
        )
        access_points.extend(response["AccessPoints"])
        while response.get("NextToken"):
            response = efs.describe_access_points(
                **remove_null_values(
                    dict(
                        AccessPointId=access_point_id,
                        FileSystemId=fs_id,
                        NextToken=response["NextToken"],
                    )
                )
            )
            access_points.extend(response["AccessPoints"])

    filtered_access_points: List[AccessPointDescriptionTypeDef] = []

    for ap in access_points:
        if access_point_name and ap.get("Name") != access_point_name:
            continue
        if access_point_tags:
            ap_tags = {tag["Key"]: tag["Value"] for tag in ap.get("Tags", {})}
            if not all([access_point_tags[k] == ap_tags.get(k) for k in access_point_tags]):
                continue
        filtered_access_points.append(ap)
    return filtered_access_points


def get_efs_access_point(
    access_point_id: Optional[str] = None,
    access_point_name: Optional[str] = None,
    access_point_tags: Optional[Dict[str, str]] = None,
    file_system_id: Optional[str] = None,
    file_system_name: Optional[str] = None,
    file_system_tags: Optional[Dict[str, str]] = None,
) -> AccessPointDescriptionTypeDef:
    access_points = get_efs_access_points(
        access_point_id=access_point_id,
        access_point_name=access_point_name,
        access_point_tags=access_point_tags,
        file_system_id=file_system_id,
        file_system_name=file_system_name,
        file_system_tags=file_system_tags,
    )
    if len(access_points) > 1:
        raise ValueError(
            f"Found more than one access points ({len(access_points)}) "
            f"based on access point filters (id={access_point_id}, name={access_point_id}, tags={access_point_tags}) "
            f"and on file system filters (id={file_system_id}, name={file_system_name}, tags={file_system_tags}) "
        )
    elif len(access_points) == 0:
        raise ValueError(
            f"Found no access points "
            f"based on access point filters (id={access_point_id}, name={access_point_id}, tags={access_point_tags}) "
            f"and on file system filters (id={file_system_id}, name={file_system_name}, tags={file_system_tags}) "
        )
    return access_points[0]
