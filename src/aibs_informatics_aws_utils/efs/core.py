from __future__ import annotations

__all__ = [
    "get_efs_client",
    "list_efs_file_systems",
    "get_efs_file_system",
    "list_efs_access_points",
    "get_efs_access_point",
]

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from aibs_informatics_core.utils.decorators import retry
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService, client_error_code_check

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_efs.type_defs import (
        AccessPointDescriptionTypeDef,
        DescribeAccessPointsResponseTypeDef,
        DescribeFileSystemsResponseTypeDef,
        FileSystemDescriptionTypeDef,
        TagTypeDef,
    )
else:
    AccessPointDescriptionTypeDef = dict
    DescribeFileSystemsResponseTypeDef = dict
    DescribeAccessPointsResponseTypeDef = dict
    FileSystemDescriptionTypeDef = dict
    TagTypeDef = dict


logger = logging.getLogger(__name__)

get_efs_client = AWSService.EFS.get_client

StrPath = Union[Path, str]


def throttling_exception_callback(ex):
    return client_error_code_check(ex, "ThrottlingException")


@retry(ClientError, [throttling_exception_callback])
def list_efs_file_systems(
    file_system_id: Optional[str] = None,
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> List[FileSystemDescriptionTypeDef]:
    """List EFS file systems.

    You can filter on id, name and tags.

    Args:
        file_system_id (Optional[str], optional): Optionally filter on file system id.
        name (Optional[str], optional): Optionally filter on name.
        tags (Optional[Dict[str, str]], optional): Optionally filter on tags.
            They should be a dict of key-value pairs.

    Returns:
        List[FileSystemDescriptionTypeDef]: List of matching file systems
    """
    efs = get_efs_client()
    paginator = efs.get_paginator("describe_file_systems")

    file_systems: List[FileSystemDescriptionTypeDef] = []
    paginator_kwargs = remove_null_values(dict(FileSystemId=file_system_id))
    for results in paginator.paginate(**paginator_kwargs):  # type: ignore
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
    """Get EFS file system.

    You can filter on id, name and tags.

    Args:
        file_system_id (Optional[str], optional): Optionally filter on file system id.
        name (Optional[str], optional): Optionally filter on name.
        tags (Optional[Dict[str, str]], optional): Optionally filter on tags.
            They should be a dict of key-value pairs.

    Raises:
        ValueError: If no file system is found based on the filters.
        ValueError: If more than one file system is found based on the filters.

    Returns:
        FileSystemDescriptionTypeDef: The file system description.
    """
    file_systems = list_efs_file_systems(file_system_id=file_system_id, name=name, tags=tags)
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


@retry(ClientError, [throttling_exception_callback])
def list_efs_access_points(
    access_point_id: Optional[str] = None,
    access_point_name: Optional[str] = None,
    access_point_tags: Optional[Dict[str, str]] = None,
    file_system_id: Optional[str] = None,
    file_system_name: Optional[str] = None,
    file_system_tags: Optional[Dict[str, str]] = None,
) -> List[AccessPointDescriptionTypeDef]:
    """List EFS access points.

    You can filter on id, name and tags for both access point and file system.

    Args:
        access_point_id (Optional[str], optional): Optionally filter on access point id.
        access_point_name (Optional[str], optional): Optionally filter on name.
        access_point_tags (Optional[Dict[str, str]], optional): Optionally filter on access point
            tags. They should be a dict of key-value pairs.
        file_system_id (Optional[str], optional): Optionally filter on file system id.
        file_system_name (Optional[str], optional): Optionally filter on file system name.
        file_system_tags (Optional[Dict[str, str]], optional): Optionally filter on file system
            tags. They should be a dict of key-value pairs.

    Returns:
        List[AccessPointDescriptionTypeDef]: List of matching access points
    """
    efs = get_efs_client()

    file_system_ids: List[str] = []
    if file_system_id:
        file_system_ids.append(file_system_id)
    elif file_system_name or file_system_tags:
        file_systems = list_efs_file_systems(
            file_system_id=file_system_id, name=file_system_name, tags=file_system_tags
        )
        file_system_ids.extend(map(lambda _: _["FileSystemId"], file_systems))

    access_points: List[AccessPointDescriptionTypeDef] = []

    if access_point_id or not file_system_ids:
        response = efs.describe_access_points(
            **remove_null_values(dict(AccessPointId=access_point_id))  # type: ignore
        )
        # If file_system_ids is empty, we want to include all access points. Otherwise,
        # we only want to include access points that belong to the file systems
        # in file_system_ids.
        for access_point in response["AccessPoints"]:
            if not file_system_ids or access_point.get("FileSystemId") in file_system_ids:
                access_points.append(access_point)
    else:
        for fs_id in file_system_ids:
            response = efs.describe_access_points(FileSystemId=fs_id)
            access_points.extend(response["AccessPoints"])
            while next_token := response.get("NextToken"):
                response = efs.describe_access_points(FileSystemId=fs_id, NextToken=next_token)
                access_points.extend(response["AccessPoints"])

    filtered_access_points: List[AccessPointDescriptionTypeDef] = []

    for ap in access_points:
        if access_point_name and ap.get("Name") != access_point_name:
            continue
        if access_point_tags:
            ap_tags = {tag["Key"]: tag["Value"] for tag in ap.get("Tags", {})}
            tags_match = [access_point_tags[k] == ap_tags.get(k) for k in access_point_tags]
            if not all(tags_match):
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
    """Get EFS access point.

    You can filter on id, name and tags for both access point and file system.

    Args:
        access_point_id (Optional[str], optional): Optionally filter on access point id.
        access_point_name (Optional[str], optional): Optionally filter on name.
        access_point_tags (Optional[Dict[str, str]], optional): Optionally filter on access point
            tags. They should be a dict of key-value pairs.
        file_system_id (Optional[str], optional): Optionally filter on file system id.
        file_system_name (Optional[str], optional): Optionally filter on file system name.
        file_system_tags (Optional[Dict[str, str]], optional): Optionally filter on file system
            tags. They should be a dict of key-value pairs.

    Raises:
        ValueError: If no access point is found based on the filters.
        ValueError: If more than one access point is found based on the filters.

    Returns:
        AccessPointDescriptionTypeDef: The access point description.
    """
    access_points = list_efs_access_points(
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
            f"based on access point filters (id={access_point_id}, "
            f"name={access_point_name}, tags={access_point_tags}) "
            f"and on file system filters (id={file_system_id}, "
            f"name={file_system_name}, tags={file_system_tags}) "
        )
    elif len(access_points) == 0:
        raise ValueError(
            f"Found no access points "
            f"based on access point filters (id={access_point_id}, "
            f"name={access_point_name}, tags={access_point_tags}) "
            f"and on file system filters (id={file_system_id}, "
            f"name={file_system_name}, tags={file_system_tags}) "
        )
    return access_points[0]
