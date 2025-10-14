from __future__ import annotations

import re
from math import ceil
from re import Pattern

__all__ = [
    "get_file_system",
    "list_file_systems",
]
import logging
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Tuple, Union

from aibs_informatics_core.collections import ValidatedStr

from aibs_informatics_aws_utils.core import AWSService

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_fsx.type_defs import (
        DataRepositoryAssociationTypeDef,
        FileSystemTypeDef,
        FilterTypeDef,
        TagTypeDef,
    )
else:
    FileSystemTypeDef = dict
    TagTypeDef = dict


logger = logging.getLogger(__name__)

get_fsx_client = AWSService.FSX.get_client

StrPath = Union[Path, str]


class FSxFileSystemId(ValidatedStr):
    regex_pattern: ClassVar[Pattern] = re.compile(r"fs-[0-9a-f]{17}")


FileSystemIdOrName = Union[FSxFileSystemId, str]
FileSystemNameOrId = Union[FSxFileSystemId, str]


def split_name_and_id(
    id_or_name: Optional[FileSystemNameOrId],
) -> Tuple[Optional[str], Optional[FSxFileSystemId]]:
    """Identify file system identifier as name or id

    Examples:
        INP: NAME1
        OUT: (NAME1, None)

        INP: ID1
        OUT: (None, ID1)

        INP: None
        OUT: (None, None)

    Args:
        id_or_name (Optional[FileSystemNameOrId]): File system id or name.


    Returns:
        Tuple[Optional[str], Optional[FSxFileSystemId]]: Tuple of name and id.
    """
    if not id_or_name:
        return None, None
    elif FSxFileSystemId.is_valid(id_or_name):
        return None, FSxFileSystemId(id_or_name)
    return id_or_name, None


def split_name_and_ids(
    names_or_ids: List[FileSystemNameOrId],
) -> Tuple[List[str], List[FSxFileSystemId]]:
    """Split file system combined list of names and ids into separate lists.

    Example:
        INP: [NAME1, ID1, NAME2, NAME3, ID2]
        OUT: ([NAME1, NAME2, NAME3], [ID1, ID2])

    Args:
        names_or_ids (List[FileSystemNameOrId]): List of names and/or ids.

    Returns:
        Tuple[List[str], List[FSxFileSystemId]]: Tuple of name and id lists.
    """
    if not names_or_ids:
        return [], []
    names, file_system_ids = zip(*(split_name_and_id(name_or_id) for name_or_id in names_or_ids))
    return [name for name in names if name], [
        file_system_id for file_system_id in file_system_ids if file_system_id
    ]


def resolve_file_system_ids(*name_or_ids: FileSystemNameOrId) -> List[FSxFileSystemId]:
    """Resolve file system ids from file system names or ids.

    Args:
        name_or_ids (Tuple[FileSystemNameOrId]): File system names or ids.

    Returns:
        str: File system id.
    """
    file_system_ids: List[FSxFileSystemId] = []
    for name_or_id in name_or_ids:
        name, file_system_id = split_name_and_id(name_or_id)
        if file_system_id:
            file_system_ids.append(file_system_id)
        else:
            file_system = get_file_system(name)
            if not file_system:
                raise ValueError(f"File system not found with name: {name}")
            file_system_ids.append(FSxFileSystemId(file_system.get("FileSystemId", "")))
    return file_system_ids


def get_file_system(
    name_or_id: Optional[FileSystemNameOrId] = None,
    tags: Optional[Dict[str, str]] = None,
) -> FileSystemTypeDef:
    """Get FSx file system.

    Args:
        file_system_id (Optional[str], optional): File system id.
        name (Optional[str], optional): File system name.
        tags (Optional[Dict[str, str]], optional): File system tags.

    Returns:
        FileSystemDescriptionTypeDef: File system description.
    """
    if not name_or_id and not tags:
        raise ValueError("At least one of file_system_id, name or tags must be provided.")

    file_systems = list_file_systems(name_or_ids=[name_or_id] if name_or_id else None, tags=tags)
    if len(file_systems) > 1:
        raise ValueError(
            f"Multiple file systems found with name/id: {name_or_id} and tags: {tags}"
        )
    if not file_systems:
        raise ValueError(f"File system not found with name/id: {name_or_id} and tags: {tags}")
    return file_systems[0]


def list_file_systems(
    name_or_ids: Optional[List[FileSystemNameOrId]] = None,
    tags: Optional[Dict[str, str]] = None,
    **kwargs,
) -> List[FileSystemTypeDef]:
    """List FSx file systems.

    You can filter on id, name and tags.

    Args:
        file_system_id (Optional[str], optional): Optionally filter on file system id.
        name (Optional[str], optional): Optionally filter on name.
        tags (Optional[Dict[str, str]], optional): Optionally filter on tags.

    Returns:
        List[FileSystemDescriptionTypeDef]: List of file systems.
    """
    client = get_fsx_client(**kwargs)
    paginator = client.get_paginator("describe_file_systems")
    names, file_system_ids = split_name_and_ids(name_or_ids or [])
    if file_system_ids:
        response_iter = paginator.paginate(FileSystemIds=file_system_ids)
    else:
        response_iter = paginator.paginate()
    file_systems: List[FileSystemTypeDef] = []
    for response in response_iter:
        filtered_file_systems = response["FileSystems"]
        if not names and not tags:
            file_systems.extend(filtered_file_systems)
            continue
        old_filtered_file_systems = filtered_file_systems
        filtered_file_systems = []
        for fs in old_filtered_file_systems:
            fs_tags_dict = {tag["Key"]: tag["Value"] for tag in fs.get("Tags", [])}
            if names and fs_tags_dict.get("Name") not in names:
                continue
            if tags and not all(
                tag in fs_tags_dict and value == fs_tags_dict[tag] for tag, value in tags.items()
            ):
                continue
            filtered_file_systems.append(fs)
        file_systems.extend(filtered_file_systems)
    return file_systems


def list_data_repository_associations(
    name_or_id: Optional[FileSystemIdOrName],
    filters: Optional[List[FilterTypeDef]] = None,
    # TODO: should I include data repository paths?
    data_repository_paths: Optional[List[str]] = None,
    **kwargs,
) -> List[DataRepositoryAssociationTypeDef]:
    """List data repository associations for a file system.

    Args:
        file_system_id (str): File system id.

    Returns:
        List[FileSystemTypeDef]: List of data repository associations.
    """
    client = get_fsx_client(**kwargs)
    if name_or_id:
        file_system_id = resolve_file_system_ids(name_or_id)
        if filters:
            if id_filter := next(
                (filter for filter in filters if filter.get("Name") == "file-system-id"), None
            ):
                id_filter["Values"] = list(id_filter.get("Values", [])) + [str(file_system_id)]
            else:
                filters.append({"Name": "file-system-id", "Values": file_system_id})
        else:
            filters = [{"Name": "file-system-id", "Values": file_system_id}]
    if not filters:
        filters = []
    associations: List[DataRepositoryAssociationTypeDef] = []
    response = client.describe_data_repository_associations(Filters=filters)
    while response["Associations"]:
        new_associations = response["Associations"]
        if data_repository_paths:
            new_associations = [
                association
                for association in new_associations
                if association.get("DataRepositoryPath") in data_repository_paths
            ]
        associations.extend(new_associations)
        if not (next_token := response.get("NextToken")):
            break
        response = client.describe_data_repository_associations(
            Filters=filters, NextToken=next_token
        )
    return associations


def calculate_size_required(bytes_required: int) -> int:
    """Calculate size of file system for the given bytes specified.

    FSx file systems are created with a size of
        - 1.2 TB,
        - 2.4 TB
        - any multiple 2.4 TB.

    Args:
        bytes_required (int): Bytes required.

    Returns:
        int: Size required.
    """
    BYTES_IN_TB = 1024 * 1024 * 1024 * 1024
    if bytes_required <= 1.2 * BYTES_IN_TB:
        return ceil(1.2 * BYTES_IN_TB)
    if bytes_required <= 2.4 * BYTES_IN_TB:
        return ceil(2.4 * BYTES_IN_TB)
    return ceil((bytes_required // (2.4 * BYTES_IN_TB) + 1) * 2.4 * BYTES_IN_TB)
