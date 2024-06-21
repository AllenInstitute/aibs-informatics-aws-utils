from __future__ import annotations

__all__ = ["BaseFileSystem", "LocalFileSystem", "S3FileSystem"]

import errno
import os
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import pytz
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.models.base import CustomAwareDateTime, custom_field
from aibs_informatics_core.models.base.model import SchemaModel
from aibs_informatics_core.utils.logging import get_logger
from aibs_informatics_core.utils.os_operations import find_all_paths
from aibs_informatics_core.utils.time import BEGINNING_OF_TIME
from aibs_informatics_core.utils.tools.strtools import removeprefix

from aibs_informatics_aws_utils.efs import get_efs_path, get_local_path
from aibs_informatics_aws_utils.s3 import get_s3_resource

logger = get_logger(__name__)

SEP = "/"


@dataclass
class PathStats(SchemaModel):
    size_bytes: int = custom_field()
    object_count: int = custom_field()
    last_modified: datetime = custom_field(mm_field=CustomAwareDateTime())


@dataclass(order=True)
class Node:
    """Represents an object or folder in an file system path.

    Args:
        path_part (str): specifies the key part of the fs path (an edge) to this node
        parent (Optional[Node]): Optionally specify the parent node to which
            this node is connected. By default, this is None.
        children (Dict[str, Node]): Child nodes that exist under this path prefix.
        size_bytes (int): The size (in bytes) of all objects under this path prefix.
        object_count (int): The number of objects under this path prefix.
        last_modified (datetime): The most recent date any objects under this prefix were
            last modified.

    """

    path_part: str
    parent: Optional["Node"] = field(default=None)
    children: Dict[str, "Node"] = field(default_factory=dict)
    size_bytes: int = field(default=0)
    object_count: int = field(default=0)
    last_modified: datetime = field(default=BEGINNING_OF_TIME)
    is_path_part_prefix: bool = field(default=False)
    is_path_part_suffix: bool = field(default=False)

    def __hash__(self) -> int:
        return hash(self.path)

    @property
    def key(self) -> str:
        return self.path

    @property
    def parent_path(self) -> str:
        parent_path = self.parent.path if self.parent else ""
        if parent_path and self.is_path_part_suffix:
            parent_path = parent_path.rstrip(SEP)
        return parent_path

    @property
    def normalized_path_part(self) -> str:
        path_part = self.path_part
        if self.has_children() and not self.is_path_part_prefix:
            path_part = path_part.rstrip(SEP) + SEP
        return path_part

    @property
    def path(self) -> str:
        return self.parent_path + self.normalized_path_part

    @property
    def path_stats(self) -> PathStats:
        return PathStats(
            size_bytes=self.size_bytes,
            object_count=self.object_count,
            last_modified=self.last_modified,
        )

    @property
    def depth(self) -> int:
        return self.parent.depth + 1 if self.parent else 0

    def has_children(self) -> bool:
        return not (len(self.children) < 1)

    def add_object(self, path: str, size: int, last_modified: datetime):
        def _add_object(node: Node, path: Optional[str]):
            node._update_stats(size=size, last_modified=last_modified)
            if path is None:
                return

            first_key_part, remaining_key = path.split(SEP, 1) if SEP in path else (path, None)
            if first_key_part:
                if first_key_part not in node.children:
                    node.children[first_key_part] = Node(path_part=first_key_part, parent=node)
                node = node.children[first_key_part]
            _add_object(node, remaining_key)

        # TODO: Right now, we cannot support non-folder prefixes
        _add_object(self, path.lstrip(SEP))

    def get(self, key: str) -> Optional["Node"]:
        try:
            return self[key]
        except KeyError:
            return None

    def list_nodes(self) -> List["Node"]:
        nodes = [self]
        for _, n in self.children.items():
            nodes.extend(n.list_nodes())
        return nodes

    def _update_stats(self, size: int, last_modified: datetime):
        # For each node, update the current node's stats
        self.size_bytes += size
        self.object_count += 1
        if self.last_modified < last_modified:
            self.last_modified = last_modified

    def __getitem__(self, key: str) -> "Node":
        _self = self
        for key_part in key.split(SEP):
            # Only access if value is not empty string
            if key_part:
                _self = _self.children[key_part]
        return _self

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"path_part={self.path_part}, "
            f"children={len(self.children)}, "
            f"size_bytes={self.size_bytes}, "
            f"object_count={self.object_count})"
        )


@dataclass  # type: ignore[misc] # mypy #5374
class BaseFileSystem:
    node: Node = field(init=False)

    def __post_init__(self):
        self.node = self.initialize_node()

    @abstractmethod
    def initialize_node(self) -> Node:
        raise NotImplementedError()

    @abstractmethod
    def refresh(self, **kwargs):
        raise NotImplementedError()

    def partition(
        self,
        size_bytes_limit: Optional[int] = None,
        object_count_limit: Optional[int] = None,
        raise_error_if_criteria_not_met: bool = False,
    ) -> List[Node]:
        """Partitions the root tree folder structure into a list of nodes

        Partitioning is guided by constraints by size and object count.

        Args:
            size_bytes_limit (Optional[int], optional): If specified, partitions must be
                less than the specified value.
            object_count_limit (Optional[int], optional): If specified, partitions
                must contain fewer objects than the specified valude
            raise_error_if_criteria_not_met (bool, optional): If True, raises error if nodes
                cannot meet criteria. In actuality, this is more relevant for size limitations
                where an object size is greater than the size limit

        Raises:
            ValueError: Thrown if raise_error_if_criteria_not_met is true and criteria not met

        Returns:
            List[Node]: List of nodes partitioning
        """
        unchecked_nodes = {self.node}
        size_bytes_exceeding_obj_nodes = []

        partitioned_nodes: List[Node] = []
        logger.info(
            f"Partitioning nodes with size_bytes_limit={size_bytes_limit} "
            f"and object_count_limit={object_count_limit}"
        )

        while unchecked_nodes:
            unchecked_node = unchecked_nodes.pop()
            if (size_bytes_limit and unchecked_node.size_bytes > size_bytes_limit) or (
                object_count_limit and unchecked_node.object_count > object_count_limit
            ):
                if unchecked_node.has_children():
                    unchecked_nodes.update(unchecked_node.children.values())
                else:
                    size_bytes_exceeding_obj_nodes.append(unchecked_node)
            else:
                partitioned_nodes.append(unchecked_node)

        if size_bytes_exceeding_obj_nodes:
            msg = (
                f"Found {len(size_bytes_exceeding_obj_nodes)} objects that exceed the "
                f"partition size limit {size_bytes_limit}."
            )
            if raise_error_if_criteria_not_met:
                raise ValueError(msg)
            logger.warning(msg)
            partitioned_nodes.extend(size_bytes_exceeding_obj_nodes)
        logger.info(f"Partitioned {len(partitioned_nodes)} nodes.")
        return partitioned_nodes

    @classmethod
    @abstractmethod
    def from_path(cls, path: str, **kwargs) -> BaseFileSystem:
        pass


@dataclass
class LocalFileSystem(BaseFileSystem):
    path: Path

    def initialize_node(self) -> Node:
        return Node(path_part=self.path.as_posix())

    def refresh(self, **kwargs):
        self.node = self.initialize_node()
        paths_to_visit = deque(find_all_paths(self.path, include_dirs=False, include_files=True))
        while paths_to_visit:
            path = paths_to_visit.popleft()
            try:
                path_stats = Path(path).stat()
                self.node.add_object(
                    path=removeprefix(path, str(self.path) + os.sep),
                    size=path_stats.st_size,
                    last_modified=datetime.fromtimestamp(path_stats.st_mtime, tz=pytz.UTC),
                )
            except FileNotFoundError:
                logger.warning(f"{path} does not exist. Not adding to {self}")
            except OSError as ose:
                # Suppress error if Stale File. This is expected error if file has been deleted:
                #   - https://stackoverflow.com/a/40351967
                #   - https://www.rfc-editor.org/rfc/rfc7530#section-4
                if ose.errno == errno.ESTALE:
                    logger.warning(f"{ose} raised for {path}.")
                    if Path(path).exists():
                        logger.warning(f"Adding {path} to end of list to check later.")
                        paths_to_visit.append(path)
                else:
                    logger.error(f"Unexpected error raised for {path}. Reason: {ose}")
                    raise ose

    @classmethod
    def from_path(cls, path: Union[str, Path], **kwargs) -> LocalFileSystem:
        local_path = Path(path)
        local_root = LocalFileSystem(path=local_path)
        local_root.refresh(**kwargs)
        return local_root


@dataclass
class EFSFileSystem(LocalFileSystem):
    efs_path: EFSPath

    def initialize_node(self) -> Node:
        return Node(path_part=self.efs_path)

    @classmethod
    def from_path(cls, path: Union[str, Path], **kwargs) -> EFSFileSystem:
        if isinstance(path, str) and EFSPath.is_valid(path):
            efs_path = EFSPath(path)
            local_path = get_local_path(efs_path=efs_path)
        else:
            local_path = Path(path)
            efs_path = get_efs_path(local_path=local_path)

        efs_root = EFSFileSystem(path=local_path, efs_path=efs_path)
        efs_root.refresh(**kwargs)
        return efs_root


@dataclass
class S3FileSystem(BaseFileSystem):
    """Generates a FS tree structure of an S3 path with size and object count stats

    Args:
        bucket (str): The S3 bucket to describe
        key (str): The S3 key to describe
        node (Node): The underlying root node of FS tree
    """

    bucket: str
    key: str

    def initialize_node(self) -> Node:
        return Node(path_part=self.key)

    def refresh(self, **kwargs):
        self.node = self.initialize_node()
        s3 = get_s3_resource(**kwargs)
        bucket = s3.Bucket(self.bucket)

        for obj in bucket.objects.filter(Prefix=self.key):
            self.node.add_object(
                path=removeprefix(obj.key, self.key),
                size=obj.size,
                last_modified=obj.last_modified,
            )

    @classmethod
    def from_path(cls, path: str, **kwargs) -> S3FileSystem:
        s3_path = S3URI(path)
        s3_root = S3FileSystem(bucket=s3_path.bucket, key=s3_path.key)
        s3_root.refresh(**kwargs)
        return s3_root


def get_file_system(path: Union[str, Path]) -> BaseFileSystem:
    if isinstance(path, str) and S3URI.is_valid(path):
        return S3FileSystem.from_path(path)
    elif isinstance(path, str) and EFSPath.is_valid(path):
        return EFSFileSystem.from_path(path)
    else:
        return LocalFileSystem.from_path(path)
