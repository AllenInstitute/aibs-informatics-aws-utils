from __future__ import annotations

__all__ = ["BaseFileSystem", "LocalFileSystem", "S3FileSystem"]

import errno
import os
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pytz
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.base import AwareIsoDateTime, PydanticBaseModel
from aibs_informatics_core.models.data_sync import DataSyncFilterConfig
from aibs_informatics_core.utils.filters import filter_paths, path_matches_filters
from aibs_informatics_core.utils.logging import get_logger
from aibs_informatics_core.utils.os_operations import find_all_paths
from aibs_informatics_core.utils.time import BEGINNING_OF_TIME
from aibs_informatics_core.utils.tools.strtools import removeprefix

from aibs_informatics_aws_utils.data_sync._filters import extract_filter_patterns
from aibs_informatics_aws_utils.efs import get_efs_path, get_local_path
from aibs_informatics_aws_utils.s3 import get_s3_resource

logger = get_logger(__name__)

SEP = "/"


class PathStats(PydanticBaseModel):
    size_bytes: int
    object_count: int
    last_modified: AwareIsoDateTime


@dataclass(order=True)
class Node:
    """Represents an object or folder in an file system path.

    Attributes:
        path_part: Specifies the key part of the fs path (an edge) to this node.
        parent: Optionally specify the parent node to which
            this node is connected. By default, this is None.
        children: Child nodes that exist under this path prefix.
        size_bytes: The size (in bytes) of all objects under this path prefix.
        object_count: The number of objects under this path prefix.
        last_modified: The most recent date any objects under this prefix were
            last modified.
    """

    path_part: str
    parent: Node | None = field(default=None)
    children: dict[str, Node] = field(default_factory=dict)
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
        def _add_object(node: Node, path: str | None):
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

    def get(self, key: str) -> Node | None:
        try:
            return self[key]
        except KeyError:
            return None

    def list_nodes(self) -> list[Node]:
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

    def __getitem__(self, key: str) -> Node:
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
    #: Number of objects encountered during the last :meth:`refresh`, counted
    #: *before* include/exclude filters are applied. The tree itself only holds
    #: the kept objects, so this is the only way to tell "the source was empty"
    #: apart from "the filters matched nothing" -- which is what the prepare
    #: handler needs in order to fail a typo'd pattern loudly rather than
    #: launching Batch jobs against an empty input set.
    total_objects_seen: int = field(init=False, default=0)

    def __post_init__(self):
        self.node = self.initialize_node()

    @abstractmethod
    def initialize_node(self) -> Node:
        raise NotImplementedError()

    @abstractmethod
    def refresh(
        self,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
        **kwargs,
    ):
        """Rebuild the tree, optionally keeping only the paths that pass filters.

        Args:
            filter_config: Optional include/exclude filters. When given, only
                matching objects contribute to the tree -- and therefore to the
                sizes that :meth:`partition` bins on.
            filter_root: Root that patterns are matched relative to. Defaults to
                this file system's own root. The distributed sync workflow splits
                a sync into sub-requests rooted at sub-prefixes, and those must
                pass the *original* root here or patterns stop matching.
            **kwargs: Additional arguments passed to the underlying client.
        """
        raise NotImplementedError()

    @property
    def kept_objects(self) -> int:
        """Number of objects retained by the last :meth:`refresh`."""
        return self.node.object_count

    @abstractmethod
    def resolve_filter_root(self, filter_root: str | None) -> str:
        """Resolve the root that filter patterns are matched relative to.

        Args:
            filter_root: Explicit root, or None to use this file system's own root.

        Returns:
            The root to relativize paths against.
        """
        raise NotImplementedError()

    def partition(
        self,
        size_bytes_limit: int | None = None,
        object_count_limit: int | None = None,
        raise_error_if_criteria_not_met: bool = False,
    ) -> list[Node]:
        """Partitions the root tree folder structure into a list of nodes.

        Partitioning is guided by constraints by size and object count.

        Args:
            size_bytes_limit: If specified, partitions must be less than the specified value.
            object_count_limit: If specified, partitions must contain fewer objects than
                the specified value.
            raise_error_if_criteria_not_met: If True, raises error if nodes cannot meet
                criteria. In actuality, this is more relevant for size limitations where
                an object size is greater than the size limit.

        Raises:
            ValueError: Thrown if raise_error_if_criteria_not_met is true and criteria not met.

        Returns:
            List of nodes representing the partition.
        """
        unchecked_nodes = {self.node}
        size_bytes_exceeding_obj_nodes = []

        partitioned_nodes: list[Node] = []
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

    def resolve_filter_root(self, filter_root: str | None) -> str:
        return filter_root if filter_root is not None else str(self.path)

    def refresh(
        self,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
        **kwargs,
    ):
        self.node = self.initialize_node()
        all_paths = find_all_paths(self.path, include_dirs=False, include_files=True)
        self.total_objects_seen = len(all_paths)
        include, exclude = extract_filter_patterns(filter_config)
        paths_to_visit = deque(
            filter_paths(
                all_paths,
                root=self.resolve_filter_root(filter_root),
                include=include,
                exclude=exclude,
            )
        )
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
    def from_path(
        cls,
        path: str | Path,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
        **kwargs,
    ) -> LocalFileSystem:
        local_path = Path(path)
        local_root = LocalFileSystem(path=local_path)
        local_root.refresh(filter_config=filter_config, filter_root=filter_root, **kwargs)
        return local_root


@dataclass
class EFSFileSystem(LocalFileSystem):
    efs_path: EFSPath

    def initialize_node(self) -> Node:
        return Node(path_part=self.efs_path)

    def resolve_filter_root(self, filter_root: str | None) -> str:
        # The paths being filtered are local mount paths, but a caller upstream
        # (e.g. the prepare handler) naturally expresses the filter root as the
        # EFS path it was given. Translate so patterns anchor where they should.
        if filter_root is not None and EFSPath.is_valid(filter_root):
            return str(get_local_path(efs_path=EFSPath(filter_root)))
        return super().resolve_filter_root(filter_root)

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
        **kwargs,
    ) -> EFSFileSystem:
        if isinstance(path, str) and EFSPath.is_valid(path):
            efs_path = EFSPath(path)
            local_path = get_local_path(efs_path=efs_path)
        else:
            local_path = Path(path)
            efs_path = get_efs_path(local_path=local_path)

        efs_root = EFSFileSystem(path=local_path, efs_path=efs_path)
        efs_root.refresh(filter_config=filter_config, filter_root=filter_root, **kwargs)
        return efs_root


@dataclass
class S3FileSystem(BaseFileSystem):
    """Generates a FS tree structure of an S3 path with size and object count stats.

    Attributes:
        bucket: The S3 bucket to describe.
        key: The S3 key to describe.
    """

    bucket: str
    key: str

    def initialize_node(self) -> Node:
        return Node(path_part=self.key)

    @property
    def s3_path(self) -> S3Path:
        return S3Path.build(bucket_name=self.bucket, key=self.key)

    def resolve_filter_root(self, filter_root: str | None) -> str:
        return filter_root if filter_root is not None else str(self.s3_path)

    def refresh(
        self,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
        **kwargs,
    ):
        self.node = self.initialize_node()
        self.total_objects_seen = 0
        s3 = get_s3_resource(**kwargs)
        bucket = s3.Bucket(self.bucket)

        resolved_filter_root = self.resolve_filter_root(filter_root)
        include, exclude = extract_filter_patterns(filter_config)

        # Filtering happens in-loop rather than via `list_s3_paths` so that a
        # filtered prefix is still walked in a single streaming pass -- these
        # prefixes run to millions of objects and we never want the full listing
        # materialized just to throw most of it away.
        for obj in bucket.objects.filter(Prefix=self.key):
            self.total_objects_seen += 1
            if not path_matches_filters(
                S3Path.build(bucket_name=self.bucket, key=obj.key),
                root=resolved_filter_root,
                include=include,
                exclude=exclude,
            ):
                continue
            self.node.add_object(
                path=removeprefix(obj.key, self.key),
                size=obj.size,
                last_modified=obj.last_modified,
            )

    @classmethod
    def from_path(
        cls,
        path: str,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
        **kwargs,
    ) -> S3FileSystem:
        s3_path = S3Path(path)
        s3_root = S3FileSystem(bucket=s3_path.bucket, key=s3_path.key)
        s3_root.refresh(filter_config=filter_config, filter_root=filter_root, **kwargs)
        return s3_root


def get_file_system(
    path: str | Path,
    filter_config: DataSyncFilterConfig | None = None,
    filter_root: str | None = None,
) -> BaseFileSystem:
    """Build the file system tree appropriate to the given path.

    Args:
        path: An S3 path, EFS path, or local path.
        filter_config: Optional include/exclude filters restricting the tree to
            matching objects.
        filter_root: Root that patterns are matched relative to. Defaults to
            ``path``.

    Returns:
        The refreshed file system.
    """
    if isinstance(path, str) and S3Path.is_valid(path):
        return S3FileSystem.from_path(path, filter_config=filter_config, filter_root=filter_root)
    elif isinstance(path, str) and EFSPath.is_valid(path):
        return EFSFileSystem.from_path(path, filter_config=filter_config, filter_root=filter_root)
    else:
        return LocalFileSystem.from_path(
            path, filter_config=filter_config, filter_root=filter_root
        )
