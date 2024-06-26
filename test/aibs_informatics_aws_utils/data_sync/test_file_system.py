import errno
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from test.aibs_informatics_aws_utils.base import AwsBaseTest
from test.aibs_informatics_aws_utils.efs.base import EFSTestsBase
from test.base import BaseTest, does_not_raise
from typing import Dict, List, Mapping, Optional, Set, Tuple, Union
from unittest import mock
from urllib import parse

import pytz
import requests
from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3URI, S3PathStats
from aibs_informatics_core.utils.time import get_current_time
from aibs_informatics_core.utils.tools.strtools import removeprefix
from moto import mock_sts
from pytest import mark, param, raises

from aibs_informatics_aws_utils.data_sync.file_system import (
    EFSFileSystem,
    LocalFileSystem,
    Node,
    S3FileSystem,
)
from aibs_informatics_aws_utils.efs import MountPointConfiguration
from aibs_informatics_aws_utils.efs.mount_point import detect_mount_points


def any_s3_uri(bucket: str = "bucket", key: str = "key") -> S3URI:
    return S3URI.build(bucket, key)


class NodeTests(BaseTest):
    def test__depth__provides_expected(self):
        n = Node("/path/to/my/root")
        n.add_object("d1/x", 1, get_current_time())
        n.add_object("d1/d2/x", 1, get_current_time())
        n.add_object("d1/d2/y", 1, get_current_time())
        self.assertEqual(n.depth, 0)

        self.assertEqual(n["d1"].depth, 1)
        self.assertEqual(n["d1/x"].depth, 2)
        self.assertEqual(n["d1/d2"].depth, 2)
        self.assertEqual(n["d1/d2/x"].depth, 3)

    def test__key__property(self):
        n = Node("/path/to/my/root")
        assert n.key == n.path

    def test__path_stats__property(self):
        n = Node("/root")
        n.add_object("d1/x", 1, get_current_time())
        self.assertEqual(n.path_stats.size_bytes, 1)

    def test__repr__works(self):
        n = Node("/root")
        assert isinstance(str(n), str)

    def test__get__works(self):
        n = Node("/path/to/my/root")
        n.add_object("d1/x", 1, get_current_time())
        n.add_object("d1/d2/x", 1, get_current_time())
        n.add_object("d1/d2/y", 1, get_current_time())

        assert n.get("doesnotexist") is None

        d1 = n.get("d1")
        assert d1 is not None
        self.assertEqual(d1.path, "/path/to/my/root/d1/")

        d2 = n.get("d1/d2")
        assert d2 is not None
        self.assertEqual(d2.path, "/path/to/my/root/d1/d2/")

    def test__list_nodes__works(self):
        n = Node("/path/to/my/root")
        n.add_object("d1/x", 1, get_current_time())
        n.add_object("d1/d2/x", 1, get_current_time())
        n.add_object("d1/d2/y", 1, get_current_time())

        nodes = n.list_nodes()
        actual_paths = [node.path for node in nodes]
        expected_paths = [
            "/path/to/my/root/",
            "/path/to/my/root/d1/",
            "/path/to/my/root/d1/x",
            "/path/to/my/root/d1/d2/",
            "/path/to/my/root/d1/d2/x",
            "/path/to/my/root/d1/d2/y",
        ]
        self.assertListEqual(actual_paths, expected_paths)


class LocalFileSystemTests(BaseTest):
    def setUp(self) -> None:
        super().setUp()

    def create_local_file_system(
        self, file_stats_map: Mapping[Union[Path, str], Tuple[int]]
    ) -> Path:
        root_file_system = self.tmp_path()
        for relative_path, (size,) in file_stats_map.items():
            full_path = root_file_system / relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text("0" * size)
        return root_file_system

    @mock.patch("aibs_informatics_aws_utils.data_sync.file_system.find_all_paths")
    @mock.patch("aibs_informatics_aws_utils.data_sync.file_system.Path")
    def test__refresh__handles_errors(self, mock_Path, mock_find_all_paths):
        root = self.tmp_path()
        a = root / "a"
        b = root / "b"

        mock_find_all_paths.return_value = [str(a), str(b)]
        p1 = mock.MagicMock()
        p1.stat.side_effect = FileNotFoundError()
        p2 = mock.MagicMock()
        ose = OSError()
        ose.errno = errno.ESTALE
        p2.stat.side_effect = ose

        p2.exists.side_effect = [True, False]
        mock_Path.side_effect = [p1, p2, p2, p2, p2]
        (n := LocalFileSystem(root)).refresh()

        with self.assertRaises(OSError):
            ose.errno = errno.ETIME
            p1.stat.side_effect = ose
            mock_find_all_paths.return_value = ["a"]
            mock_Path.side_effect = [p1]
            LocalFileSystem(root).refresh()

    def test__partition__partitions_by_size__partitions_to_object_level(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/A/X": (5,),
                "A/A/Y": (5,),
                "A/B/X": (5,),
                "A/B/Y": (5,),
            },
            size_bytes_limit=6,
            expected_node_paths={
                f"A/A/X",
                f"A/A/Y",
                f"A/B/X",
                f"A/B/Y",
            },
        )

    def test__partition__partitions_by_size__eats_error_obj_too_large(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/X": (5,),
                "A/B/X": (2,),
                "A/B/Y": (2,),
            },
            size_bytes_limit=4,
            expected_node_paths={
                f"A/X",
                f"A/B/",
            },
        )

    def test__partition__partitions_by_size__partitions_at_top_level(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/A/X": (1,),
                "A/A/Y": (1,),
                "A/B/X": (1,),
                "A/B/Y": (1,),
            },
            size_bytes_limit=2,
            expected_node_paths={
                f"A/A/",
                f"A/B/",
            },
        )

    def test__partition__partitions_by_size__partitions_at_varying_levels(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/A/X": (1,),
                "A/A/Y": (1,),
                "A/B/X": (3,),
                "A/B/Y": (2,),
            },
            size_bytes_limit=4,
            expected_node_paths={
                f"A/A/",
                f"A/B/X",
                f"A/B/Y",
            },
        )

    def test__partition__partitions_by_size__partitions_at_varying_levels_case_2(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/A/X": (1,),
                "A/A/Y": (1,),
                "A/B/A/X": (2,),
                "A/B/A/Y": (2,),
                "A/B/Y": (5,),
            },
            size_bytes_limit=5,
            expected_node_paths={
                f"A/A/",
                f"A/B/A/",
                f"A/B/Y",
            },
        )

    def test__partition__partitions_by_count__partitions_at_varying_levels(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/A/X": (1,),
                "A/A/Y": (1,),
                "A/B/A/X": (2,),
                "A/B/A/Y": (2,),
                "A/B/Y": (5,),
            },
            object_count_limit=2,
            expected_node_paths={
                f"A/A/",
                f"A/B/A/",
                f"A/B/Y",
            },
        )

    def test__partition__partitions_by_size_and_count__partitions_at_varying_levels(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/A/X": (5,),
                "A/A/Y": (1,),
                "A/B/A/X": (2,),
                "A/B/A/Y": (2,),
                "A/B/Y": (1,),
            },
            size_bytes_limit=5,
            object_count_limit=2,
            expected_node_paths={
                f"A/A/X",
                f"A/A/Y",
                f"A/B/A/",
                f"A/B/Y",
            },
        )

    def test__partition__partitions_by_size__no_partitioning_required(self):
        self.assertLocalFileSystem_partition(
            file_stats_map={
                "A/A/X": (1,),
                "A/A/Y": (1,),
                "A/B/X": (3,),
                "A/B/Y": (2,),
                "B/B/Y": (2,),
            },
            size_bytes_limit=10,
            object_count_limit=10,
            expected_node_paths={f""},
        )

    def assertLocalFileSystem_partition(
        self,
        file_stats_map: Mapping[Union[Path, str], Tuple[int]],
        expected_node_paths: Set[str],
        size_bytes_limit: Optional[int] = None,
        object_count_limit: Optional[int] = None,
    ):
        local_path = self.create_local_file_system(file_stats_map=file_stats_map)
        local_root = LocalFileSystem.from_path(str(local_path))
        local_nodes = local_root.partition(
            size_bytes_limit=size_bytes_limit, object_count_limit=object_count_limit
        )
        local_node_paths = {removeprefix(node.path, f"{local_path}/") for node in local_nodes}

        self.assertSetEqual(expected_node_paths, local_node_paths)


@mock_sts
class EFSFileSystemTests(EFSTestsBase):
    def setUp(self) -> None:
        super().setUp()
        detect_mount_points.cache_clear()

    def setUpEFSFileSystem(
        self, name: str, access_point_path: Optional[Union[str, Path]] = None
    ) -> Tuple[Path, EFSPath]:
        mount_point_path = self.tmp_path()
        file_system_id = self.create_file_system()
        if access_point_path is not None:
            mount_point_id = self.create_access_point(
                file_system_id=file_system_id,
                access_point_name=name,
                access_point_path=access_point_path,
            )
        else:
            mount_point_id = file_system_id
        self.set_env_vars(
            *MountPointConfiguration.to_env_vars(mount_point_path, mount_point_id).items()
        )
        return mount_point_path, EFSPath.build(
            resource_id=file_system_id, path=access_point_path or "/"
        )

    def test__from_path__resolves_efs_path(self):
        mount_point_path, efs_path = self.setUpEFSFileSystem("ap", access_point_path="/A/B")
        self.populate_file_system(mount_point_path, {"X": (1,)})
        efs_root = EFSFileSystem.from_path(str(efs_path))
        assert efs_root.path == mount_point_path
        assert efs_root.efs_path == efs_path

    def test__from_path__resolves_local_path(self):
        mount_point_path, efs_path = self.setUpEFSFileSystem("ap", access_point_path="/A/B")
        self.populate_file_system(mount_point_path, {"X": (1,)})
        efs_root = EFSFileSystem.from_path(str(mount_point_path))
        assert efs_root.path == mount_point_path
        assert efs_root.efs_path == efs_path

    def test__partition__partitions_by_size__partitions_to_object_level(self):
        mount_point_path, efs_path = self.setUpEFSFileSystem("ap", access_point_path="/A/B")
        self.populate_file_system(mount_point_path, {"X": (1,), "Y": (1,)})
        efs_root = EFSFileSystem.from_path(efs_path)

        efs_root.from_path(efs_path)
        efs_nodes = efs_root.partition(size_bytes_limit=1)
        efs_node_paths = {node.path for node in efs_nodes}
        assert efs_node_paths == {f"{efs_path}/X", f"{efs_path}/Y"}

    def populate_file_system(
        self, path: Path, file_stats_map: Mapping[Union[Path, str], Tuple[int]]
    ):
        for relative_path, (size,) in file_stats_map.items():
            full_path = path / relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text("0" * size)


# HACK: this is recreating the ObjectSummary class that does not explicitly
#       exist in boto package.
@dataclass
class ObjectSummary:
    last_modified: datetime
    size: int
    bucket_name: str
    key: str


class S3FileSystemTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.BUCKET_NAME = "my-fav-bucket"
        self.KEY_PREFIX = "my-fav/prefix/"
        self.KEY_PREFIX_NO_SEP = "my-fav/prefix"

        self.mock_s3_resource = mock.MagicMock()
        self.mock_get_s3_resource = self.create_patch(
            "aibs_informatics_aws_utils.data_sync.file_system.get_s3_resource"
        )
        self.mock_get_s3_resource.return_value = self.mock_s3_resource

    def mock_bucket(self, s3_paths_stats: Mapping[S3URI, S3PathStats]):
        mock_bucket = mock.MagicMock()
        mock_bucket.objects = mock.MagicMock()

        def object_filter(Prefix: str = None):
            return [
                ObjectSummary(
                    last_modified=s3_path_stats.last_modified,
                    size=s3_path_stats.size_bytes,
                    bucket_name=s3_path.bucket,
                    key=s3_path.key,
                )
                for s3_path, s3_path_stats in s3_paths_stats.items()
            ]

        mock_bucket.objects.filter.side_effect = object_filter
        self.mock_s3_resource.Bucket.return_value = mock_bucket

    def get_s3_path_and_stats(
        self,
        key_suffix: str,
        size: int = 1,
        last_modified: datetime = None,
        key_prefix: str = None,
        bucket_name: str = None,
    ) -> Tuple[S3URI, S3PathStats]:
        return (
            S3URI.build(
                bucket_name=bucket_name or self.BUCKET_NAME,
                key=f"{key_prefix or self.KEY_PREFIX}{key_suffix}",
            ),
            S3PathStats(
                last_modified=last_modified or datetime.now(tz=pytz.UTC),
                size_bytes=size,
                object_count=1,
            ),
        )

    def test__partition__partitions_by_size__partitions_single_object(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "": S3PathStats(last_modified, 5, 1),
            },
            size_bytes_limit=6,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}",
            },
        )

    def test__partition__partitions_by_size__partitions_to_object_level(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/A/X": S3PathStats(last_modified, 5, 1),
                "A/A/Y": S3PathStats(get_current_time(), 5, 1),
                "A/B/X": S3PathStats(last_modified, 5, 1),
                "A/B/Y": S3PathStats(last_modified, 5, 1),
            },
            size_bytes_limit=6,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}A/A/X",
                f"{self.KEY_PREFIX}A/A/Y",
                f"{self.KEY_PREFIX}A/B/X",
                f"{self.KEY_PREFIX}A/B/Y",
            },
        )

    # def test__partition__partitions_by_size__partitions_to_object_level__key_no_sep(self):
    #     s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX_NO_SEP)
    #     last_modified = get_current_time()
    #     self.assertS3Root_partition(
    #         s3_root_uri=s3_root_uri,
    #         key_stats_map={
    #             "/A/A/X": S3PathStats(last_modified, 5, 1),
    #             "A/A/Y": S3PathStats(get_current_time(), 5, 1),
    #             "A/B/X": S3PathStats(last_modified, 5, 1),
    #             "A/B/Y": S3PathStats(last_modified, 5, 1),
    #         },
    #         size_bytes_limit=6,
    #         expected_s3_node_keys={
    #             f"{self.KEY_PREFIX_NO_SEP}/A/A/X",
    #             f"{self.KEY_PREFIX_NO_SEP}A/A/Y",
    #             f"{self.KEY_PREFIX_NO_SEP}A/B/X",
    #             f"{self.KEY_PREFIX_NO_SEP}A/B/Y",
    #         },
    #     )

    def test__partition__partitions_by_size__eats_error_obj_too_large(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/X": S3PathStats(last_modified, 5, 1),
                "A/B/X": S3PathStats(last_modified, 2, 1),
                "A/B/Y": S3PathStats(last_modified, 2, 1),
            },
            size_bytes_limit=4,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}A/X",
                f"{self.KEY_PREFIX}A/B/",
            },
        )

    def test__partition__partitions_by_size__partitions_at_top_level(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/A/X": S3PathStats(last_modified, 1, 1),
                "A/A/Y": S3PathStats(last_modified, 1, 1),
                "A/B/X": S3PathStats(last_modified, 1, 1),
                "A/B/Y": S3PathStats(last_modified, 1, 1),
            },
            size_bytes_limit=2,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}A/A/",
                f"{self.KEY_PREFIX}A/B/",
            },
        )

    def test__partition__partitions_by_size__partitions_at_varying_levels(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/A/X": S3PathStats(last_modified, 1, 1),
                "A/A/Y": S3PathStats(last_modified, 1, 1),
                "A/B/X": S3PathStats(last_modified, 3, 1),
                "A/B/Y": S3PathStats(last_modified, 2, 1),
            },
            size_bytes_limit=4,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}A/A/",
                f"{self.KEY_PREFIX}A/B/X",
                f"{self.KEY_PREFIX}A/B/Y",
            },
        )

    def test__partition__partitions_by_size__partitions_at_varying_levels_case_2(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/A/X": S3PathStats(last_modified, 1, 1),
                "A/A/Y": S3PathStats(last_modified, 1, 1),
                "A/B/A/X": S3PathStats(last_modified, 2, 1),
                "A/B/A/Y": S3PathStats(last_modified, 2, 1),
                "A/B/Y": S3PathStats(last_modified, 5, 1),
            },
            size_bytes_limit=5,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}A/A/",
                f"{self.KEY_PREFIX}A/B/A/",
                f"{self.KEY_PREFIX}A/B/Y",
            },
        )

    def test__partition__partitions_by_count__partitions_at_varying_levels(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/A/X": S3PathStats(last_modified, 1, 1),
                "A/A/Y": S3PathStats(last_modified, 1, 1),
                "A/B/A/X": S3PathStats(last_modified, 2, 1),
                "A/B/A/Y": S3PathStats(last_modified, 2, 1),
                "A/B/Y": S3PathStats(last_modified, 5, 1),
            },
            object_count_limit=2,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}A/A/",
                f"{self.KEY_PREFIX}A/B/A/",
                f"{self.KEY_PREFIX}A/B/Y",
            },
        )

    def test__partition__partitions_by_size_and_count__partitions_at_varying_levels(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/A/X": S3PathStats(last_modified, 5, 1),
                "A/A/Y": S3PathStats(last_modified, 1, 1),
                "A/B/A/X": S3PathStats(last_modified, 2, 1),
                "A/B/A/Y": S3PathStats(last_modified, 2, 1),
                "A/B/Y": S3PathStats(last_modified, 1, 1),
            },
            size_bytes_limit=5,
            object_count_limit=2,
            expected_s3_node_keys={
                f"{self.KEY_PREFIX}A/A/X",
                f"{self.KEY_PREFIX}A/A/Y",
                f"{self.KEY_PREFIX}A/B/A/",
                f"{self.KEY_PREFIX}A/B/Y",
            },
        )

    def test__partition__partitions_by_size__no_partitioning_required(self):
        s3_root_uri = S3URI.build(bucket_name=self.BUCKET_NAME, key=self.KEY_PREFIX)
        last_modified = get_current_time()
        self.assertS3FileSystem_partition(
            s3_root_uri=s3_root_uri,
            key_stats_map={
                "A/A/X": S3PathStats(last_modified, 1, 1),
                "A/A/Y": S3PathStats(last_modified, 1, 1),
                "A/B/X": S3PathStats(last_modified, 3, 1),
                "A/B/Y": S3PathStats(last_modified, 2, 1),
                "B/B/Y": S3PathStats(last_modified, 2, 1),
            },
            size_bytes_limit=10,
            object_count_limit=10,
            expected_s3_node_keys={f"{self.KEY_PREFIX}"},
        )

    def assertS3FileSystem_partition(
        self,
        s3_root_uri: S3URI,
        key_stats_map: Dict[str, S3PathStats],
        expected_s3_node_keys: Set[str],
        size_bytes_limit: Optional[int] = None,
        object_count_limit: Optional[int] = None,
    ):
        s3_paths_stats = dict(
            [
                self.get_s3_path_and_stats(
                    key_suffix=key,
                    size=stats.size_bytes,
                    last_modified=stats.last_modified,
                    bucket_name=s3_root_uri.bucket,
                    key_prefix=s3_root_uri.key,
                )
                for key, stats in key_stats_map.items()
            ]
        )
        self.mock_bucket(s3_paths_stats=s3_paths_stats)

        s3_root = S3FileSystem.from_path(s3_root_uri)
        s3_nodes = s3_root.partition(
            size_bytes_limit=size_bytes_limit, object_count_limit=object_count_limit
        )
        s3_node_keys = {node.path for node in s3_nodes}

        self.assertSetEqual(expected_s3_node_keys, s3_node_keys)
