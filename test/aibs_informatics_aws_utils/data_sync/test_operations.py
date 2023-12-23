from pathlib import Path
from test.aibs_informatics_aws_utils.base import AwsBaseTest
from typing import Union

import moto
from aibs_informatics_core.models.aws.s3 import S3URI
from aibs_informatics_core.utils.os_operations import find_all_paths

from aibs_informatics_aws_utils.data_sync.operations import sync_data
from aibs_informatics_aws_utils.s3 import get_s3_client, get_s3_resource, is_object, list_s3_paths


def any_s3_uri(bucket: str = "bucket", key: str = "key") -> S3URI:
    return S3URI.build(bucket, key)


@moto.mock_s3
class OperationsTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.DEFAULT_REGION)
        self.DEFAULT_BUCKET_NAME = "a-random-bucket"

    def setUpLocalFS(self) -> Path:
        fs = self.tmp_path()
        return fs

    def setUpBucket(self, bucket_name: str = None) -> str:
        bucket_name = bucket_name or self.DEFAULT_BUCKET_NAME
        self.s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": self.DEFAULT_REGION},
        )
        return bucket_name

    def put_object(self, key: str, content: str, bucket_name: str = None, **kwargs) -> S3URI:
        bucket_name = bucket_name or self.DEFAULT_BUCKET_NAME
        self.s3_client.put_object(Bucket=bucket_name, Key=key, Body=content, **kwargs)
        return self.get_s3_path(key=key, bucket_name=bucket_name)

    def get_object(self, key: str, bucket_name: str = None) -> str:
        bucket_name = bucket_name or self.DEFAULT_BUCKET_NAME
        response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read().decode()

    def put_file(self, path: Path, content: str) -> Path:
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(content)
        return path

    def get_file(self, path: Path) -> str:
        return path.read_text()

    @property
    def s3_client(self):
        return get_s3_client(region=self.DEFAULT_REGION)

    @property
    def s3_resource(self):
        return get_s3_resource(region=self.DEFAULT_REGION)

    def get_s3_path(self, key: str, bucket_name: str = None) -> S3URI:
        bucket_name = bucket_name or self.DEFAULT_BUCKET_NAME
        return S3URI.build(bucket_name=bucket_name, key=key)

    def client__list_objects_v2(self, **kwargs):
        if "Bucket" not in kwargs:
            kwargs["Bucket"] = self.DEFAULT_BUCKET_NAME
        return self.s3_client.list_objects_v2(**kwargs)

    def test__sync_data__s3_to_s3__folder__succeeds(self):
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        path1 = self.put_object("source/path/obj1", "hello")
        path2 = self.put_object("source/path/dir1/obj2", "did you hear me")
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__s3_to_s3__file__succeeds(self):
        self.setUpBucket()
        source_path = self.put_object("source/path/obj1", "hello")
        destination_path = self.get_s3_path("destination/path/")
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__s3_to_s3__file__succeeds__source_deleted(self):
        self.setUpBucket()
        source_path = self.put_object("source/path/obj1", "hello")
        destination_path = self.get_s3_path("destination/path/")
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            retain_source_data=False,
        )
        assert self.get_object(destination_path.key) == "hello"
        assert not is_object(source_path)

    def test__sync_data__local_to_local__folder__succeeds(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path / "file1", "hello")
        self.put_file(source_path / "file2", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__local_to_local__file__succeeds(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path, "hello")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__local_to_local__relative_file__succeeds(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path, "hello")
        with self.chdir(fs):
            sync_data(
                source_path=Path("source"),
                destination_path=Path("destination"),
            )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__local_to_local__file__source_deleted(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path, "hello")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            retain_source_data=False,
        )
        assert destination_path.read_text() == "hello"
        assert not source_path.exists()

    def test__sync_data__s3_to_local__folder__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        destination_path = fs / "destination2"

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__s3_to_local__folder__cached_results_mtime_updated(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        destination_path = fs / "destination"

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__s3_to_local__file__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.put_object("source", "hello")
        destination_path = fs / "destination"
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__s3_to_local__file__lock_required__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.put_object("source", "hello")
        destination_path = fs / "destination"
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            require_lock=True,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__s3_to_local__file__source_not_deleted_despite_flag(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.put_object("source", "hello")
        destination_path = fs / "destination"

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            retain_source_data=False,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__local_to_s3__folder__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = fs / "source"
        destination_path = self.get_s3_path("destination/path")
        self.put_file(source_path / "file1", "hello")
        self.put_file(source_path / "file2", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__local_to_s3__file__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = fs / "source"
        destination_path = self.get_s3_path("destination/path")
        self.put_file(source_path, "hello")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__local_to_s3__file__source_deleted(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = fs / "source"
        destination_path = self.get_s3_path("destination/path")
        self.put_file(source_path, "hello")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            retain_source_data=False,
        )
        assert not source_path.exists()

    def assertPathsEqual(
        self, src_path: Union[Path, S3URI], dst_path: Union[Path, S3URI], expected_num_files: int
    ):
        is_src_local = isinstance(src_path, Path)
        is_dst_local = isinstance(dst_path, Path)
        src_paths = find_all_paths(src_path, False) if is_src_local else list_s3_paths(src_path)
        dst_paths = find_all_paths(dst_path, False) if is_dst_local else list_s3_paths(dst_path)

        self.assertEqual(len(src_paths), len(dst_paths), "number of files don't match")
        self.assertEqual(expected_num_files, len(src_paths), "number of files don't match")

        self.assertSetEqual(
            {str(_)[len(str(src_path)) :].lstrip("/") for _ in src_paths},
            {str(_)[len(str(dst_path)) :].lstrip("/") for _ in dst_paths},
        )
