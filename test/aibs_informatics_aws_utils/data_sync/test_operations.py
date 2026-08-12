import sys
from pathlib import Path
from unittest import mock

import moto
from aibs_informatics_core.models.aws.s3 import S3Path
from aibs_informatics_core.models.data_sync import DataSyncFilterConfig, RemoteToLocalConfig
from aibs_informatics_core.utils.os_operations import find_all_paths
from pytest import mark

from aibs_informatics_aws_utils.data_sync.operations import DataSyncOperations, sync_data
from aibs_informatics_aws_utils.s3 import get_s3_client, get_s3_resource, is_object, list_s3_paths
from test.aibs_informatics_aws_utils.base import AwsBaseTest


def any_s3_uri(bucket: str = "bucket", key: str = "key") -> S3Path:
    return S3Path.build(bucket, key)


@moto.mock_aws
class OperationsTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.DEFAULT_REGION)
        self.DEFAULT_BUCKET_NAME = "a-random-bucket"

    def setUpLocalFS(self) -> Path:
        fs = self.tmp_path()
        return fs

    def setUpBucket(self, bucket_name: str | None = None) -> str:
        bucket_name = bucket_name or self.DEFAULT_BUCKET_NAME
        self.s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": self.DEFAULT_REGION},  # type: ignore
        )
        return bucket_name

    def put_object(
        self, key: str, content: str, bucket_name: str | None = None, **kwargs
    ) -> S3Path:
        bucket_name = bucket_name or self.DEFAULT_BUCKET_NAME
        self.s3_client.put_object(Bucket=bucket_name, Key=key, Body=content, **kwargs)
        return self.get_s3_path(key=key, bucket_name=bucket_name)

    def get_object(self, key: str, bucket_name: str | None = None) -> str:
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

    def get_s3_path(self, key: str, bucket_name: str | None = None) -> S3Path:
        bucket_name = bucket_name or self.DEFAULT_BUCKET_NAME
        return S3Path.build(bucket_name=bucket_name, key=key)

    def client__list_objects_v2(self, **kwargs):
        if "Bucket" not in kwargs:
            kwargs["Bucket"] = self.DEFAULT_BUCKET_NAME
        return self.s3_client.list_objects_v2(**kwargs)

    def test__sync_data__s3_to_s3__folder__succeeds(self):
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=True,
        )
        self.assertPathsEqual(source_path, destination_path, 2)
        self.assertEqual(result.files_transferred, 2)
        self.assertEqual(result.bytes_transferred, 20)

    def test__sync_data__s3_to_s3__folder__succeeds__no_detailed_response(self):
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=False,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__s3_to_s3__file__succeeds(self):
        self.setUpBucket()
        source_path = self.put_object("source/path/obj1", "hello")
        destination_path = self.get_s3_path("destination/path/")
        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=True,
        )
        self.assertPathsEqual(source_path, destination_path, 1)
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

    def test__sync_data__s3_to_s3__file__succeeds__no_detailed_response(self):
        self.setUpBucket()
        source_path = self.put_object("source/path/obj1", "hello")
        destination_path = self.get_s3_path("destination/path/")
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=False,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__s3_to_s3__file__succeeds__source_deleted(self):
        self.setUpBucket()
        source_path = self.put_object("source/path/obj1", "hello")
        destination_path = self.get_s3_path("destination/path/")
        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            retain_source_data=False,
            include_detailed_response=True,
        )
        assert self.get_object(destination_path.key) == "hello"
        assert not is_object(source_path)
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

    def test__sync_data__s3_to_s3__file__succeeds__source_deleted__no_detailed_response(self):
        self.setUpBucket()
        source_path = self.put_object("source/path/obj1", "hello")
        destination_path = self.get_s3_path("destination/path/")
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            retain_source_data=False,
            include_detailed_response=False,
        )
        assert self.get_object(destination_path.key) == "hello"
        assert not is_object(source_path)

    def test__sync_data__s3_to_s3__file__does_not_exist(self):
        self.setUpBucket()
        source_path = self.get_s3_path("source")
        destination_path = self.get_s3_path("destination")
        with self.assertRaises(FileNotFoundError):
            sync_data(
                source_path=source_path,
                destination_path=destination_path,
            )
        sync_data(
            source_path=source_path, destination_path=destination_path, fail_if_missing=False
        )
        assert not is_object(destination_path)

    def test__sync_data__local_to_local__folder__succeeds(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path / "file1", "hello")
        self.put_file(source_path / "file2", "did you hear me")

        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=True,
        )
        self.assertPathsEqual(source_path, destination_path, 2)
        self.assertEqual(result.files_transferred, 2)
        self.assertEqual(result.bytes_transferred, 20)

    def test__sync_data__local_to_local__folder__succeeds__no_detailed_response(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path / "file1", "hello")
        self.put_file(source_path / "file2", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=False,
        )
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__local_to_local__file__succeeds(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path, "hello")

        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=True,
        )
        self.assertPathsEqual(source_path, destination_path, 1)
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

    def test__sync_data__local_to_local__file__succeeds__no_detailed_response(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path, "hello")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=False,
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__local_to_local__relative_file__succeeds(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path, "hello")
        with self.chdir(fs):
            result = sync_data(
                source_path=Path("source"),
                destination_path=Path("destination"),
                include_detailed_response=True,
            )
        self.assertPathsEqual(source_path, destination_path, 1)
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

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

    def test__sync_data__local_to_local__file__does_not_exist(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        with self.assertRaises(FileNotFoundError):
            sync_data(
                source_path=source_path,
                destination_path=destination_path,
            )
        sync_data(
            source_path=source_path, destination_path=destination_path, fail_if_missing=False
        )
        assert not destination_path.exists()

    def test__sync_data__s3_to_local__folder__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        destination_path = fs / "destination2"

        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=True,
        )
        self.assertPathsEqual(source_path, destination_path, 2)
        self.assertEqual(result.files_transferred, 2)
        self.assertEqual(result.bytes_transferred, 20)

    def test__sync_data__s3_to_local__folder__cached_results_mtime_updated(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        destination_path = fs / "destination"

        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=True,
        )
        self.assertPathsEqual(source_path, destination_path, 2)
        self.assertEqual(result.files_transferred, 2)
        self.assertEqual(result.bytes_transferred, 20)

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
        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            include_detailed_response=True,
        )
        self.assertPathsEqual(source_path, destination_path, 1)
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

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

    def test__sync_data__s3_to_local__file__does_not_exist(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.get_s3_path("source")
        destination_path = fs / "destination"
        with self.assertRaises(FileNotFoundError):
            sync_data(
                source_path=source_path,
                destination_path=destination_path,
            )
        sync_data(
            source_path=source_path, destination_path=destination_path, fail_if_missing=False
        )
        assert not destination_path.exists()

    @mark.xfail(
        sys.platform == "darwin",
        reason="Test does not run on macOS (tmp dir is /private which is not accessible)",
    )
    def test__sync_data__s3_to_local__file__auto_custom_tmp_dir__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.put_object("source", "hello")
        destination_path = fs / "destination"
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            remote_to_local_config=RemoteToLocalConfig(use_custom_tmp_dir=True),
        )
        self.assertPathsEqual(source_path, destination_path, 1)

    def test__sync_data__s3_to_local__file__specified_custom_tmp_dir__succeeds(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.put_object("source", "hello")
        destination_path = fs / "destination"
        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            remote_to_local_config=RemoteToLocalConfig(
                use_custom_tmp_dir=True,
                custom_tmp_dir=fs,
            ),
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

    def test__sync_data__local_to_s3__file__does_not_exist(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = fs / "source"
        destination_path = self.get_s3_path("destination")
        with self.assertRaises(FileNotFoundError):
            sync_data(
                source_path=source_path,
                destination_path=destination_path,
            )
        sync_data(
            source_path=source_path, destination_path=destination_path, fail_if_missing=False
        )
        assert not is_object(destination_path)

    # -----------------------------------------------------------------------
    # include/exclude filtering (OCSDV-452)
    # -----------------------------------------------------------------------

    def test__sync_data__s3_to_s3__filtered__moves_only_matching_objects(self):
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/sampleA/reads.bam", "hello")
        self.put_object("source/path/sampleA/notes.txt", "did you hear me")

        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r".*\.bam"),
            include_detailed_response=True,
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)},
            {"destination/path/sampleA/reads.bam"},
        )
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

    def test__sync_data__s3_to_s3__filtered__exclude_wins_over_include(self):
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/a.bam", "hello")
        self.put_object("source/path/b.bam", "hello")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r".*\.bam", exclude=r"b\.bam"),
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)}, {"destination/path/a.bam"}
        )

    def test__sync_data__s3_to_s3__filtered__filter_root_anchors_patterns(self):
        """A sub-request rooted at a sub-prefix honors patterns from the original root."""
        self.setUpBucket()
        root_path = self.get_s3_path("source/path/")
        source_path = self.get_s3_path("source/path/sampleA/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/sampleA/reads.bam", "hello")
        self.put_object("source/path/sampleA/notes.txt", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r"sampleA/.*\.bam"),
            filter_root=str(root_path),
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)}, {"destination/path/reads.bam"}
        )

    def test__sync_data__s3_to_local__filtered__downloads_only_matching_objects(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = fs / "destination"
        self.put_object("source/path/sampleA/reads.bam", "hello")
        self.put_object("source/path/sampleA/notes.txt", "did you hear me")

        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r".*\.bam"),
            include_detailed_response=True,
        )

        self.assertSetEqual(
            {
                str(p)[len(str(destination_path)) :].lstrip("/")
                for p in find_all_paths(destination_path, False)
            },
            {"sampleA/reads.bam"},
        )
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

    def test__sync_data__s3_to_local__filtered__filter_root_anchors_patterns(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        root_path = self.get_s3_path("source/path/")
        source_path = self.get_s3_path("source/path/sampleA/")
        destination_path = fs / "destination"
        self.put_object("source/path/sampleA/reads.bam", "hello")
        self.put_object("source/path/sampleA/notes.txt", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r"sampleA/.*\.bam"),
            filter_root=str(root_path),
        )

        self.assertSetEqual(
            {Path(p).name for p in find_all_paths(destination_path, False)}, {"reads.bam"}
        )

    def test__sync_data__local_to_s3__filtered__uploads_only_matching_files(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = fs / "source"
        destination_path = self.get_s3_path("destination/path/")
        self.put_file(source_path / "sampleA" / "reads.bam", "hello")
        self.put_file(source_path / "sampleA" / "notes.txt", "did you hear me")

        result = sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r".*\.bam"),
            include_detailed_response=True,
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)},
            {"destination/path/sampleA/reads.bam"},
        )
        # Regression: these were computed over the UNFILTERED source, and so
        # reported 2 files / 20 bytes for a sync that moved 1 file / 5 bytes.
        self.assertEqual(result.files_transferred, 1)
        self.assertEqual(result.bytes_transferred, 5)

    def test__sync_data__local_to_s3__filtered__patterns_are_relative_to_source(self):
        """`find_paths` matched the full absolute path, so this pattern matched nothing."""
        fs = self.setUpLocalFS()
        self.setUpBucket()
        source_path = fs / "source"
        destination_path = self.get_s3_path("destination/path/")
        self.put_file(source_path / "sample" / "reads.bam", "hello")
        self.put_file(source_path / "other" / "reads.bam", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r"sample/.*"),
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)},
            {"destination/path/sample/reads.bam"},
        )

    def test__sync_data__local_to_s3__filtered__filter_root_anchors_patterns(self):
        fs = self.setUpLocalFS()
        self.setUpBucket()
        root_path = fs / "source"
        source_path = root_path / "sampleA"
        destination_path = self.get_s3_path("destination/path/")
        self.put_file(source_path / "reads.bam", "hello")
        self.put_file(source_path / "notes.txt", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r"sampleA/.*\.bam"),
            filter_root=str(root_path),
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)}, {"destination/path/reads.bam"}
        )

    def test__sync_data__delete__false__retains_unmatched_destination_files(self):
        """`delete` is the gate on the filtered-sync-deletes-extra-files hazard."""
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/a.bam", "hello")
        self.put_object("destination/path/stale.txt", "i was here first")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            delete=False,
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)},
            {"destination/path/a.bam", "destination/path/stale.txt"},
        )

    def test__sync_data__delete__true__removes_filtered_out_destination_files(self):
        """Documented hazard: a filtered sync with delete=True mirrors, so it deletes."""
        self.setUpBucket()
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/a.bam", "hello")
        self.put_object("source/path/b.txt", "did you hear me")
        # An earlier unfiltered copy already sitting at the destination.
        self.put_object("destination/path/b.txt", "did you hear me")

        sync_data(
            source_path=source_path,
            destination_path=destination_path,
            filter_config=DataSyncFilterConfig(include=r".*\.bam"),
            delete=True,
        )

        self.assertSetEqual(
            {p.key for p in list_s3_paths(destination_path)}, {"destination/path/a.bam"}
        )

    def test__sync_data__local_to_local__filtered__warns_and_copies_everything(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path / "reads.bam", "hello")
        self.put_file(source_path / "notes.txt", "did you hear me")

        with self.assertLogs(
            "aibs_informatics_aws_utils.data_sync.operations", level="WARNING"
        ) as logs:
            sync_data(
                source_path=source_path,
                destination_path=destination_path,
                filter_config=DataSyncFilterConfig(include=r".*\.bam"),
            )

        self.assertTrue(
            any("NOT supported for local -> local" in message for message in logs.output),
            f"expected an unsupported-filter warning, got: {logs.output}",
        )
        # Deliberately unimplemented: the full source is copied regardless.
        self.assertPathsEqual(source_path, destination_path, 2)

    def test__sync_data__local_to_local__no_filters__does_not_warn(self):
        fs = self.setUpLocalFS()
        source_path = fs / "source"
        destination_path = fs / "destination"
        self.put_file(source_path / "reads.bam", "hello")

        with mock.patch.object(DataSyncOperations, "logger") as mock_logger:
            sync_data(source_path=source_path, destination_path=destination_path)

        self.assertFalse(
            any(
                "NOT supported for local -> local" in str(call)
                for call in mock_logger.warning.call_args_list
            )
        )

    def assertPathsEqual(
        self, src_path: Path | S3Path, dst_path: Path | S3Path, expected_num_files: int
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
