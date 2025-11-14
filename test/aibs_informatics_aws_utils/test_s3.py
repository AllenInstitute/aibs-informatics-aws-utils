import re
from pathlib import Path
from time import sleep

import moto
import requests
from aibs_informatics_core.models.aws.s3 import (
    S3URI,
    S3CopyRequest,
    S3DownloadRequest,
    S3StorageClass,
    S3UploadRequest,
)
from aibs_informatics_core.utils.os_operations import find_all_paths
from aibs_informatics_test_resources import does_not_raise
from boto3.s3.transfer import TransferConfig
from pytest import fixture, mark, param, raises

from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_aws_utils.s3 import (
    AWS_S3_DEFAULT_CHUNK_SIZE_BYTES,
    MB,
    PresignedUrlAction,
    check_paths_in_sync,
    copy_s3_object,
    delete_s3_path,
    determine_multipart_attributes,
    download_s3_path,
    download_to_json,
    download_to_json_object,
    generate_presigned_urls,
    generate_transfer_request,
    get_object,
    get_s3_client,
    get_s3_path_collection_stats,
    get_s3_path_stats,
    get_s3_resource,
    is_folder,
    is_folder_placeholder_object,
    is_object,
    is_object_prefix,
    list_s3_paths,
    move_s3_path,
    process_transfer_requests,
    should_sync,
    sync_paths,
    update_s3_storage_class,
    upload_file,
    upload_json,
    upload_path,
    upload_scratch_file,
)
from test.aibs_informatics_aws_utils.base import AwsBaseTest


def any_s3_uri(key: str = "key", bucket: str = "bucket") -> S3URI:
    return S3URI.build(bucket, key)


@moto.mock_aws
class S3Tests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.DEFAULT_REGION)
        self.DEFAULT_BUCKET_NAME = "a-random-bucket"
        self.setUpBucket(self.DEFAULT_BUCKET_NAME)

    def setUpBucket(self, bucket_name: str = None):
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

    def test__get_presigned_urls__default_generates_READ_ONLY_urls(self):
        ## Setup
        s3_path_a = self.get_s3_path("path/to/object_a")
        s3_path_b = self.get_s3_path("path/to/object_b")
        s3_path_c = self.get_s3_path("path/to/object/c")

        contents_a = "Hello, it's me"
        contents_b = "I was wondering if after all these years you'd like to meet"
        contents_c = "To go over everything"

        self.put_object(s3_path_a.key, contents_a)
        self.put_object(s3_path_b.key, contents_b)
        self.put_object(s3_path_c.key, contents_c)

        ## Act
        s3_presigned_urls = generate_presigned_urls(
            s3_paths=[s3_path_a, s3_path_b], region=self.DEFAULT_REGION
        )

        ## Assertions
        # Assert exactly same number of urls returned for s3 paths provided
        self.assertEqual(len(s3_presigned_urls), 2)

        url_a, url_b = s3_presigned_urls

        response_a = requests.get(url_a)
        response_b = requests.get(url_b)

        # For each URL, assert content was successfully fetched.
        self.assertTrue(response_a.ok)
        self.assertTrue(response_b.ok)

        self.assertEqual(response_a.content.decode(), contents_a)
        self.assertEqual(response_b.content.decode(), contents_b)

        # Finally assert that content cannot be written to urls.
        # TODO: moto does not create entirely correct URLs so this PUT incorrectly passes
        # put_response_a = requests.put(url_a, data=contents_c)
        # response_a_again = requests.get(url_a)
        # self.assertEqual(response_a_again.content.decode(), contents_a)
        # self.assertFalse(put_response_a.ok)

    def test__get_presigned_urls__generates_WRITE_ONLY_urls(self):
        ## Setup
        s3_path_a = self.get_s3_path("path/to/object_a")
        s3_path_b = self.get_s3_path("path/to/object_b")

        contents_a = "Hello, it's me"
        contents_b = "I was wondering if after all these years you'd like to meet"
        # put 1/2 objects to test both prev existing object and no prev object
        self.put_object(s3_path_a.key, contents_b)

        ## Act
        s3_presigned_urls = generate_presigned_urls(
            s3_paths=[s3_path_a, s3_path_b],
            action=PresignedUrlAction.WRITE,
            region=self.DEFAULT_REGION,
        )

        ## Assertions
        # Assert exactly same number of urls returned for s3 paths provided
        self.assertEqual(len(s3_presigned_urls), 2)

        url_a, url_b = s3_presigned_urls

        # Before putting data, check the existing value of s3_path_a:
        existing_content_a = self.get_object(key=s3_path_a.key, bucket_name=s3_path_a.bucket)
        self.assertEqual(existing_content_a, contents_b)

        # put stuff
        response_a = requests.put(url_a, data=contents_a)
        response_b = requests.put(url_b, data=contents_b)

        self.assertTrue(response_a.ok)
        self.assertTrue(response_b.ok)

        self.assertEqual(self.get_object(s3_path_a.key, s3_path_a.bucket), contents_a)
        self.assertEqual(self.get_object(s3_path_b.key, s3_path_b.bucket), contents_b)

    def test__list_s3_paths__all_cases(self):
        ## Setup
        s3_path = self.get_s3_path("path/to/object")
        s3_path_a = self.get_s3_path("path/to/object_a")
        s3_path_b = self.get_s3_path("path/to/object-b")
        s3_path_c = self.get_s3_path("path/to/object/a")
        s3_path_d = self.get_s3_path("path/to/another/object_d")

        contents_a = "Hello, it's me"
        contents_b = "I was wondering if after all these years you'd like to meet"
        contents_c = "To go over everything"
        contents_d = "They say that time's supposed to heal ya, but I ain't done much healing"

        self.put_object(s3_path_a.key, contents_a)
        self.put_object(s3_path_b.key, contents_b)
        self.put_object(s3_path_c.key, contents_c)
        self.put_object(s3_path_d.key, contents_d)

        # Test (no filters)
        s3_paths_no_filters = list_s3_paths(s3_path)
        self.assertListEqual(
            sorted(s3_paths_no_filters), sorted([s3_path_a, s3_path_b, s3_path_c])
        )

        matching_patterns = [re.compile(".*_.*")]
        missing_patterns = [re.compile("this_wont_match_anything")]
        patterns = [*matching_patterns, *missing_patterns]
        # Test (with include filters)
        s3_paths_include_filters = list_s3_paths(s3_path, include=patterns)
        self.assertListEqual(sorted(s3_paths_include_filters), sorted([s3_path_a]))
        s3_paths_include_filters = list_s3_paths(s3_path, include=matching_patterns)
        self.assertListEqual(sorted(s3_paths_include_filters), sorted([s3_path_a]))
        s3_paths_include_filters = list_s3_paths(s3_path, include=missing_patterns)
        self.assertListEqual(sorted(s3_paths_include_filters), sorted([]))

        # Test (with exclude filters)
        s3_paths_exclude_filters = list_s3_paths(s3_path, exclude=patterns)
        self.assertListEqual(sorted(s3_paths_exclude_filters), sorted([s3_path_b, s3_path_c]))
        s3_paths_exclude_filters = list_s3_paths(s3_path, exclude=missing_patterns)
        self.assertListEqual(
            sorted(s3_paths_exclude_filters), sorted([s3_path_a, s3_path_b, s3_path_c])
        )

        # Test (with include and exclude filters)
        # 1. include provided - no match; exclude provided - no match
        s3_paths_all_filters_1 = list_s3_paths(
            s3_path, include=missing_patterns, exclude=missing_patterns
        )
        self.assertListEqual(sorted(s3_paths_all_filters_1), [])

        # 2. include provided - no match; exclude provided - match
        s3_paths_all_filters_2 = list_s3_paths(
            s3_path, include=missing_patterns, exclude=matching_patterns
        )
        self.assertListEqual(sorted(s3_paths_all_filters_2), [])

        # 3. include provided - match; exclude provided - no match
        s3_paths_all_filters_3 = list_s3_paths(
            s3_path, include=[re.compile(r"_a")], exclude=missing_patterns
        )
        self.assertListEqual(sorted(s3_paths_all_filters_3), [s3_path_a])

        # 4. include provided - match; exclude provided - match
        s3_paths_all_filters_4 = list_s3_paths(
            s3_path, include=[re.compile(".+")], exclude=[re.compile(r"_a")]
        )
        self.assertListEqual(sorted(s3_paths_all_filters_4), sorted([s3_path_b, s3_path_c]))

    def test__is_folder__is_object__is_object_prefix__is_folder_placeholder_object__work(self):
        ## Setup

        content = "Hello, it's me"
        s3_path_to_object = self.put_object("path/to/object", content)

        s3_path_to_object_dash = self.get_s3_path("path/to/object-")
        s3_path_to_object_dash_a = self.put_object("path/to/object-a", content)
        s3_path_to_object_dash_b = self.put_object("path/to/object-b", content)

        s3_path_to_object_slash = self.get_s3_path("path/to/object/")
        s3_path_to_object_slash_a = self.put_object("path/to/object/a", content)
        s3_path_to_object_slash_b = self.put_object("path/to/object/b", content)

        s3_path_to_folder = self.get_s3_path("path/to/folder")
        s3_path_to_folder_slash_a = self.put_object("path/to/folder/a", content)
        s3_path_to_folder_slash_b = self.put_object("path/to/folder/b", content)

        s3_path_to_folder_object = self.put_object("path/to/folderA/", content)
        s3_path_to_folder_object_a = self.put_object("path/to/folderA/a", content)
        s3_path_to_folder_placeholder_object = self.put_object("path/to/folderB/", "")

        s3_path_to_non_existent = self.get_s3_path("path/to/objectX")

        # fmt: off
        # Expected values:
        #  is_folder_placeholder_object --------------------------------------|
        #  is_folder -------------------------------------------------|       |
        #  is_object_prefix ----------------------------------|       |       |
        #  is_object ---------------------------------|       |       |       |
        assertions = [
            (s3_path_to_object,                     (True,   True,   True,  False)),
            (s3_path_to_object_dash,                (False,  True,   False, False)),
            (s3_path_to_object_dash_a,              (True,   False,  False, False)),
            (s3_path_to_object_dash_b,              (True,   False,  False, False)),
            (s3_path_to_object_slash,               (False,   True,  True,  False)),
            (s3_path_to_object_slash_a,             (True,   False,  False, False)),
            (s3_path_to_object_slash_b,             (True,   False,  False, False)),
            (s3_path_to_folder,                     (False,  True,   True,  False)),
            (s3_path_to_folder_slash_a,             (True,   False,  False, False)),
            (s3_path_to_folder_slash_b,             (True,   False,  False, False)),
            (s3_path_to_non_existent,               (False,  False,  False, False)),
            (s3_path_to_folder_object,              (True,   True,   True,  False)),
            (s3_path_to_folder_object_a,            (True,   False,  False, False)),
            (s3_path_to_folder_placeholder_object,  (True,   False,  False, True)),
        ]
        # fmt: on

        for p, expected in assertions:
            actual_is_object = is_object(p)
            actual_is_object_prefix = is_object_prefix(p)
            actual_is_folder = is_folder(p)
            actual_is_folder_placeholder_object = is_folder_placeholder_object(p)
            actual = (
                actual_is_object,
                actual_is_object_prefix,
                actual_is_folder,
                actual_is_folder_placeholder_object,
            )

            self.assertEqual(
                expected,
                actual,
                (
                    f"Assertions for {p} failed for (is_object, is_object_prefix, is_folder):"
                    f"(Expected) {expected} != {actual} (Actual)"
                ),
            )

    def test__upload_json__download_to_json__works(self):
        content = [{"1": 1}, "asdf"]
        s3_path = self.get_s3_path("content.json")
        upload_json(content=content, s3_path=s3_path)
        self.assertTrue(is_object(s3_path))
        new_content = download_to_json(s3_path)
        self.assertEqual(content, new_content)

    def test__upload_json__download_to_json_object__works(self):
        content = {"a": 1}
        s3_path = self.get_s3_path("content.json")
        upload_json(content=content, s3_path=s3_path)
        self.assertTrue(is_object(s3_path))
        new_content = download_to_json_object(s3_path)
        self.assertEqual(content, new_content)

    def test__upload_json__download_to_json_object__fails_for_non_json_object(self):
        content = [{"a": 1}]
        s3_path = self.get_s3_path("conetnt.json")
        upload_json(content=content, s3_path=s3_path)
        self.assertTrue(is_object(s3_path))
        with self.assertRaises(Exception):
            download_to_json_object(s3_path)

    def test__download_to_json__fails_for_invalid_json(self):
        content = "asdf"
        s3_path = self.get_s3_path("content.json")
        self.put_object(s3_path.key, content)
        with self.assertRaises(AWSError):
            download_to_json(s3_path)

    def test__upload_path__does_not_upload_file_if_already_exists(self):
        root = self.tmp_path()
        previous_file = root / "previous"
        previous_file.write_text("hello")

        s3_path = self.get_s3_path("path/to/file")
        upload_path(previous_file, s3_path, force=True)
        self.assertTrue(is_object(s3_path))
        s3_object = get_object(s3_path)
        upload_path(previous_file, s3_path, force=False)
        s3_object2 = get_object(s3_path)
        self.assertEqual(s3_object.last_modified, s3_object2.last_modified)

        # Now update previous_file locally
        sleep(0.5)
        previous_file.write_text("hello2")
        upload_path(previous_file, s3_path, force=False)
        s3_object3 = get_object(s3_path)
        self.assertNotEqual(s3_object.e_tag, s3_object3.e_tag)

    def test__upload_path__download_s3_path__handles_folder(self):
        orig_root = self.tmp_path()
        orig_file1 = orig_root / "file1.txt"
        orig_file2 = orig_root / "dir1" / "file2.txt"
        orig_file1.touch()
        orig_file2.parent.mkdir(parents=True, exist_ok=True)
        orig_file2.touch(exist_ok=True)

        orig_files = {orig_file1.relative_to(orig_root), orig_file2.relative_to(orig_root)}

        new_root = self.tmp_path()
        existing_folder = new_root / "existing"
        existing_folder.mkdir()
        existing_non_empty_folder = new_root / "existing_non_empty"
        existing_non_empty_folder.mkdir()
        (existing_non_empty_folder / "file").touch()
        non_existing_folder = new_root / "non-existing"

        s3_path = self.get_s3_path("path/to/folder/")
        upload_path(orig_root, s3_path)
        self.assertTrue(is_folder(s3_path))
        # assert fails when folder exists and not allowed to exist
        with self.assertRaises(Exception):
            download_s3_path(s3_path, existing_non_empty_folder, exist_ok=False)
        # assert succeeds when folder exists and not allowed to exist but is empty
        download_s3_path(s3_path, existing_folder, exist_ok=False)
        # assert succeeds when folder exists and allowed to exist
        download_s3_path(s3_path, existing_folder, exist_ok=True)
        # assert succeeds when folder does not exist and not allowed to exist
        download_s3_path(s3_path, non_existing_folder, exist_ok=False)
        orig_files = {Path(_).relative_to(orig_root) for _ in find_all_paths(orig_root, True)}
        new_files = {
            Path(_).relative_to(existing_folder) for _ in find_all_paths(existing_folder, True)
        }
        self.assertSetEqual(orig_files, new_files)

    def test__upload_path__download_s3_path__handles_file(self):
        root = self.tmp_path()
        previous_file = root / "previous"
        previous_file.write_text("hello")
        existing_file = root / "existing"
        existing_file.write_text("bye")
        non_existing_file = root / "non-existing"

        s3_path = self.get_s3_path("path/to/file")
        upload_path(previous_file, s3_path)
        self.assertTrue(is_object(s3_path))
        with self.assertRaises(Exception):
            download_s3_path(s3_path, existing_file, force=False, exist_ok=False)
        download_s3_path(s3_path, existing_file, force=False, exist_ok=True)
        download_s3_path(s3_path, non_existing_file, exist_ok=False)
        self.assertEqual(existing_file.read_text(), previous_file.read_text())
        self.assertEqual(non_existing_file.read_text(), previous_file.read_text())

    def test__upload_path__handles_extra_args(self):
        root = self.tmp_path()
        previous_file = root / "previous"
        previous_file.write_text("hello")

        s3_path = self.get_s3_path("path/to/file")
        upload_path(previous_file, s3_path)
        self.assertTrue(is_object(s3_path))
        s3_object = get_object(s3_path)
        self.assertEqual(s3_object.expiration, None)

        upload_path(
            previous_file,
            s3_path,
            force=False,
            extra_args={
                "Tagging": "time-to-live=1",
                "Metadata": {"foo": "bar"},
            },
        )
        s3_object = get_object(s3_path)
        self.assertEqual(s3_object.metadata, {"foo": "bar"})

    def test__upload_path__download_s3_path__fails_for_non_existent_paths(self):
        root = self.tmp_path()
        file = root / "file"

        s3_path = self.get_s3_path("path/to/file")
        with self.assertRaises(ValueError):
            upload_path(file, s3_path)
        with self.assertRaises(Exception):
            download_s3_path(s3_path, file)

    def test__upload_scratch_file__works(self):
        s3_path = self.get_s3_path("path/to/file")
        local_path = self.tmp_path() / "file"
        local_path.write_text("hello")
        upload_scratch_file(local_path, s3_path)
        self.assertTrue(is_object(s3_path))
        response = self.s3_client.get_object_tagging(Bucket=s3_path.bucket, Key=s3_path.key)
        tags = response["TagSet"]
        self.assertEqual(len(tags), 1)
        self.assertDictEqual(tags[0], {"Key": "time-to-live", "Value": "scratch"})

    def test__download_s3_path__handles_existing_dir_for_file(self):
        root = self.tmp_path()
        existing_empty_dir = root / "empty_dir"
        existing_empty_dir.mkdir(exist_ok=True, parents=True)
        existing_non_empty_dir = root / "existing_dir"
        existing_non_empty_dir.mkdir(exist_ok=True, parents=True)

        existing_file = existing_non_empty_dir / "existing"
        existing_file.write_text("bye")

        s3_path = self.put_object("path/to/file", "hello")
        self.assertTrue(is_object(s3_path))
        download_s3_path(s3_path, existing_file, exist_ok=True)
        download_s3_path(s3_path, existing_empty_dir, exist_ok=True)
        with self.assertRaises(Exception):
            download_s3_path(s3_path, existing_non_empty_dir, exist_ok=True)
        self.assertEqual(existing_file.read_text(), "hello")
        self.assertEqual(existing_empty_dir.read_text(), "hello")

    def test__download_s3_path__handles_trailing_slash_objects(self):
        s3_empty = self.get_s3_path("empty/")
        s3_empty_prefix = self.get_s3_path("empty_prefix/")
        s3_not_empty = self.get_s3_path("not_empty/")
        s3_not_empty_prefix = self.get_s3_path("not_empty_prefix/")

        self.put_object(s3_empty.key, "")
        self.put_object(s3_empty_prefix.key, "")
        self.put_object(s3_empty_prefix.key + "file", "")
        self.put_object(s3_not_empty.key, "asdf")
        self.put_object(s3_not_empty_prefix.key, "asdf")
        self.put_object(s3_not_empty_prefix.key + "file", "asdf")

        local_path = self.tmp_path()
        download_s3_path(s3_empty, local_path / "empty")
        download_s3_path(s3_empty_prefix, local_path / "empty_prefix")
        download_s3_path(s3_not_empty, local_path / "not_empty")

        with self.assertRaises(ValueError):
            download_s3_path(s3_not_empty_prefix, local_path / "not_empty_prefix")

    def test__download_s3_path__handles_nested_trailing_slash_objects(self):
        s3_empty_prefix = self.get_s3_path("empty_prefix/")
        s3_not_empty_prefix = self.get_s3_path("not_empty_prefix/")

        self.put_object(s3_empty_prefix.key + "dir/", "")
        self.put_object(s3_empty_prefix.key + "dir/file", "")
        self.put_object(s3_not_empty_prefix.key + "dir/", "asdf")
        self.put_object(s3_not_empty_prefix.key + "dir/file", "asdf")

        local_path = self.tmp_path()
        download_s3_path(s3_empty_prefix, local_path / "empty_prefix")

        with self.assertRaises(ValueError):
            download_s3_path(s3_not_empty_prefix, local_path / "not_empty_prefix")

    def test__copy_s3_object__handles_same_path(self):
        s3_path = self.put_object("path/to/file", "hello")
        copy_s3_object(s3_path, s3_path)
        self.assertTrue(is_object(s3_path))

    def test__copy_s3_object__handles_same_path_with_new_metadata(self):
        s3_path = self.put_object("path/to/file", "hello")

        copy_s3_object(
            s3_path,
            s3_path,
            extra_args={
                "Tagging": "time-to-live=1",
                "Metadata": {"foo": "bar"},
            },
        )
        self.assertEqual(get_object(s3_path).metadata, {"foo": "bar"})

    def test__get_s3_path_stats__handles_file(self):
        root = self.tmp_path()
        local_path = root / "file"
        local_path.write_text("abc")
        s3_path = self.get_s3_path("path/to/file")
        upload_file(local_path, s3_path)
        s3_path_stats = get_s3_path_stats(s3_path)
        self.assertEqual(s3_path_stats.size_bytes, 3)
        self.assertEqual(s3_path_stats.object_count, 1)

    def test__get_s3_path_collection_stats__handles_file(self):
        root = self.tmp_path()
        local_path = root / "file"
        local_path.write_text("abc")
        s3_path = self.get_s3_path("path/to/file")
        upload_file(local_path, s3_path)
        s3_path_stats_collection = get_s3_path_collection_stats(s3_path)
        assert len(s3_path_stats_collection) == 1
        assert s3_path in s3_path_stats_collection
        s3_path_stats = s3_path_stats_collection[s3_path]
        self.assertEqual(s3_path_stats.size_bytes, 3)
        self.assertEqual(s3_path_stats.object_count, 1)

    def test__get_s3_path_stats__handles_folder(self):
        root = self.tmp_path()
        local_file1 = root / "file1"
        local_file1.write_text("abc")
        local_file2 = root / "dir1" / "file2"
        local_file2.parent.mkdir()
        local_file2.write_text("xyz")

        s3_path = self.get_s3_path("path/to/file")
        upload_path(root, s3_path)
        s3_path_stats = get_s3_path_stats(s3_path)
        self.assertEqual(s3_path_stats.size_bytes, 6)
        self.assertEqual(s3_path_stats.object_count, 2)

    def test__get_s3_path_stats__raises_error_for_missing_bucket(self):
        s3_path = self.get_s3_path("path/to/file")
        with self.assertRaises(Exception):
            get_s3_path_stats(s3_path)

    def test__sync_paths__syncs_folders(self):
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        path1 = self.put_object("source/path/obj1", "hello")
        path2 = self.put_object("source/path/dir1/obj2", "did you hear me")
        source_paths = [path1, path2]
        sync_paths(source_path=source_path, destination_path=destination_path)
        destination_paths = list_s3_paths(destination_path)

        self.assertSetEqual(
            {_[len(source_path) :] for _ in source_paths},
            {_[len(destination_path) :] for _ in destination_paths},
        )

    def test__sync_paths__syncs_folders__deletes_paths_not_in_source(self):
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        path1 = self.put_object("source/path/obj1", "hello")
        path2 = self.put_object("source/path/dir1/obj2", "did you hear me")
        # This should be deleted
        self.put_object("destination/path/obj0", "hello")
        source_paths = [path1, path2]
        sync_paths(source_path=source_path, destination_path=destination_path, delete=True)
        destination_paths = list_s3_paths(destination_path)

        self.assertSetEqual(
            {_[len(source_path) :] for _ in source_paths},
            {_[len(destination_path) :] for _ in destination_paths},
        )

    def test__sync_paths__syncs_path_with_object_and_folder__but_not_object_with_prefix(self):
        source_path = self.get_s3_path("source/path")
        destination_path = self.get_s3_path("destination/path")
        path1 = self.put_object("source/path", "hello")
        path2 = self.put_object("source/path/obj1", "hello again")
        self.put_object("source/path_obj", "did you hear me")
        # This should be deleted
        source_paths = [path1, path2]
        sync_paths(source_path=source_path, destination_path=destination_path)
        destination_paths = list_s3_paths(destination_path)

        self.assertSetEqual(
            {_[len(source_path) :] for _ in source_paths},
            {_[len(destination_path) :] for _ in destination_paths},
        )

    def test__sync_paths__fails_for_invalid_source_path_prefix(self):
        source_path = self.put_object("source/path", "hello")
        destination_path = self.get_s3_path("destination/path")
        with self.assertRaises(ValueError):
            sync_paths(
                source_path=source_path,
                destination_path=destination_path,
                source_path_prefix="asdf",
            )

    def test__sync_paths__syncs_file(self):
        source_path = self.put_object("source/path", "hello")
        destination_path = self.get_s3_path("destination/path")
        sync_paths(source_path=source_path, destination_path=destination_path)
        self.assertTrue(is_object(destination_path))
        self.assertEqual(
            get_object(source_path).get()["Body"].readlines(),
            get_object(destination_path).get()["Body"].readlines(),
        )

    def test__process_transfer_requests__works_around_the_horn(self):
        local_path = self.tmp_path() / "file"
        local_path.write_text("hello")

        s3_path = self.get_s3_path("path")
        another_s3_path = self.get_s3_path("path2")
        another_local_path = self.tmp_path() / "file2"
        upload_request = S3UploadRequest(
            source_path=local_path,
            destination_path=s3_path,
        )

        copy_request = S3CopyRequest(
            source_path=s3_path,
            destination_path=another_s3_path,
        )
        download_request = S3DownloadRequest(
            source_path=another_s3_path,
            destination_path=another_local_path,
        )
        responses = process_transfer_requests(
            upload_request,
            copy_request,
            download_request,
        )
        self.assertEqual(len(responses), 3)
        self.assertEqual(responses[0].request, upload_request)
        self.assertEqual(responses[1].request, copy_request)
        self.assertEqual(responses[2].request, download_request)

    def test__process_transfer_requests__handles_errors(self):
        s3_path = self.get_s3_path("path")
        another_s3_path = self.get_s3_path("path2")
        request = S3CopyRequest(
            source_path=s3_path,
            destination_path=another_s3_path,
        )
        responses = process_transfer_requests(request, suppress_errors=True)
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0].request, request)
        self.assertEqual(responses[0].failed, True)

        with self.assertRaises(Exception):
            process_transfer_requests(request, suppress_errors=False)

    def test__move_s3_path__handles_folder(self):
        source_path = self.get_s3_path("source/path/")
        destination_path = self.get_s3_path("destination/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        move_s3_path(source_path=source_path, destination_path=destination_path)
        self.assertEqual(0, len(list_s3_paths(source_path)))
        self.assertEqual(2, len(list_s3_paths(destination_path)))

    def test__move_s3_path__handles_prefix(self):
        source_path = self.get_s3_path("source")
        destination_path = self.get_s3_path("destination")
        path1 = self.put_object("source/obj.txt", "hello")
        path2 = self.put_object("source/obj.txt.metadata", "metadata")
        move_s3_path(source_path=path1, destination_path=destination_path)
        self.assertEqual(1, len(list_s3_paths(source_path)))
        self.assertFalse(is_object(path1))
        self.assertTrue(is_object(path2))
        self.assertTrue(is_object(destination_path))

    def test__delete_s3_path__handles_non_existent_object(self):
        s3_path = self.put_object("source/path/obj1", "hello")
        self.assertEqual(1, len(list_s3_paths(s3_path)))
        delete_s3_path(s3_path=s3_path)
        self.assertEqual(0, len(list_s3_paths(s3_path)))
        delete_s3_path(s3_path=s3_path)

    def test__delete_s3_path__handles_file(self):
        s3_path = self.put_object("source/path/obj1", "hello")
        self.assertEqual(1, len(list_s3_paths(s3_path)))
        delete_s3_path(s3_path=s3_path)
        self.assertEqual(0, len(list_s3_paths(s3_path)))

    def test__delete_s3_path__handles_folder(self):
        s3_path = self.get_s3_path("source/path/")
        self.put_object("source/path/obj1", "hello")
        self.put_object("source/path/dir1/obj2", "did you hear me")
        self.assertEqual(2, len(list_s3_paths(s3_path)))
        delete_s3_path(s3_path=s3_path)
        self.assertEqual(0, len(list_s3_paths(s3_path)))

    def test__update_s3_storage_class__handles_shallow_to_GLACIER(self):
        s3_root = self.get_s3_path("source/path/")

        for storage_class in [
            S3StorageClass.STANDARD,
            S3StorageClass.STANDARD_IA,
            S3StorageClass.INTELLIGENT_TIERING,
            S3StorageClass.GLACIER,
            # These will not be tested since they require restore before transition
            # Unfortunately, archive restores are not currently supported by moto
            # S3StorageClass.GLACIER_IR,
            # S3StorageClass.DEEP_ARCHIVE,
        ]:
            self.put_object(
                f"{s3_root.key}{storage_class.value}",
                "content",
                StorageClass=storage_class.value,
            )

        target_storage_class = S3StorageClass.GLACIER
        update_s3_storage_class(s3_root, target_storage_class)

        for object_contents in self.client__list_objects_v2()["Contents"]:
            self.assertEqual(object_contents.get("StorageClass"), target_storage_class.value)

    def test__update_s3_storage_class__handles_STANDARD_to_STANDARD_IA(self):
        s3_path = self.put_object(
            "source/path/to/file", "content", StorageClass=S3StorageClass.STANDARD.value
        )

        update_s3_storage_class(s3_path, S3StorageClass.STANDARD_IA)

        for object_contents in self.client__list_objects_v2()["Contents"]:
            self.assertEqual(object_contents.get("StorageClass"), S3StorageClass.STANDARD_IA)

    def test__update_s3_storage_class__handles_no_change_to_storage(self):
        s3_root = self.get_s3_path("source/path/")
        storage_class_paths = {
            storage_class: self.put_object(
                f"{s3_root.key}{storage_class.value}",
                "content",
                StorageClass=storage_class.value,
            )
            for storage_class in [
                S3StorageClass.STANDARD,
                S3StorageClass.STANDARD_IA,
                S3StorageClass.INTELLIGENT_TIERING,
                S3StorageClass.GLACIER_IR,
                S3StorageClass.GLACIER,
                S3StorageClass.DEEP_ARCHIVE,
            ]
        }

        for target_storage_class, s3_path in storage_class_paths.items():
            update_s3_storage_class(s3_path, target_storage_class)
            self.assertEqual(
                (get_object(s3_path).storage_class or S3StorageClass.STANDARD.value),
                target_storage_class.value,
            )

    def test__update_s3_storage_class__should_error_on_invalid_storage_class(self):
        s3_root = self.get_s3_path("source/path")
        storage_class_paths = {
            storage_class: self.put_object(
                f"{s3_root.key}/{storage_class.value}/test_file.txt",
                "content",
                StorageClass=storage_class.value,
            )
            for storage_class in [
                S3StorageClass.STANDARD,
                S3StorageClass.STANDARD_IA,
                S3StorageClass.INTELLIGENT_TIERING,
                S3StorageClass.GLACIER,
                S3StorageClass.GLACIER_IR,
                S3StorageClass.DEEP_ARCHIVE,
                S3StorageClass.REDUCED_REDUNDANCY,
            ]
        }

        for target_storage_class, s3_path in storage_class_paths.items():
            if target_storage_class in [S3StorageClass.REDUCED_REDUNDANCY]:
                with self.assertRaisesRegex(
                    RuntimeError, expected_regex=r".+unsupported current storage class.+"
                ):
                    update_s3_storage_class(s3_path, S3StorageClass.STANDARD)
            else:
                print(f"{target_storage_class}")
                self.assertEqual(True, update_s3_storage_class(s3_path, target_storage_class))

    def test__update_s3_storage_class__should_error_on_invalid_target_storage_class(self):
        s3_path = self.put_object(
            "source/path/to/file", "content", StorageClass=S3StorageClass.STANDARD.value
        )

        with self.assertRaisesRegex(
            RuntimeError, expected_regex=r".+unsupported target storage class.+"
        ):
            update_s3_storage_class(s3_path, S3StorageClass.OUTPOSTS)

    def test__should_sync__local_to_s3__outdated__SHOULD(self):
        s3_path = self.put_object("source", "hello")
        sleep(1)
        local_path = self.tmp_file(content="hello")
        assert should_sync(local_path, s3_path) is True

    def test__should_sync__local_to_s3__size_mismatch__SHOULD(self):
        local_path = self.tmp_file(content="hello")
        s3_path = self.put_object("source", "helloo")
        assert should_sync(local_path, s3_path) is True

    def test__should_sync__local_to_s3__content_mismatch__SHOULD(self):
        local_path = self.tmp_file(content="hello")
        s3_path = self.put_object("source", "olleh")
        assert should_sync(local_path, s3_path) is True

    def test__should_sync__local_to_s3__size_only__content_mismatch__SHOULD_NOT(self):
        local_path = self.tmp_file(content="hello")
        s3_path = self.put_object("source", "olleh")
        assert should_sync(local_path, s3_path, size_only=True) is False

    def test__should_sync__s3_to_local__size_mismatch__SHOULD(self):
        s3_path = self.put_object("source", "helloo")
        local_path = self.tmp_file(content="hello")
        assert should_sync(s3_path, local_path) is True

    def test__should_sync__s3_to_local__content_mismatch__SHOULD(self):
        s3_path = self.put_object("source", "olleh")
        local_path = self.tmp_file(content="hello")
        assert should_sync(s3_path, local_path) is True

    def test__should_sync__s3_to_local__size_only__content_mismatch__SHOULD_NOT(self):
        s3_path = self.put_object("source", "olleh")
        local_path = self.tmp_file(content="hello")
        assert should_sync(s3_path, local_path, size_only=True) is False

    def test__should_sync__s3_to_local__multipart_upload_with_custom_chunk_size_works(self):
        s3 = self.s3_client
        orig_file = self.tmp_file(content="0" * (5 * 1024 * 1024 + 1))
        source_path = self.get_s3_path("source")
        destination_path = self.tmp_path() / "destination"

        s3.upload_file(
            Filename=str(orig_file),
            **source_path.as_dict(),
            Config=TransferConfig(multipart_threshold=1024, multipart_chunksize=1024),
        )
        destination_path.write_text(orig_file.read_text())

        assert should_sync(orig_file, source_path) is False
        assert should_sync(source_path, destination_path) is False

    def test__should_sync__s3_to_local__multipart_upload_chunksize__gt__default(self):
        orig_file = self.tmp_file(content="0" * (AWS_S3_DEFAULT_CHUNK_SIZE_BYTES + 1))
        source_path1 = self.get_s3_path("source1")
        source_path2 = self.get_s3_path("source2")
        destination_path = self.tmp_path() / "destination"

        # This does not upload as multipart for custom chunk size
        self.s3_client.upload_file(
            Filename=str(orig_file),
            **source_path1.as_dict(),
            Config=TransferConfig(
                multipart_threshold=AWS_S3_DEFAULT_CHUNK_SIZE_BYTES * 2,
                multipart_chunksize=AWS_S3_DEFAULT_CHUNK_SIZE_BYTES * 2,
            ),
        )
        # This one uploads as multipart even though there is only one part
        self.s3_client.upload_file(
            Filename=str(orig_file),
            **source_path2.as_dict(),
            Config=TransferConfig(
                multipart_threshold=AWS_S3_DEFAULT_CHUNK_SIZE_BYTES,
                multipart_chunksize=AWS_S3_DEFAULT_CHUNK_SIZE_BYTES * 2,
            ),
        )

        destination_path.write_text(orig_file.read_text())

        assert should_sync(orig_file, source_path1) is False
        assert should_sync(orig_file, source_path2) is False
        assert should_sync(source_path1, destination_path) is False
        assert should_sync(source_path2, destination_path) is False

    def test__should_sync__handles_multipart_upload_chunksize(self):
        orig_file = self.tmp_file(content="0" * (MB + 1))
        source_path = self.get_s3_path("source")
        destination_path = self.tmp_path() / "destination"

        # This does not upload as multipart for custom chunk size
        self.s3_client.upload_file(
            Filename=str(orig_file),
            **source_path.as_dict(),
            Config=TransferConfig(multipart_threshold=MB, multipart_chunksize=MB),
        )

        destination_path.write_text(orig_file.read_text())

        assert should_sync(orig_file, source_path) is False
        assert should_sync(source_path, destination_path) is False

    def test__should_sync__handles_multipart_upload_chunksize__single_part(self):
        orig_file = self.tmp_file(content="0" * (MB + 1))
        source_path = self.get_s3_path("source")
        destination_path = self.tmp_path() / "destination"

        # This does not upload as multipart for custom chunk size
        self.s3_client.upload_file(
            Filename=str(orig_file),
            **source_path.as_dict(),
            Config=TransferConfig(multipart_threshold=MB, multipart_chunksize=2 * MB),
        )

        destination_path.write_text(orig_file.read_text())

        assert should_sync(orig_file, source_path) is False
        assert should_sync(source_path, destination_path) is False

    def test__should_sync__handles_multipart_upload__threshold_not_passed(self):
        orig_file = self.tmp_file(content="0" * (2 * MB))
        source_path = self.get_s3_path("source")
        destination_path = self.tmp_path() / "destination"

        # This does not upload as multipart for custom chunk size
        self.s3_client.upload_file(
            Filename=str(orig_file),
            **source_path.as_dict(),
            Config=TransferConfig(multipart_threshold=3 * MB, multipart_chunksize=MB),
        )

        destination_path.write_text(orig_file.read_text())

        assert should_sync(orig_file, source_path) is False
        assert should_sync(source_path, destination_path) is False

    def test__should_sync__handles_multipart_upload__gt_threshold_passed(self):
        orig_file = self.tmp_file(content="0" * (3 * MB))
        source_path = self.get_s3_path("source")
        destination_path = self.tmp_path() / "destination"

        # This does not upload as multipart for custom chunk size
        self.s3_client.upload_file(
            Filename=str(orig_file),
            **source_path.as_dict(),
            Config=TransferConfig(multipart_threshold=2 * MB, multipart_chunksize=MB),
        )
        destination_path.write_text(orig_file.read_text())

        assert should_sync(orig_file, source_path) is False
        assert should_sync(source_path, destination_path) is False

    def test__should_sync__source_missing_raises_error(self):
        s3_path = self.get_s3_path("source")
        local_path = self.tmp_file(content="hello")
        with self.assertRaises(ValueError):
            should_sync(s3_path, local_path)

    def test__check_paths_in_sync__simple__files_in_sync(self):
        source_path = self.tmp_file(content="hello")
        source_s3_path = self.put_object(key="source", content="hello")
        destination_path = self.tmp_file(content="hello")
        destination_s3_path = self.put_object(key="destination", content="hello")
        assert check_paths_in_sync(source_path, destination_path) is True
        assert check_paths_in_sync(source_path, destination_s3_path) is True
        assert check_paths_in_sync(source_s3_path, destination_path) is True
        assert check_paths_in_sync(source_s3_path, destination_s3_path) is True

    def test__check_paths_in_sync__simple__files_different(self):
        source_path = self.tmp_file(content="a")
        source_s3_path = self.put_object(key="source", content="b")
        destination_path = self.tmp_file(content="c")
        destination_s3_path = self.put_object(key="destination", content="d")
        assert check_paths_in_sync(source_path, destination_path) is False
        assert check_paths_in_sync(source_path, destination_s3_path) is False
        assert check_paths_in_sync(source_s3_path, destination_path) is False
        assert check_paths_in_sync(source_s3_path, destination_s3_path) is False

    def test__check_paths_in_sync__simple__dest_does_not_exist(self):
        source_path = self.tmp_file(content="a")
        source_s3_path = self.put_object(key="source", content="b")
        destination_path = self.tmp_path() / "destination"
        destination_s3_path = self.get_s3_path(key="destination")
        assert check_paths_in_sync(source_path, destination_path) is False
        assert check_paths_in_sync(source_path, destination_s3_path) is False
        assert check_paths_in_sync(source_s3_path, destination_path) is False
        assert check_paths_in_sync(source_s3_path, destination_s3_path) is False

    def test__check_paths_in_sync__simple__folders_in_sync(self):
        source_path = self.tmp_path()
        (source_path / "a").write_text("hello")
        (source_path / "b").write_text("again")

        source_s3_path = self.get_s3_path("source")
        self.put_object(key="source/a", content="hello")
        self.put_object(key="source/b", content="again")

        destination_path = self.tmp_path()
        (destination_path / "a").write_text("hello")
        (destination_path / "b").write_text("again")

        destination_s3_path = self.get_s3_path("destination")
        self.put_object(key="destination/a", content="hello")
        self.put_object(key="destination/b", content="again")

        assert check_paths_in_sync(source_path, destination_path) is True
        assert check_paths_in_sync(source_path, destination_s3_path) is True
        assert check_paths_in_sync(source_s3_path, destination_path) is True
        assert check_paths_in_sync(source_s3_path, destination_s3_path) is True

        # Test with trailing slash
        assert check_paths_in_sync(source_path, destination_path) is True
        assert check_paths_in_sync(source_path, destination_s3_path.with_folder_suffix) is True
        assert check_paths_in_sync(source_s3_path.with_folder_suffix, destination_path) is True
        assert (
            check_paths_in_sync(
                source_s3_path.with_folder_suffix, destination_s3_path.with_folder_suffix
            )
            is True
        )

    def test__check_paths_in_sync__simple__folders_different(self):
        source_path = self.tmp_path()
        (source_path / "a").write_text("hello")
        (source_path / "b").write_text("again")

        source_s3_path = self.get_s3_path("source")
        self.put_object(key="source/a", content="hello")
        self.put_object(key="source/b", content="again")

        destination_path = self.tmp_path()
        (destination_path / "A").write_text("hello")
        (destination_path / "B").write_text("again")

        destination_s3_path = self.get_s3_path("destination")
        self.put_object(key="destination/A", content="hello")
        self.put_object(key="destination/B", content="again")

        destination_path2 = self.tmp_path()
        (destination_path2 / "a").write_text("helloo")
        (destination_path2 / "b").write_text("againn")

        destination_s3_path2 = self.get_s3_path("destination2")
        self.put_object(key="destination2/a", content="helloo")
        self.put_object(key="destination2/b", content="againn")

        # Should fail for filename mismatch
        assert check_paths_in_sync(source_path, destination_path) is False
        assert check_paths_in_sync(source_path, destination_s3_path) is False
        assert check_paths_in_sync(source_s3_path, destination_path) is False
        assert check_paths_in_sync(source_s3_path, destination_s3_path) is False

        # Should fail for content mismatch
        assert check_paths_in_sync(source_path, destination_path2) is False
        assert check_paths_in_sync(source_path, destination_s3_path2) is False
        assert check_paths_in_sync(source_s3_path, destination_path2) is False
        assert check_paths_in_sync(source_s3_path, destination_s3_path2) is False

    def test__check_paths_in_sync__handles_sorting_issues__folders_same(self):
        source_path = self.tmp_path()
        (source_path / "a.txt").write_text("hello")
        (source_path / "a").mkdir()
        (source_path / "a" / "b.txt").write_text("again")

        source_s3_path = self.get_s3_path("source")
        self.put_object(key="source/a.txt", content="hello")
        self.put_object(key="source/a/b.txt", content="again")

        destination_path = self.tmp_path()
        (destination_path / "a.txt").write_text("hello")
        (destination_path / "a").mkdir()
        (destination_path / "a" / "b.txt").write_text("again")

        destination_s3_path = self.get_s3_path("destination")
        self.put_object(key="destination/a.txt", content="hello")
        self.put_object(key="destination/a/b.txt", content="again")

        # Should succeed
        assert check_paths_in_sync(source_path, destination_path) is True
        assert check_paths_in_sync(source_path, destination_s3_path) is True
        assert check_paths_in_sync(source_s3_path, destination_path) is True
        assert check_paths_in_sync(source_s3_path, destination_s3_path) is True

    def test__check_paths_in_sync__handles_folder_placeholders(self):
        source_s3_path = self.get_s3_path("source")
        self.put_object(key="source/", content="")
        self.put_object(key="source/dirA/", content="")
        self.put_object(key="source/dirA/a.txt", content="hello")

        destination_s3_path = self.get_s3_path("destination/")
        self.put_object(key="destination/dirA/a.txt", content="hello")

        # Should succeed
        assert (
            check_paths_in_sync(
                source_s3_path, destination_s3_path, ignore_folder_placeholder_objects=True
            )
            is True
        )

        # Should fail if not ignoring folder placeholder objects
        assert (
            check_paths_in_sync(
                source_s3_path, destination_s3_path, ignore_folder_placeholder_objects=False
            )
            is False
        )


@fixture(scope="function")
def s3_bucket_fixture(aws_credentials_fixture, request):
    with moto.mock_aws():
        s3 = get_s3_resource()
        s3_bucket = s3.Bucket(request.param)
        s3_bucket.create(CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        yield s3_bucket


@fixture(scope="function")
def create_file(tmp_path_factory, request):
    temp_file = tmp_path_factory.mktemp("data") / "temp_file.txt"
    temp_file.write_text("*" * request.param)
    yield temp_file


@mark.parametrize(
    "s3_bucket_fixture, create_file, transfer_config, expected_threshold, expected_chunksize",
    [
        param(
            "bucket-name",
            MB,
            TransferConfig(multipart_threshold=2 * MB, multipart_chunksize=2 * MB),
            None,
            None,
            id="file_size<threshold=chunksize<5MB",
        ),
        param(
            "bucket-name",
            MB,
            TransferConfig(multipart_threshold=MB, multipart_chunksize=2 * MB),
            MB,
            None,
            id="threshold=file_size<chunksize",
        ),
        param(
            "bucket-name",
            4 * MB,
            TransferConfig(multipart_threshold=MB, multipart_chunksize=2 * MB),
            4 * MB,
            None,
            id="threshold<chunksize<file_size<5MB",
        ),
        param(
            "bucket-name",
            7 * MB,
            TransferConfig(multipart_threshold=8 * MB, multipart_chunksize=6 * MB),
            None,
            None,
            id="chunksize<file_size<threshold",
        ),
        param(
            "bucket-name",
            7 * MB,
            TransferConfig(multipart_threshold=7 * MB, multipart_chunksize=6 * MB),
            6 * MB,
            6 * MB,
            id="chunksize<threshold<file_size",
        ),
        param(
            "bucket-name",
            20 * MB,
            TransferConfig(multipart_threshold=10 * MB, multipart_chunksize=4 * MB),
            # Minimum value is 5 MiB
            5 * MB,
            5 * MB,
            id="chunksize<5MB<threshold<<file_size",
        ),
        param(
            "bucket-name",
            12 * MB,
            TransferConfig(multipart_threshold=MB, multipart_chunksize=6 * MB),
            6 * MB,
            6 * MB,
            id="threshold<5MB<chunksize<<file_size",
        ),
        param(
            "bucket-name",
            9 * MB,
            TransferConfig(multipart_threshold=10 * MB, multipart_chunksize=10 * MB),
            9 * MB + 1,
            9 * MB + 1,
            id="default<file_size<threshold=chunksize",
        ),
    ],
    indirect=["s3_bucket_fixture", "create_file"],
)
def test__determine_multipart_attributes__works(
    s3_bucket_fixture, create_file, transfer_config, expected_threshold, expected_chunksize
):
    local_file = create_file
    s3_bucket_fixture.upload_file(Filename=str(local_file), Key="path", Config=transfer_config)
    s3_path = S3URI(f"s3://{s3_bucket_fixture.name}/path")

    chunksize, threshold = determine_multipart_attributes(s3_path)
    assert threshold == expected_threshold
    assert chunksize == expected_chunksize


@mark.parametrize(
    "input, expected, raises_error",
    [
        # Local -> Local tests
        param(
            (Path("/src/key"), Path("/dest/key")),
            None,
            raises(ValueError),
            id="(local -> local) Invalid because not supported",
        ),
        # Local -> S3 tests
        param(
            (Path("/src/key"), any_s3_uri("dest/key")),
            S3UploadRequest(Path("/src/key"), any_s3_uri("dest/key")),
            does_not_raise(),
            id="(local -> s3) simple inputs, no prefix to remove",
        ),
        param(
            (Path("/src/key"), any_s3_uri("dest/key"), "/src/key"),
            S3UploadRequest(Path("/src/key"), any_s3_uri("dest/key")),
            does_not_raise(),
            id="(local -> s3) removes entire source path prefix explicitly",
        ),
        param(
            (Path("/src/key"), any_s3_uri("dest/key"), "/src"),
            S3UploadRequest(Path("/src/key"), any_s3_uri("dest/key/key")),
            does_not_raise(),
            id="(local -> s3) removes part of source path specified from prefix",
        ),
        param(
            (Path("/src/key"), any_s3_uri("dest/key"), "/src/"),
            S3UploadRequest(Path("/src/key"), any_s3_uri("dest/keykey")),
            does_not_raise(),
            id="(local -> s3) removes part of source path specified from prefix with trailing slash",  # noqa: E501
        ),
        param(
            (Path("/A/folder/"), any_s3_uri("B/key", "dest"), "/A"),
            S3UploadRequest(Path("/A/folder/"), any_s3_uri("B/key/folder", "dest")),
            does_not_raise(),
            id="(local -> s3) removes part of source path folder specified from prefix",
        ),
        param(
            (Path("/A/folder/"), any_s3_uri("B/key", "dest"), "/A/folder"),
            S3UploadRequest(Path("/A/folder/"), any_s3_uri("B/key", "dest")),
            does_not_raise(),
            id="(local -> s3) removes all including trailing slash of source path folder",
        ),
        # S3 -> Local tests
        param(
            (any_s3_uri(key="src/key"), Path("/dest/key")),
            S3DownloadRequest(any_s3_uri(key="src/key"), Path("/dest/key")),
            does_not_raise(),
            id="(s3 -> local) simple inputs, no prefix to remove",
        ),
        param(
            (any_s3_uri(key="src/key"), Path("/dest/key"), "src/key"),
            S3DownloadRequest(any_s3_uri(key="src/key"), Path("/dest/key")),
            does_not_raise(),
            id="(s3 -> local) removes entire source path prefix explicitly",
        ),
        param(
            (any_s3_uri("src/key"), Path("/dest/key"), "src"),
            S3DownloadRequest(any_s3_uri("src/key"), Path("/dest/key/key")),
            does_not_raise(),
            id="(s3 -> local) removes part of source path specified from prefix",
        ),
        param(
            (any_s3_uri("src/key"), Path("/dest/key"), "src/"),
            # TODO: this might not be appropriate
            S3DownloadRequest(any_s3_uri("src/key"), Path("/dest/key/key")),
            does_not_raise(),
            id="(s3 -> local) removes part of source path specified from prefix with trailing slash",  # noqa: E501
        ),
        param(
            (any_s3_uri("A/folder/"), Path("/B/key"), "A"),
            S3DownloadRequest(any_s3_uri("A/folder/"), Path("/B/key/folder/")),
            does_not_raise(),
            id="(s3 -> local) removes part of source path folder specified from prefix",
        ),
        param(
            (any_s3_uri("A/folder/"), Path("/B/key"), "A/folder"),
            S3DownloadRequest(any_s3_uri("A/folder/"), Path("/B/key/")),
            does_not_raise(),
            id="(s3 -> local) removes all but trailing slash of source path folder",
        ),
        param(
            (any_s3_uri("src/key//value"), Path("/dest/key//value"), "src"),
            S3DownloadRequest(any_s3_uri("src/key//value"), Path("/dest/key/value/key/value")),
            does_not_raise(),
            id="(s3 -> local) removes part of source path specified from prefix and cleans extra slashes",  # noqa: E501
        ),
        # S3 to S3
        param(
            (any_s3_uri("src/key"), any_s3_uri("dest/key")),
            S3CopyRequest(any_s3_uri("src/key"), any_s3_uri("dest/key")),
            does_not_raise(),
            id="(s3 -> s3) simple inputs, no prefix to remove",
        ),
        param(
            (any_s3_uri("src/key"), any_s3_uri("dest/key"), "src/key"),
            S3CopyRequest(any_s3_uri("src/key"), any_s3_uri("dest/key")),
            does_not_raise(),
            id="(s3 -> s3) removes entire source path prefix explicitly",
        ),
        param(
            (any_s3_uri("src/key"), any_s3_uri("dest/key"), "src"),
            S3CopyRequest(any_s3_uri("src/key"), any_s3_uri("dest/key/key")),
            does_not_raise(),
            id="(s3 -> s3) removes part of source path specified from prefix",
        ),
        param(
            (any_s3_uri("src/key"), any_s3_uri("dest/key"), "src/"),
            S3CopyRequest(any_s3_uri("src/key"), any_s3_uri("dest/keykey")),
            does_not_raise(),
            id="(s3 -> s3) removes part of source path specified from prefix with trailing slash",
        ),
        param(
            (any_s3_uri("A/folder/"), any_s3_uri("B/key", "dest"), "A"),
            S3CopyRequest(any_s3_uri("A/folder/"), any_s3_uri("B/key/folder/", "dest")),
            does_not_raise(),
            id="(s3 -> s3) removes part of source path folder specified from prefix",
        ),
        param(
            (any_s3_uri("A/folder/"), any_s3_uri("B/key", "dest"), "A/folder"),
            S3CopyRequest(any_s3_uri("A/folder/"), any_s3_uri("B/key/", "dest")),
            does_not_raise(),
            id="(s3 -> s3) removes all but trailing slash of source path folder",
        ),
        param(
            (any_s3_uri("src/key//value"), any_s3_uri("dest/key//value"), "src"),
            S3CopyRequest(any_s3_uri("src/key//value"), any_s3_uri("dest/key/value/key/value")),
            does_not_raise(),
            id="(s3 -> s3) removes part of source path specified from prefix and cleans extra slashes",  # noqa: E501
        ),
        param(
            (any_s3_uri("src/key"), any_s3_uri("dest/key"), "dest"),
            None,
            raises(ValueError),
            id="(s3 -> s3) Invalid prefix: does not match",
        ),
        param(
            (any_s3_uri("src/key"), any_s3_uri("dest/key"), "src/key/"),
            None,
            raises(ValueError),
            id="(s3 -> s3) Invalid prefix: too long",
        ),
    ],
)
def test__generate_transfer_request(input, expected, raises_error):
    with raises_error:
        actual = generate_transfer_request(*input)

    if expected is not None:
        assert actual == expected
