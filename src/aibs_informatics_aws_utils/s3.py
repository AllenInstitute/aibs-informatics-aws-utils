import hashlib
import json
import logging
import math
import os
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from multiprocessing.pool import ThreadPool
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Pattern,
    Set,
    Tuple,
    Union,
)
from urllib import parse

from aibs_informatics_core.models.aws.s3 import (
    S3URI,
    S3CopyRequest,
    S3DownloadRequest,
    S3PathStats,
    S3RestoreStatus,
    S3RestoreStatusEnum,
    S3StorageClass,
    S3TransferRequest,
    S3TransferResponse,
    S3UploadRequest,
)
from aibs_informatics_core.utils.decorators import retry
from aibs_informatics_core.utils.file_operations import (
    find_paths,
    get_path_with_root,
    remove_path,
    strip_path_root,
)
from aibs_informatics_core.utils.json import JSON
from aibs_informatics_core.utils.logging import get_logger
from aibs_informatics_core.utils.multiprocessing import parallel_starmap
from aibs_informatics_core.utils.time import get_current_time
from aibs_informatics_core.utils.tools.strtools import is_prefixed
from boto3.s3.transfer import TransferConfig as TransferConfig
from botocore.client import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    ResponseStreamingError,
)

from aibs_informatics_aws_utils.core import (
    AWSService,
    client_error_code_check,
    get_client_error_message,
)
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.service_resource import Object
else:
    S3Client = object
    Object = object


logger = get_logger()
logger.setLevel(level=logging.INFO)

get_s3_client = AWSService.S3.get_client
get_s3_resource = AWSService.S3.get_resource


S3_SCRATCH_TAGGING_KEY = "time-to-live"
S3_SCRATCH_TAGGING_VALUE = "scratch"


PathOrUri = Union[Path, S3URI]


SCRATCH_EXTRA_ARGS = {
    "Tagging": parse.urlencode({S3_SCRATCH_TAGGING_KEY: S3_SCRATCH_TAGGING_VALUE})
}

KB = 1024
MB = KB * KB
AWS_S3_DEFAULT_CHUNK_SIZE_BYTES = 8 * MB

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
AWS_S3_MULTIPART_LIMIT = 10000


def download_to_json_object(s3_path: S3URI, **kwargs) -> Dict[str, Any]:
    content = download_to_json(s3_path=s3_path, **kwargs)
    assert isinstance(content, dict)
    return content


def download_to_json(s3_path: S3URI, **kwargs) -> JSON:
    """Helper method to read a json file from S3."""
    logger.info(f"Reading file from s3: {s3_path}")
    s3_obj = get_object(s3_path=s3_path, **kwargs)

    try:
        data = json.load(s3_obj.get()["Body"])
    except Exception as e:
        raise AWSError(f"Error reading json data from {s3_path} [{e}]")

    return data


def download_s3_path(
    s3_path: S3URI,
    local_path: Path,
    exist_ok: bool = False,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = True,
    size_only: bool = False,
    **kwargs,
):
    """Download an S3 Object or Folder to a local path

    This mimics the behavior of `aws s3 cp`, both with and without the `--recursive` flag.

    The logic for using recursive behavior is as follows:
    - for non-trailing-slash S3 paths,
        - If S3 path is an object, it will be downloaded as a file.
        - If S3 path is an object prefix, it will be downloaded as a folder.
    - for trailing-slash S3 paths,
        - If S3 path is a non-empty object and object prefix, an error will be raised.
        - If S3 path is an object prefix, it will be downloaded as a folder.
        - If S3 path is an object, it will be downloaded as a file.

    Args:
        s3_path (S3URI): URI of the object of folder
        local_path (Path): local destination path.
        exist_ok (bool, optional): If true, local path may exist previously. Defaults to False.

    Raises:
        ApplicationException: If S3 URI does not exist.
    """
    path_is_object = is_object(s3_path, **kwargs)
    path_is_prefix = is_object_prefix(s3_path, **kwargs)
    path_has_folder_slash = s3_path.key.endswith("/")
    path_is_non_empty_object_fn = lru_cache(None)(
        lambda: path_is_object and get_object(s3_path, **kwargs).content_length != 0
    )

    # Scenarios for object download:
    #   1. path IS OBJECT and NOT HAS FOLDER SLASH
    #   2. path IS OBJECT and HAS FOLDER SLASH but NOT OBJECT PREFIX
    # Scenarios for object prefix download:
    #   1. path IS OBJECT PREFIX and NOT OBJECT
    #   2. path IS OBJECT PREFIX and HAS FOLDER SLASH and (NOT OBJECT or IS NON-EMPTY OBJECT)
    # Scenarios for raising error:
    #   1. path is NOT OBJECT and NOT OBJECT PREFIX
    #   2. path is OBJECT and OBJECT PREFIX and HAS FOLDER SLASH and IS NON-EMPTY OBJECT
    if (path_is_object and not path_has_folder_slash) or (path_is_object and not path_is_prefix):
        logger.info(f"{s3_path} is an object. Downloading to {local_path} as file.")
        download_s3_object(
            s3_path=s3_path,
            local_path=local_path,
            exist_ok=exist_ok,
            transfer_config=transfer_config,
            force=force,
            size_only=size_only,
            **kwargs,
        )
    elif (path_is_prefix and not path_is_object) or (
        path_is_prefix and path_has_folder_slash and not path_is_non_empty_object_fn()
    ):
        logger.info(f"{s3_path} is an object prefix. Downloading to {local_path} as folder.")
        download_s3_object_prefix(
            s3_path=s3_path,
            local_path=local_path,
            exist_ok=exist_ok,
            transfer_config=transfer_config,
            force=force,
            size_only=size_only,
            **kwargs,
        )
    elif not path_is_object and not path_is_prefix:
        raise AWSError(f"{s3_path} is neither an object or prefix. Does it exist?")
    elif (
        path_is_object
        and path_is_prefix
        and path_has_folder_slash
        and path_is_non_empty_object_fn()
    ):
        raise AWSError(
            f"{s3_path} is an object and object prefix and has folder suffix "
            "and is non-empty. This is not supported"
        )
    else:
        raise AWSError(
            f"Unhandled scenario for {s3_path}: is_object={path_is_object}, "
            f"is_object_prefix={path_is_prefix}, "
            f"has_folder_slash={path_has_folder_slash}, "
            f"is_non_empty_object={path_is_non_empty_object_fn()}"
        )


def download_s3_object_prefix(
    s3_path: S3URI,
    local_path: Path,
    exist_ok: bool = False,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = True,
    size_only: bool = False,
    **kwargs,
):
    """Download an S3 object prefix to a local path

    Args:
        s3_path (S3URI): URI of the object of folder
        local_path (Path): local destination path.
        exist_ok (bool, optional): If true, local path may exist previously. Defaults to False.

    """

    s3_object_paths = list_s3_paths(s3_path=s3_path, **kwargs)
    logger.info(f"Downloading {len(s3_object_paths)} objects under {local_path}")

    if not exist_ok and local_path.exists() and local_path.is_dir() and any(local_path.iterdir()):
        raise AWSError(
            f"{local_path} already exists and is not empty. Cannot download to the directory"
        )
    for s3_object_path in s3_object_paths:
        relative_key = s3_object_path.key[len(s3_path.key) :].lstrip("/")
        if s3_object_path.has_folder_suffix():
            if get_object(s3_object_path, **kwargs).content_length != 0:
                err_msg = (
                    f"Cannot download S3 object {s3_path} to {local_path}. Downloads of "
                    "objects with '/' suffixed keys and of non-zero size are NOT supported."
                )
                logger.error(err_msg)
                raise AWSError(err_msg)
            continue
        local_filepath = (local_path / relative_key).resolve()
        download_s3_object(
            s3_path=s3_object_path,
            local_path=local_filepath,
            exist_ok=exist_ok,
            transfer_config=transfer_config,
            force=force,
            size_only=size_only,
            **kwargs,
        )


@retry(
    (ConnectionClosedError, EndpointConnectionError, ResponseStreamingError, ClientError),
    tries=10,
    backoff=2.0,
)
def download_s3_object(
    s3_path: S3URI,
    local_path: Path,
    exist_ok: bool = False,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = True,
    size_only: bool = False,
    **kwargs,
):
    """Download contents of an S3 object to file

    Args:
        s3_path (S3URI): S3 URI to object
        local_path (Path): destination path
        exist_ok (bool): If true, local path can already exist. Defaults to False.

    Raises:
        ValueError: Raised if local path exists.
    """
    s3_object = get_object(s3_path, **kwargs)
    if force or should_sync(
        source_path=s3_path, destination_path=local_path, size_only=size_only, **kwargs
    ):
        if local_path.exists() and not exist_ok and not force:
            raise ValueError(f"Unable to download S3 object to {local_path}. Path exists.")
        elif local_path.exists() and local_path.is_dir() and exist_ok:
            logger.warning(
                f"Overwriting directory {local_path} with S3 object {s3_path}"
                "This may cause unexpected behavior."
            )
            try:
                local_path.rmdir()
            except Exception as e:
                logger.error("Error removing directory: {e}")
                raise e
        local_path.parent.mkdir(parents=True, exist_ok=True)
        s3_object.download_file(Filename=str(local_path.resolve()), Config=transfer_config)


def upload_json(
    content: JSON, s3_path: S3URI, extra_args: Optional[Dict[str, Any]] = None, **kwargs
):
    with NamedTemporaryFile("w") as f:
        f.write(json.dumps(content, sort_keys=True))
        f.flush()

        upload_file(Path(f.name), s3_path=s3_path, extra_args=extra_args, **kwargs)


def upload_scratch_file(
    local_path: Path, s3_path: S3URI, extra_args: Optional[Dict[str, Any]] = None, **kwargs
):
    extra_args = extra_args or {}
    extra_args.update(SCRATCH_EXTRA_ARGS)
    upload_file(local_path=local_path, s3_path=s3_path, extra_args=extra_args, **kwargs)


def upload_path(
    local_path: Path,
    s3_path: S3URI,
    extra_args: Optional[Dict[str, Any]] = None,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = True,
    size_only: bool = False,
    **kwargs,
):
    logger.info(f"Uploading contents at {local_path} to {s3_path}")
    if local_path.is_file():
        logger.info(f"{local_path} is a file.")
        upload_file(
            local_path=local_path,
            s3_path=s3_path,
            extra_args=extra_args,
            transfer_config=transfer_config,
            force=force,
            size_only=size_only,
            **kwargs,
        )
    elif local_path.is_dir():
        logger.info(f"{local_path} is a directory. Uploading all nested files to {s3_path}")
        upload_folder(
            local_path=local_path,
            s3_path=s3_path,
            extra_args=extra_args,
            transfer_config=transfer_config,
            force=force,
            size_only=size_only,
            **kwargs,
        )
    else:
        msg = f"Cannot upload {local_path} to S3. Path does not exist!"
        logger.error(msg)
        raise ValueError(msg)


def upload_folder(
    local_path: Path,
    s3_path: S3URI,
    extra_args: Optional[Dict[str, Any]] = None,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = True,
    size_only: bool = False,
    **kwargs,
):
    local_paths = find_paths(local_path, include_dirs=False, include_files=True)
    for source_path in local_paths:
        destination_key = os.path.normpath(s3_path.key + source_path[len(str(local_path)) :])
        destination_path = S3URI.build(bucket_name=s3_path.bucket, key=destination_key)
        logger.debug(f"Uploading '{source_path}' to '{destination_path}'")
        upload_file(
            local_path=source_path,
            s3_path=destination_path,
            extra_args=extra_args,
            transfer_config=transfer_config,
            force=force,
            size_only=size_only,
            **kwargs,
        )
    logger.info(f"Uploaded {len(local_paths)} files to: {s3_path}")


@retry(ResponseStreamingError, tries=10, backoff=2.0)
def upload_file(
    local_path: Union[str, Path],
    s3_path: S3URI,
    extra_args: Optional[Dict[str, Any]] = None,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = True,
    size_only: bool = False,
    **kwargs,
):
    s3_client = get_s3_client(**kwargs)
    if force or should_sync(
        source_path=Path(local_path), destination_path=s3_path, size_only=size_only, **kwargs
    ):
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=s3_path.bucket,
            Key=s3_path.key,
            ExtraArgs=extra_args or {},
            Config=transfer_config or TransferConfig(),
        )
    elif extra_args:
        # This handles scenario where extra args are specified but destination is more recent
        if "Tagging" in extra_args and "TaggingDirective" not in extra_args:
            extra_args["TaggingDirective"] = "REPLACE"
        if "Metadata" in extra_args and "MetadataDirective" not in extra_args:
            extra_args["MetadataDirective"] = "REPLACE"

        copy_s3_object(
            source_path=s3_path,
            destination_path=s3_path,
            extra_args=extra_args,
            transfer_config=transfer_config,
            force=True,
        )


def get_object(s3_path: S3URI, **kwargs) -> Object:
    s3 = get_s3_resource(**kwargs)
    try:
        return s3.Object(s3_path.bucket, s3_path.key)
    except ClientError as e:
        raise AWSError(f"Error finding s3 object: {s3_path} {e}") from e


def is_object(s3_path: S3URI, **kwargs) -> bool:
    s3 = get_s3_client(**kwargs)
    try:
        s3.head_object(Bucket=s3_path.bucket, Key=s3_path.key)
    except ClientError:
        return False
    return True


def is_object_prefix(s3_path: S3URI, **kwargs) -> bool:
    s3 = get_s3_client(**kwargs)
    response = s3.list_objects_v2(
        Bucket=s3_path.bucket,
        Prefix=s3_path.key,
        # StartAfter ensures we don't include objects at this key prefix.
        StartAfter=s3_path.key,
        MaxKeys=1,
    )
    return len(response.get("Contents", [])) > 0


def is_folder(s3_path: S3URI, **kwargs) -> bool:
    """Check if S3 Path is a "folder" or object

    To be a "folder", it must satisfy following conditions:
    - The Key Prefix HAS objects under prefix
    - All objects under Key Prefix are separated by '/'
        - i.e. key=key_prefix/...obj1, key=key_prefix/...obj2, etc.

    Example:

        For a bucket="s3://bucket" with the following keys:
            /path/to/one/object1
            /path/to/one/object2
            /path/to/one_object1
            /path/to/one_object2
            /path/to/another
            /path/to/another/object1
            /path/to/another/object2

        is_folder("s3://bucket/path/to/one") >> TRUE
        is_folder("s3://bucket/path/to/another") >> TRUE
        is_folder("s3://bucket/path/to/another/") >> TRUE

        is_folder("s3://bucket/path/to/one_") >> FALSE
        is_folder("s3://bucket/path/to/one_object1") >> FALSE
        is_folder("s3://bucket/path/to/doesnotexist") >> FALSE

    Args:
        s3_path (S3URI): S3 URI

    Returns:
        True if s3 path is a folder
    """
    return is_object_prefix(
        s3_path=S3URI.build(bucket_name=s3_path.bucket, key=s3_path.key_with_folder_suffix),
        **kwargs,
    )


def get_s3_path_collection_stats(*s3_paths: S3URI, **kwargs) -> Mapping[S3URI, S3PathStats]:
    return dict(
        zip(
            s3_paths,
            parallel_starmap(get_s3_path_stats, [(_,) for _ in s3_paths], kwargs, ThreadPool),
        )
    )


def get_s3_path_stats(s3_path: S3URI, **kwargs) -> S3PathStats:
    """Adds some additional metadata to the file metadata to be stored
    in the GFS, such as ingestion time and file size.

    Args:
        s3_path (S3URI): Path to the file in s3.

    Returns:
        file_stats (dict): Dictionary object containing the additional
            values for the file stats.
    """
    s3 = get_s3_client(**kwargs)
    last_modified = get_current_time()
    bucket, key = s3_path.bucket, s3_path.key
    logger.info(f"Getting file stats: {s3_path}")

    size_bytes: Optional[int] = None
    object_count: Optional[int] = None

    try:
        file_info = s3.head_object(Bucket=bucket, Key=key)
        last_modified = file_info["LastModified"]
        size_bytes = file_info["ContentLength"]
        object_count = 1
    except ClientError:
        logger.debug("Caught client error, assuming it means this is a dir")
        # This means that the path is most likely a prefix, not an object key
        size_bytes, object_count = _get_prefix_size_and_count(
            bucket_name=bucket, key_prefix=key, **kwargs
        )
        last_modified = _get_prefix_last_modified(bucket_name=bucket, key_prefix=key, **kwargs)
    except Exception as e:
        logger.error("Caught unexpected exception.")
        logger.exception(e)
        raise e

    return S3PathStats(
        last_modified=last_modified, size_bytes=size_bytes, object_count=object_count
    )


# TODO: Two things
# 1. allow for a way to specify `size_only` for this fn when transfering large number of files
# 2. add flag for failing if no source data exists.
def sync_paths(
    source_path: Union[Path, S3URI],
    destination_path: Union[Path, S3URI],
    source_path_prefix: Optional[str] = None,
    include: Optional[List[Pattern]] = None,
    exclude: Optional[List[Pattern]] = None,
    extra_args: Optional[Dict[str, Any]] = None,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = False,
    size_only: bool = False,
    delete: bool = False,
    **kwargs,
) -> List[S3TransferResponse]:
    logger.info(f"Syncing {source_path} to {destination_path}")

    source_path_key = source_path.key if isinstance(source_path, S3URI) else str(source_path)
    if not source_path_prefix:
        source_path_prefix = source_path_key
    elif not is_prefixed(source_path_key, source_path_prefix):
        raise ValueError(
            f"The source path prefix '{source_path_prefix}' does not match "
            f"source path {source_path}"
        )
    if isinstance(source_path, S3URI):
        nested_source_paths = list_s3_paths(
            s3_path=source_path.with_folder_suffix,
            include=include,
            exclude=exclude,
            **kwargs,
        )
        if is_object(source_path, **kwargs) and source_path not in nested_source_paths:
            nested_source_paths.insert(0, source_path)
    else:
        nested_source_paths = [
            Path(p)  # type: ignore
            for p in find_paths(
                root=source_path,
                include_dirs=False,
                includes=include,  # type: ignore
                excludes=exclude,  # type: ignore
            )
        ]

    logger.info(f"S3 Sync: Identified {len(nested_source_paths)} objects to sync")
    requests = [
        generate_transfer_request(
            source_path=nested_source_path,
            destination_path=destination_path,
            source_path_prefix=source_path_prefix,
            extra_args=extra_args,
        )
        for nested_source_path in nested_source_paths
    ]

    responses = process_transfer_requests(
        *requests,
        transfer_config=transfer_config,
        force=force,
        size_only=size_only,
        **kwargs,
    )

    if delete:
        logger.info("Sync: checking for files to delete following sync")
        if isinstance(destination_path, S3URI):
            unexpected_paths = set(list_s3_paths(destination_path, **kwargs)).difference(
                [S3URI(_.request.destination_path) for _ in responses]
            )
            logger.info(f"Sync: identified {len(unexpected_paths)} paths for deletion")
            for unexpected_path in unexpected_paths:
                delete_s3_path(unexpected_path, **kwargs)
        else:
            unexpected_local_paths = set(
                find_paths(destination_path, include_dirs=False)
            ).difference([str(_.request.destination_path) for _ in responses])
            logger.info(f"Sync: identified {len(unexpected_local_paths)} paths for deletion")
            for unexpected_local_path in unexpected_local_paths:
                remove_path(Path(unexpected_local_path))

    failures = 0
    for response in responses:
        failures += response.failed
    logger.info(f"Synced {len(requests) - failures} successfully. {failures} requests failed.")

    return responses


def generate_transfer_request(
    source_path: Union[Path, S3URI],
    destination_path: Union[Path, S3URI],
    source_path_prefix: Optional[str] = None,
    extra_args: Optional[Dict[str, Any]] = None,
) -> S3TransferRequest:
    """Create a S3 transfer request

    Args:
        source_path (Path|S3URI): source copy path
        destination_path (Path|S3URI): destination copy path
        source_path_prefix (str, optional): Optional prefix to remove from source path.
            Defaults to source path key.
    Returns:
        S3TransferRequest: _description_
    """
    relative_source_path = ""
    if source_path_prefix:
        source_key = source_path.key if isinstance(source_path, S3URI) else str(source_path)

        if not source_key.startswith(source_path_prefix):
            raise ValueError(
                f"Cannot generate S3CopyRequest with src={source_path}, dst={destination_path}. "
                f"source path prefix={source_path_prefix} is specified but does"
                " not match prefix or source path"
            )
        relative_source_path = source_key[len(source_path_prefix) :]

    if isinstance(destination_path, S3URI):
        # This will be sanitized by S3URI class (removing double slashes)
        new_destination_path = S3URI(destination_path + relative_source_path)
        if isinstance(source_path, S3URI):
            return S3CopyRequest(source_path, new_destination_path, extra_args=extra_args)
        else:
            return S3UploadRequest(source_path, new_destination_path, extra_args=extra_args)
    elif isinstance(source_path, S3URI) and isinstance(destination_path, Path):
        local_destination_path: Path = (
            Path(get_path_with_root(relative_source_path, destination_path))
            if relative_source_path
            else destination_path
        )
        return S3DownloadRequest(source_path, local_destination_path)
    else:
        raise ValueError("Local to local transfer is not ")


def process_transfer_requests(
    *transfer_requests: S3TransferRequest,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = False,
    size_only: bool = False,
    suppress_errors: bool = False,
    **kwargs,
) -> List[S3TransferResponse]:
    """Process a list of S3 transfer requests

    Args:
        transfer_config (Optional[TransferConfig]): transfer config.
            Defaults to None.
        force (bool): Whether to force the transfer.
            Defaults to False.
        size_only (bool): Whether to only check size when transferring.
            Defaults to False.
        suppress_errors (bool): Whether to suppress errors.
            Defaults to False.

    Returns:
        List[S3TransferResponse]: List of transfer responses
    """
    transfer_responses = []

    for i, request in enumerate(transfer_requests):
        try:
            if isinstance(request, S3CopyRequest):
                copy_s3_object(
                    source_path=request.source_path,
                    destination_path=request.destination_path,
                    extra_args=request.extra_args,
                    transfer_config=transfer_config,
                    force=force,
                    size_only=size_only,
                    **kwargs,
                )
            elif isinstance(request, S3UploadRequest):
                upload_file(
                    local_path=request.source_path,
                    s3_path=request.destination_path,
                    extra_args=request.extra_args,
                    transfer_config=transfer_config,
                    force=force,
                    size_only=size_only,
                    **kwargs,
                )
            elif isinstance(request, S3DownloadRequest):
                # This is a special case where s3 object path has a trailing slash.
                # This should be ignored if the object is empty and raise an error otherwise.
                if request.source_path.has_folder_suffix():
                    if get_object(request.source_path, **kwargs).content_length != 0:
                        raise ValueError(
                            "Cannot download S3 object to local path. Downloads of objects "
                            "with '/' suffixed keys and of non-zero size are NOT supported."
                        )
                else:
                    download_s3_object(
                        s3_path=request.source_path,
                        local_path=request.destination_path,
                        transfer_config=transfer_config,
                        force=force,
                        size_only=size_only,
                        **kwargs,
                    )
            transfer_responses.append(S3TransferResponse(request, False))
            logger.info(f"Processed s3 transfer request {i + 1} of {len(transfer_requests)}")
        except Exception as e:
            msg = f"Failed to copy {request.source_path} to {request.destination_path}: {e}"
            if not suppress_errors:
                logger.error(msg)
                logger.exception(msg)
                raise e
            logger.warning(msg)
            transfer_responses.append(S3TransferResponse(request, True, f"{e}"))
    return transfer_responses


# Creating alias to enable easy recursive support
copy_s3_path = sync_paths


def client_error_code_check__SlowDown(ex: Exception) -> bool:
    return isinstance(ex, ClientError) and client_error_code_check(ex, "SlowDown")


@retry(ClientError, [client_error_code_check__SlowDown], tries=10, backoff=2.0)
def copy_s3_object(
    source_path: S3URI,
    destination_path: S3URI,
    extra_args: Optional[Dict[str, Any]] = None,
    transfer_config: Optional[TransferConfig] = None,
    force: bool = True,
    size_only: bool = False,
    **kwargs,
):
    s3 = get_s3_client(**kwargs)
    logger.info(f"S3: Copying {source_path} to {destination_path}")

    # This handles scenario where extra args are specified but destination is more recent
    if extra_args and "Tagging" in extra_args and "TaggingDirective" not in extra_args:
        extra_args["TaggingDirective"] = "REPLACE"
    if extra_args and "Metadata" in extra_args and "MetadataDirective" not in extra_args:
        extra_args["MetadataDirective"] = "REPLACE"

    if force or should_sync(
        source_path=source_path, destination_path=destination_path, size_only=size_only, **kwargs
    ):
        try:
            s3.copy(
                CopySource=source_path.as_dict(),
                Bucket=destination_path.bucket,
                Key=destination_path.key,
                ExtraArgs=extra_args or {},
                Config=transfer_config or TransferConfig(),
            )
        except ClientError as e:
            skippable_err_msg = (
                "This copy request is illegal because it is trying to copy an object "
                "to itself without changing the object's metadata, storage class, "
                "website redirect location or encryption attributes."
            )
            if get_client_error_message(e) != skippable_err_msg:
                raise e

    elif extra_args is not None:
        # This handles scenario where extra args are specified but destination is more recent
        copy_s3_object(
            source_path=destination_path,
            destination_path=destination_path,
            extra_args=extra_args,
            transfer_config=transfer_config,
            force=True,
            **kwargs,
        )


def delete_s3_path(
    s3_path: S3URI,
    include: Optional[List[Pattern]] = None,
    exclude: Optional[List[Pattern]] = None,
    **kwargs,
):
    """Deletes an S3 path (object or prefix)

    Args:
        s3_path (S3URI): Path or key prefix to delete
    """
    logger.info(f"Deleting S3 path {s3_path}")
    s3_paths = list_s3_paths(s3_path, include=include, exclude=exclude, **kwargs)
    delete_s3_objects(s3_paths, **kwargs)


def delete_s3_objects(s3_paths: List[S3URI], **kwargs):
    """Deletes a list of S3 objects

    Args:
        s3_paths (List[S3URI]): List of S3 paths to delete
    """
    logger.info(f"Found {len(s3_paths)} objects to delete.")
    s3 = get_s3_client(**kwargs)

    bucket_objects: Dict[str, Set[str]] = defaultdict(set)
    for s3_path in s3_paths:
        bucket_objects[s3_path.bucket].add(s3_path.key)

    # Can only specify a max of 1000 objects per request.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
    MAX_KEYS_PER_REQUEST = 1000

    for bucket, keys in bucket_objects.items():
        key_list = list(keys)
        for i in range(0, len(keys), MAX_KEYS_PER_REQUEST):
            i_max = min(len(keys), i + MAX_KEYS_PER_REQUEST)
            logger.info(f"Deleting objects {i + 1}..{i_max + 1} in {bucket} bucket")
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key_list[j]} for j in range(i, i_max)]},
            )


def move_s3_path(
    source_path: S3URI,
    destination_path: S3URI,
    include: Optional[List[Pattern]] = None,
    exclude: Optional[List[Pattern]] = None,
    extra_args: Optional[Dict[str, Any]] = None,
    transfer_config: Optional[TransferConfig] = None,
    **kwargs,
):
    """Move S3 Path from source to destination.

    There is no explicit "move" s3 method, so we combine COPY + DELETE operations

    Args:
        source_path (S3URI): source S3 Path
        destination_path (S3URI): destination S3 Path
        transfer_config (Optional[TransferConfig], optional): transfer config.
    """
    logger.info(f"Moving {source_path} to {destination_path}. Starting copy")
    responses = sync_paths(
        source_path=source_path,
        destination_path=destination_path,
        include=include,
        exclude=exclude,
        extra_args=extra_args,
        transfer_config=transfer_config,
        **kwargs,
    )
    logger.info(f"Copy complete. Starting deletion of {source_path}")
    paths_to_delete = [_.request.source_path for _ in responses if not _.failed]
    delete_s3_objects(paths_to_delete, **kwargs)


def list_s3_paths(
    s3_path: S3URI,
    include: Optional[List[Pattern]] = None,
    exclude: Optional[List[Pattern]] = None,
    **kwargs,
) -> List[S3URI]:
    """List all S3 paths under a Key prefix (as defined by S3 path)

    Include/Exclude patterns are applied to the RELATIVE KEY PATH

    Logic for how the include/exclude patterns are applied are as follows:

    - include/exclude: pattern provided? Y/N
    - I/E Match: If pattern provided, does s3 relative Key match? Y/N

    |  include | I Match | exclude | E Match | Append? |
    | ------------- No patterns provided ------------- |
    |     N    |    -    |    N    |    -    |    Y    |
    | ------ Include XOR Exclude pattern provided ---- |
    |     Y    |    Y    |    N    |    -    |    Y    |
    |     Y    |    N    |    N    |    -    |    N    |
    |     N    |    -    |    Y    |    Y    |    N    |
    |     N    |    -    |    Y    |    N    |    Y    |
    | ------ Include AND Exclude pattern provided ---- |
    |     Y    |    Y    |    Y    |    Y    |    N    |
    |     Y    |    N    |    Y    |    Y    |    N    |
    |     Y    |    Y    |    Y    |    N    |    Y    |
    |     Y    |    N    |    Y    |    N    |    N    |

    Args:
        s3_path (S3URI): The Root key path under which to find objects
        include (List[Pattern], optional): Optional list of regex patterns on which
            to retain objects if matching any. Defaults to None.
        exclude (List[Pattern], optional): Optional list of regex patterns on which
            to filter out objects if matching any. Defaults to None.

    Returns:
        List[S3URI]: List of S3 paths under root that satisfy filters
    """

    empty_include = (include is None) or (not any(include))
    empty_exclude = (exclude is None) or (not any(exclude))

    s3 = get_s3_client(**kwargs)

    def match_results(value: str, patterns: List[Pattern]) -> List[bool]:
        return [_.match(value) is not None for _ in patterns]

    paginator = s3.get_paginator("list_objects_v2")

    s3_paths: List[S3URI] = []
    for response in paginator.paginate(Bucket=s3_path.bucket, Prefix=s3_path.key):
        for item in response.get("Contents", []):
            key = item.get("Key", "")
            relative_key = key[len(s3_path.key) :]
            if empty_include or any(match_results(relative_key, include)):  # type: ignore
                if empty_exclude or (not any(match_results(relative_key, exclude))):  # type: ignore
                    s3_paths.append(S3URI.build(bucket_name=s3_path.bucket, key=key))
    return s3_paths


class PresignedUrlAction(Enum):
    READ = "get_object"
    WRITE = "put_object"


def generate_presigned_urls(
    s3_paths: List[S3URI],
    action: PresignedUrlAction = PresignedUrlAction.READ,
    expires_in: int = 3600,
    **kwargs,
) -> List[str]:
    """Generate Pre-signed URLs for given S3 Paths

    Args:
        s3_paths (List[S3URI]): List of S3 Paths to generate URLs for.
        action (PresignedUrlAction): Desired action for presigned URL (READ or WRITE),
            defaults to READ
        expires_in (int, optional): TTL of URL in seconds. Defaults to 3600.

    Returns:
        List[str]: List of pre-signed URLs
    """
    return [generate_presigned_url(s3_path, action, expires_in, **kwargs) for s3_path in s3_paths]


def generate_presigned_url(
    s3_path: S3URI,
    action: PresignedUrlAction = PresignedUrlAction.READ,
    expires_in: int = 3600,
    **kwargs,
) -> str:
    """Generate a Pre-signed URL for an S3 object

    Args:
        s3_path (S3URI): Intended S3 Path of the presigned URL
        action (PresignedUrlAction): Desired action for presigned URL (READ or WRITE),
            defaults to READ
        expires_in (int, optional): TTL of URL in seconds. Defaults to 3600.

    Returns:
        str: A Pre-signed URL
    """
    s3 = get_s3_client(config=Config(signature_version="s3v4"), **kwargs)
    presigned_url = s3.generate_presigned_url(
        ClientMethod=action.value,
        Params={"Bucket": s3_path.bucket, "Key": s3_path.key},
        HttpMethod="GET" if action == PresignedUrlAction.READ else "PUT",
        ExpiresIn=expires_in,
    )
    return presigned_url


def should_sync(
    source_path: Union[Path, S3URI],
    destination_path: Union[Path, S3URI],
    size_only: bool = False,
    **kwargs,
) -> bool:
    """Checks whether transfer from source to destination is required.

    This logic matches the logic in `aws s3 sync` command.

    A transfer from SRC -> DST is necessary if any of the following are true:
        - DST does not exist
        - SRC was last modified more recently than DST
        - SRC size is different than DST
        - not size_only and SRC ETag is differerent than DST

    Args:
        source_path (Union[Path, S3URI]): source path
        destination_path (Union[Path, S3URI]): destination to transfer to
        size_only (bool, optional): Limits content comparison to just size and date (no ETag).
            Defaults to False.

    Raises:
        ValueError: if the source path does not exist.

    Returns:
        bool: True if a transfer is necessary, False, otherwise
    """
    source_last_modified: datetime
    source_size_bytes: int
    source_hash: Optional[Callable[[], str]] = None
    dest_last_modified: Optional[datetime] = None
    dest_size_bytes: Optional[int] = None
    dest_hash: Optional[Callable[[], str]] = None
    multipart_chunk_size_bytes: Optional[int] = None
    multipart_threshold_bytes: Optional[int] = None

    if isinstance(destination_path, S3URI) and is_object(destination_path):
        dest_s3_object = get_object(destination_path, **kwargs)
        dest_last_modified = dest_s3_object.last_modified
        dest_size_bytes = dest_s3_object.content_length
        multipart_chunk_size_bytes, multipart_threshold_bytes = determine_multipart_attributes(
            destination_path, **kwargs
        )
        if not size_only:

            def dest_hash():
                return dest_s3_object.e_tag
    elif isinstance(destination_path, Path) and destination_path.exists():
        dest_local_path = destination_path
        local_stats = dest_local_path.stat()
        dest_last_modified = datetime.fromtimestamp(local_stats.st_mtime, tz=timezone.utc)
        dest_size_bytes = local_stats.st_size
        if not size_only:

            def dest_hash():
                return get_local_etag(
                    dest_local_path, multipart_chunk_size_bytes, multipart_threshold_bytes
                )
    else:
        return True

    if isinstance(source_path, S3URI) and is_object(source_path):
        src_s3_object = get_object(source_path, **kwargs)
        source_last_modified = src_s3_object.last_modified
        source_size_bytes = src_s3_object.content_length
        multipart_chunk_size_bytes, multipart_threshold_bytes = determine_multipart_attributes(
            source_path, **kwargs
        )
        if not size_only:

            def source_hash():
                return src_s3_object.e_tag
    elif isinstance(source_path, Path) and source_path.exists():
        src_local_path = source_path
        local_stats = src_local_path.stat()
        source_last_modified = datetime.fromtimestamp(local_stats.st_mtime, tz=timezone.utc)
        source_size_bytes = local_stats.st_size
        if not size_only:

            def source_hash():
                return get_local_etag(
                    src_local_path, multipart_chunk_size_bytes, multipart_threshold_bytes
                )
    else:
        raise ValueError(
            f"Cannot transfer, source path {source_path} does not exist! "
            f"is s3={isinstance(source_path, S3URI)}, is local={isinstance(source_path, Path)} "
            f"is object={isinstance(source_path, S3URI) and is_object(source_path, **kwargs)}, "
            f"is local exists={isinstance(source_path, Path) and source_path.exists()}, "
            f"type={type(source_path)}"
        )

    if dest_size_bytes is None or dest_last_modified is None:
        return True
    if source_size_bytes != dest_size_bytes:
        return True
    if source_last_modified.replace(microsecond=0) > dest_last_modified.replace(microsecond=0):
        return True
    if not size_only and source_hash and dest_hash and source_hash() != dest_hash():
        return True
    return False


def check_paths_in_sync(
    source_path: Union[Path, S3URI],
    destination_path: Union[Path, S3URI],
    size_only: bool = False,
    **kwargs,
) -> bool:
    """Checks whether source and destination paths are in sync.

    Args:
        source_path (Union[Path, S3URI]): source path
        destination_path (Union[Path, S3URI]): destination path
        size_only (bool, optional): Limits content comparison to just size and date
            (no checksum/ETag). Defaults to False.

    Raises:
        ValueError: if the source path does not exist.

    Returns:
        bool: True if paths are in sync, False, otherwise
    """
    source_paths: Union[List[Path], List[S3URI]] = (
        (
            [Path(p) for p in sorted(find_paths(source_path, include_dirs=False))]
            if source_path.is_dir()
            else [source_path]
        )
        if isinstance(source_path, Path)
        else (
            sorted(list_s3_paths(source_path, **kwargs))  # type: ignore[arg-type]
            if not is_object(source_path)
            else [source_path]  # type: ignore
        )
    )
    destination_paths: Union[List[Path], List[S3URI]] = (
        (
            list(map(Path, sorted(find_paths(destination_path, include_dirs=False))))
            if destination_path.is_dir()
            else [destination_path]
        )
        if isinstance(destination_path, Path)
        else (
            sorted(list_s3_paths(destination_path, **kwargs))  # type: ignore[arg-type]
            if not is_object(destination_path)
            else [destination_path]  # type: ignore
        )
    )
    if len(source_paths) == 0:
        raise ValueError(f"Source path {source_path} does not exist")
    if len(source_paths) != len(destination_paths):
        logger.info(
            "Source and destination paths have different number of paths. "
            f"Source path {source_path} has {len(source_paths)} paths, "
            f"destination path {destination_path} has {len(destination_paths)} paths"
        )
        return False
    for sp, dp in zip(source_paths, destination_paths):
        rsp = strip_path_root(str(sp).removeprefix("s3:"), str(source_path).removeprefix("s3:"))
        rdp = strip_path_root(
            str(dp).removeprefix("s3:"), str(destination_path).removeprefix("s3:")
        )
        if rsp != rdp:
            logger.info(
                f"Source path {sp} (relative={rsp}) does not match "
                f"destination path {dp} (relative={rdp})"
            )
            return False
        if should_sync(source_path=sp, destination_path=dp, size_only=size_only, **kwargs):  # type: ignore[arg-type]  # mypy complains but sp/dp are Path|S3Path
            logger.info(f"Source path {sp} content does not match destination path {dp}")
            return False
    return True


def update_s3_storage_class(
    s3_path: S3URI,
    target_storage_class: S3StorageClass,
) -> bool:
    """Tries to transition an object (or objects) represented by an s3_path to a
    desired target_storage_class.

    NOTE: This function needs to be called again if it returns False.

    Args:
        s3_path (S3URI): The s3_path representing an s3 key or s3 prefix whose object or
            objects should have their storage class updated.
        target_storage_class (S3StorageClass): The target storage class.

    Raises:
        RuntimeError: If an unsupported target_storage_class is provided
        RuntimeError: If the path or paths under the provided root s3_path have a storage class
            that does not support transitions (e.g. S3StorageClass.OUTPOSTS,
            S3StorageClass.REDUCED_REDUNDANCY)

    Returns:
        bool:
            Returns True if the s3_path successfully had its storage class updated
            Returns False if s3_path did not fully update its storage class. Specifically:
            - A constituent object or objects under s3 archive storage class are still restoring
            - A constituent object or objects failed to transition to the desired storage class
    """

    if target_storage_class not in S3StorageClass.list_transitionable_storage_classes():
        raise RuntimeError(
            f"Error trying to update s3 storage class for s3_path ({s3_path}) "
            f"with unsupported target storage class ({target_storage_class.value})."
        )

    s3_paths = list_s3_paths(s3_path=s3_path)

    # 1. Iterate over all s3 paths under our s3_path and determine archive restores to be done.
    #    Also, start any storage class transitions that can be done.
    paths_to_restore: List[S3URI] = []
    paths_restoring: List[S3URI] = []
    failed_transitions: List[S3URI] = []
    for p in s3_paths:
        run_path_transition: bool = False
        s3_obj = get_object(p)
        print(
            f"debug: current storage class: {s3_obj.storage_class}, target: {target_storage_class}"
        )
        current_storage_class = S3StorageClass.from_boto_s3_obj(s3_obj)  # type: ignore[arg-type]
        # Current storage class matches target: No-op
        if current_storage_class == target_storage_class:
            continue
        # Current storage class is archived: Check restore status
        elif current_storage_class in S3StorageClass.list_archive_storage_classes():
            o = S3RestoreStatus.from_raw_s3_restore_status(s3_obj.restore)
            print(
                f"s3 path ({p}), current: {current_storage_class}, "
                f"target: {target_storage_class}, restore status: {o}"
            )
            if o.restore_status == S3RestoreStatusEnum.NOT_STARTED:
                paths_to_restore.append(p)
            elif o.restore_status == S3RestoreStatusEnum.IN_PROGRESS:
                paths_restoring.append(p)
            elif o.restore_status == S3RestoreStatusEnum.FINISHED:
                run_path_transition = True
        # Current storage class does not match target: Needs transition
        elif current_storage_class in S3StorageClass.list_transitionable_storage_classes():
            run_path_transition = True
        # Current storage class cannot be transitioned
        else:
            raise RuntimeError(
                f"Error trying to update the s3 storage class for s3_path ({p}) "
                f"which has an unsupported current storage class: {current_storage_class}"
            )

        if run_path_transition:
            try:
                copy_s3_object(
                    source_path=p,
                    destination_path=p,
                    extra_args={"StorageClass": target_storage_class.value},
                )
            except ClientError as e:
                logger.error(
                    f"Failed to transition s3 path ({p}) to storage "
                    f"class ({target_storage_class.value}). Error details: {e}"
                )
                failed_transitions.append(p)

    # 2. Start off any restores that need to be done
    for p in paths_to_restore:
        s3_obj = get_object(p)
        s3_obj.restore_object(
            RestoreRequest={
                "Days": 2,
                "GlacierJobParameters": {"Tier": "Standard"},
            }
        )

    # 3. If there are restores (started or in progress) or failed transitions for objects
    #    under our `s3_path` return False and we'll need to call update_s3_storage class in
    #    the future again.
    if any(paths_to_restore) or any(paths_restoring):
        return False

    if any(failed_transitions):
        logger.warning(
            "The following paths failed to transition to the target_storage_class "
            f"({target_storage_class}): {failed_transitions}"
        )
        return False

    return True


# --------------------------------------------------------------------
#                               Helpers
# --------------------------------------------------------------------
def _get_prefix_size_and_count(bucket_name: str, key_prefix: str, **kwargs) -> Tuple[int, int]:
    s3 = get_s3_resource(**kwargs)
    bucket = s3.Bucket(bucket_name)
    object_count = 0
    total_size = 0
    for obj in bucket.objects.filter(Prefix=key_prefix):
        total_size += obj.size
        object_count += 1
    return total_size, object_count


def _get_prefix_last_modified(bucket_name: str, key_prefix: str, **kwargs) -> datetime:
    s3 = get_s3_resource(**kwargs)
    bucket = s3.Bucket(bucket_name)
    last_modified = None
    for obj in bucket.objects.filter(Prefix=key_prefix):
        if not last_modified or obj.last_modified > last_modified:
            last_modified = obj.last_modified
    assert isinstance(last_modified, datetime)  # mollify mypy
    return last_modified


def determine_multipart_attributes(
    s3_path: S3URI, **kwargs
) -> Tuple[Optional[int], Optional[int]]:
    """Determines multipart upload chunk size and approximate threshold, if applicable

    Multipart attributes are the following:
        - chunk size: The size of each part in bytes
        - threshold: The size in bytes at which a multipart upload is created

    Notes:
        - The threshold is only an approximation, as it is not possible to
          determine the exact threshold used.
        - This assumes chunk size is constant for all parts.
          This is not necessarily always true, although rare.
        - The chunksize for a multipart upload has the following constraints:
            https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
            - Most importantly, must be between 5MB and 5GB
    Args:
        s3_path (S3URI): S3 object to check

    Returns:
        Tuple[Optional[int], Optional[int]]: (chunk_size, threshold)
    """
    s3_client = get_s3_client(**kwargs)
    head_object_part = s3_client.head_object(Bucket=s3_path.bucket, Key=s3_path.key, PartNumber=1)

    object_parts = head_object_part.get("PartsCount", 1)
    object_part_content_length = head_object_part.get("ContentLength", 0)

    threshold, chunk_size = None, None
    if object_parts > 1:
        threshold, chunk_size = object_part_content_length, object_part_content_length
    else:
        head_object = s3_client.head_object(Bucket=s3_path.bucket, Key=s3_path.key)
        is_multipart_etag = head_object["ETag"].endswith('-1"')
        if is_multipart_etag:
            # This should ensure that multipart etag is created as expected
            threshold = object_part_content_length
        if object_part_content_length > AWS_S3_DEFAULT_CHUNK_SIZE_BYTES:
            chunk_size = object_part_content_length
            if not is_multipart_etag:
                chunk_size += 1
                threshold = chunk_size
    return chunk_size, threshold


def determine_chunk_size(
    path: Path, default_chunk_size_bytes: int = AWS_S3_DEFAULT_CHUNK_SIZE_BYTES
) -> int:
    """Function to determine the chunk size that `aws s3 cp` would use for a very large file"""
    file_size = path.stat().st_size
    correct_chunk_size_bytes = default_chunk_size_bytes
    while math.ceil(file_size / correct_chunk_size_bytes) > AWS_S3_MULTIPART_LIMIT:
        correct_chunk_size_bytes *= 2
    return correct_chunk_size_bytes


@retry(OSError)
def get_local_etag(
    path: Path, chunk_size_bytes: Optional[int] = None, threshold_bytes: Optional[int] = None
) -> str:
    """Calculates an expected AWS s3 upload etag for a local on-disk file.
    Takes into account multipart uploads, but does NOT account for additional encryption
    (like KMS keys)

    Args:
        path (Path): The path of the file that will be uploaded to s3
        chunk_size (int): The default multipart upload chunksize in bytes.
            If None, we determine the chunk size based on file size

    Returns:
        str: The expected etag
    """
    if chunk_size_bytes is None:
        chunk_size_bytes = determine_chunk_size(path)

    if threshold_bytes is None:
        threshold_bytes = determine_chunk_size(path)

    size_bytes = path.stat().st_size

    md5s = []

    with open(path, "rb") as fp:
        while True:
            data = fp.read(chunk_size_bytes)
            if not data:
                break
            md5s.append(hashlib.md5(data))

    if len(md5s) == 0:  # Empty file (can never be multipart upload)
        expected_etag = f'"{hashlib.md5().hexdigest()}"'
    elif len(md5s) == 1 and size_bytes < threshold_bytes:  # File smaller than threshold
        expected_etag = f'"{md5s[0].hexdigest()}"'
    else:  # We are dealing with a multipart upload
        digests = b"".join(m.digest() for m in md5s)
        multipart_md5 = hashlib.md5(digests)
        expected_etag = f'"{multipart_md5.hexdigest()}-{len(md5s)}"'

    return expected_etag
