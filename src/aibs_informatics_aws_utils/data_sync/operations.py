import functools
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union, cast

from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3URI, S3KeyPrefix
from aibs_informatics_core.models.data_sync import (
    DataSyncConfig,
    DataSyncRequest,
    DataSyncResult,
    DataSyncTask,
    RemoteToLocalConfig,
)
from aibs_informatics_core.utils.decorators import retry
from aibs_informatics_core.utils.file_operations import (
    CannotAcquirePathLockError,
    PathLock,
    copy_path,
    find_filesystem_boundary,
    get_path_size_bytes,
    move_path,
    remove_path,
)
from aibs_informatics_core.utils.logging import LoggingMixin, get_logger
from aibs_informatics_core.utils.os_operations import find_all_paths
from botocore.client import Config

from aibs_informatics_aws_utils.efs import get_local_path
from aibs_informatics_aws_utils.s3 import (
    TransferConfig,
    delete_s3_path,
    get_s3_path_stats,
    is_folder,
    is_object,
    sync_paths,
)

logger = get_logger(__name__)


MAX_LOCK_WAIT_TIME_IN_SECS = 60 * 60 * 6  # 6 hours

LOCK_ROOT_ENV_VAR = "DATA_SYNC_LOCK_ROOT"

LocalPath = Union[Path, EFSPath]


@functools.cache
def get_botocore_config(max_pool_connections: int, **kwargs) -> Config:
    return Config(max_pool_connections=max_pool_connections, **kwargs)


@dataclass
class DataSyncOperations(LoggingMixin):
    config: DataSyncConfig

    @property
    def s3_transfer_config(self) -> TransferConfig:
        return TransferConfig(max_concurrency=self.config.max_concurrency)

    @property
    def botocore_config(self) -> Config:
        return get_botocore_config(max_pool_connections=self.config.max_concurrency)

    def sync_local_to_s3(self, source_path: LocalPath, destination_path: S3URI) -> DataSyncResult:
        source_path = self.sanitize_local_path(source_path)
        if not source_path.exists():
            if self.config.fail_if_missing:
                raise FileNotFoundError(f"Local path {source_path} does not exist")
            self.logger.warning(f"Local path {source_path} does not exist")
            if self.config.include_detailed_response:
                return DataSyncResult(bytes_transferred=0, files_transferred=0)
            else:
                return DataSyncResult()
        if source_path.is_dir():
            self.logger.info("local source path is folder. Adding suffix to destination path")
            destination_path = S3URI.build(
                bucket_name=destination_path.bucket_name,
                key=destination_path.key_with_folder_suffix,
            )
        self.logger.info(f"Uploading local content from {source_path} -> {destination_path}")
        sync_paths(
            source_path=source_path,
            destination_path=destination_path,
            transfer_config=self.s3_transfer_config,
            config=self.botocore_config,
            force=self.config.force,
            size_only=self.config.size_only,
            delete=True,
        )
        result = DataSyncResult()
        if self.config.include_detailed_response:
            result.files_transferred = len(find_all_paths(source_path, include_dirs=False))
            result.bytes_transferred = get_path_size_bytes(source_path)
        if not self.config.retain_source_data:
            remove_path(source_path)
        return result

    def sync_s3_to_local(self, source_path: S3URI, destination_path: LocalPath) -> DataSyncResult:
        self.logger.info(f"Downloading s3 content from {source_path} -> {destination_path}")
        start_time = datetime.now(tz=timezone.utc)
        destination_path = self.sanitize_local_path(destination_path)
        source_is_object = is_object(source_path)

        if not source_is_object and not is_folder(source_path):
            message = f"S3 path {source_path} does not exist as object or folder"
            if self.config.fail_if_missing:
                raise FileNotFoundError(message)
            self.logger.warning(message)
            if self.config.include_detailed_response:
                return DataSyncResult(bytes_transferred=0, files_transferred=0)
            else:
                return DataSyncResult()

        _sync_paths = sync_paths

        if self.config.require_lock:
            delay = 5
            tries = MAX_LOCK_WAIT_TIME_IN_SECS // delay
            self.logger.info(
                f"File lock required for transfer. Will attempt to aquire lock {tries} times, "
                f"with {delay} sec delays between attempts. "
            )

            @retry(CannotAcquirePathLockError, tries=tries, delay=delay, backoff=1)
            @functools.wraps(sync_paths)
            def sync_paths_with_lock(*args, **kwargs):
                with PathLock(destination_path, lock_root=os.getenv(LOCK_ROOT_ENV_VAR)):
                    response = sync_paths(*args, **kwargs)
                return response

            _sync_paths = sync_paths_with_lock

        remote_to_local_config = self.config.remote_to_local_config
        if source_is_object and remote_to_local_config.use_custom_tmp_dir:
            # If our source is an s3 object (not prefix) and we want to use custom object
            # download logic (default False), then we save s3 objects to a temporary location
            # that is on the SAME file system.
            #
            # This is necessary because if the normal boto3 download gets interrupted in a
            # catastrophic way that prevents built-in cleanup strategies, it leaves
            # a 'partial' file (e.g. `*.6eF5b5da`) that resides in the SAME parent directory
            # as the actual intended destination path. This 'partial' file can be picked up by
            # some scientific executables (e.g. cellranger) and interpreted as an invalid input
            if remote_to_local_config.custom_tmp_dir is None:
                custom_tmp_dir = find_filesystem_boundary(destination_path)
            elif isinstance(remote_to_local_config.custom_tmp_dir, EFSPath):
                custom_tmp_dir = self.sanitize_local_path(remote_to_local_config.custom_tmp_dir)
            else:
                custom_tmp_dir = remote_to_local_config.custom_tmp_dir

            with tempfile.TemporaryDirectory(dir=custom_tmp_dir) as tmp_dir:
                tmp_destination_path = Path(tmp_dir) / destination_path.name
                _sync_paths(
                    source_path=source_path,
                    destination_path=tmp_destination_path,
                    transfer_config=self.s3_transfer_config,
                    config=self.botocore_config,
                    force=self.config.force,
                    size_only=self.config.size_only,
                )
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                os.rename(src=tmp_destination_path, dst=destination_path)
        else:
            # If our source is a prefix, then _sync_paths has builtin logic to deal with deleting
            # excess files in the destination dir that do not match the source prefix layout.
            _sync_paths(
                source_path=source_path,
                destination_path=destination_path,
                transfer_config=self.s3_transfer_config,
                config=self.botocore_config,
                force=self.config.force,
                size_only=self.config.size_only,
                delete=True,
            )

        self.logger.info(f"Updating last modified time on local files to at least {start_time}")
        refresh_local_path__mtime(destination_path, start_time.timestamp())

        if not self.config.retain_source_data:
            # TODO: maybe tag for deletion
            self.logger.warning(
                "Deleting s3 objects not allowed when downloading them to local file system"
            )

        result = DataSyncResult()
        # Collecting stats for detailed response
        if self.config.include_detailed_response:
            result.files_transferred = len(find_all_paths(destination_path, include_dirs=False))
            result.bytes_transferred = get_path_size_bytes(destination_path)
        return result

    def sync_local_to_local(
        self, source_path: LocalPath, destination_path: LocalPath
    ) -> DataSyncResult:
        source_path = self.sanitize_local_path(source_path)
        destination_path = self.sanitize_local_path(destination_path)
        self.logger.info(f"Copying local content from {source_path} -> {destination_path}")
        start_time = datetime.now(tz=timezone.utc)

        if not source_path.exists():
            if self.config.fail_if_missing:
                raise FileNotFoundError(f"Local path {source_path} does not exist")
            self.logger.warning(f"Local path {source_path} does not exist")
            return DataSyncResult(bytes_transferred=0)

        if self.config.retain_source_data:
            copy_path(source_path=source_path, destination_path=destination_path, exists_ok=True)
        else:
            move_path(source_path=source_path, destination_path=destination_path, exists_ok=True)
        self.logger.info(f"Updating last modified time on local files to at least {start_time}")
        refresh_local_path__mtime(destination_path, start_time.timestamp())

        result = DataSyncResult()
        # Collecting stats for detailed response
        if self.config.include_detailed_response:
            result.files_transferred = len(find_all_paths(source_path, include_dirs=False))
            result.bytes_transferred = get_path_size_bytes(source_path)
        return result

    def sync_s3_to_s3(
        self,
        source_path: S3URI,
        destination_path: S3URI,
        source_path_prefix: Optional[S3KeyPrefix] = None,
    ) -> DataSyncResult:
        self.logger.info(f"Syncing s3 content from {source_path} -> {destination_path}")

        if not is_object(source_path) and not is_folder(source_path):
            message = f"S3 path {source_path} does not exist as object or folder"
            if self.config.fail_if_missing:
                raise FileNotFoundError(message)
            self.logger.warning(message)
            if self.config.include_detailed_response:
                return DataSyncResult(bytes_transferred=0, files_transferred=0)
            else:
                return DataSyncResult()

        sync_paths(
            source_path=source_path,
            destination_path=destination_path,
            source_path_prefix=source_path_prefix,
            transfer_config=self.s3_transfer_config,
            config=self.botocore_config,
            force=self.config.force,
            size_only=self.config.size_only,
            delete=True,
        )
        if not self.config.retain_source_data:
            delete_s3_path(s3_path=source_path)

        result = DataSyncResult()
        if self.config.include_detailed_response:
            path_stats = get_s3_path_stats(destination_path)
            result.files_transferred = path_stats.object_count or 0
            result.bytes_transferred = path_stats.size_bytes
        return result

    def sync(
        self,
        source_path: Union[LocalPath, S3URI],
        destination_path: Union[LocalPath, S3URI],
        source_path_prefix: Optional[str] = None,
    ) -> DataSyncResult:
        if isinstance(source_path, S3URI) and isinstance(destination_path, S3URI):
            return self.sync_s3_to_s3(
                source_path=source_path,
                destination_path=destination_path,
                source_path_prefix=S3KeyPrefix(source_path_prefix) if source_path_prefix else None,
            )

        elif isinstance(source_path, S3URI):
            return self.sync_s3_to_local(
                source_path=source_path,
                destination_path=cast(LocalPath, destination_path),
            )
        elif isinstance(destination_path, S3URI):
            return self.sync_local_to_s3(
                source_path=cast(LocalPath, source_path),
                destination_path=destination_path,
            )
        else:
            return self.sync_local_to_local(
                source_path=source_path,
                destination_path=destination_path,
            )

    def sync_task(self, task: DataSyncTask) -> DataSyncResult:
        return self.sync(
            source_path=task.source_path,
            destination_path=task.destination_path,
            source_path_prefix=task.source_path_prefix,
        )

    @classmethod
    def sync_request(cls, request: DataSyncRequest) -> DataSyncResult:
        sync_operations = cls(config=request.config)
        return sync_operations.sync_task(task=request.task)

    # -----------------------------------
    # Helper methods
    # -----------------------------------

    def sanitize_local_path(self, path: Union[EFSPath, Path]) -> Path:
        if isinstance(path, EFSPath):
            self.logger.info(f"Sanitizing efs path {path}")
            new_path = get_local_path(path, raise_if_unmounted=True)
            self.logger.info(f"Sanitized efs path -> {new_path}")
            return new_path
        return path


# We should consider using cloudpathlib[s3] in the future
def sync_data(
    source_path: Union[S3URI, LocalPath],
    destination_path: Union[S3URI, LocalPath],
    source_path_prefix: Optional[str] = None,
    max_concurrency: int = 10,
    retain_source_data: bool = True,
    require_lock: bool = False,
    force: bool = False,
    size_only: bool = False,
    fail_if_missing: bool = True,
    remote_to_local_config: Optional[RemoteToLocalConfig] = None,
    include_detailed_response: bool = False,
):
    request = DataSyncRequest(
        source_path=source_path,
        destination_path=destination_path,
        source_path_prefix=S3KeyPrefix(source_path_prefix) if source_path_prefix else None,
        max_concurrency=max_concurrency,
        retain_source_data=retain_source_data,
        require_lock=require_lock,
        force=force,
        size_only=size_only,
        fail_if_missing=fail_if_missing,
        remote_to_local_config=remote_to_local_config or RemoteToLocalConfig(),
        include_detailed_response=include_detailed_response,
    )
    return DataSyncOperations.sync_request(request=request)


def refresh_local_path__mtime(path: Path, min_mtime: Union[int, float]):
    paths = find_all_paths(path, include_dirs=False, include_files=True)
    for subpath in paths:
        path_stats = os.stat(subpath)
        if path_stats.st_mtime < min_mtime:
            os.utime(subpath, times=(path_stats.st_atime, min_mtime))
