import functools
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union, cast

from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3URI, S3KeyPrefix
from aibs_informatics_core.models.data_sync import DataSyncConfig, DataSyncRequest, DataSyncTask
from aibs_informatics_core.utils.decorators import retry
from aibs_informatics_core.utils.file_operations import (
    CannotAcquirePathLockError,
    PathLock,
    copy_path,
    move_path,
    remove_path,
)
from aibs_informatics_core.utils.logging import LoggingMixin, get_logger
from aibs_informatics_core.utils.os_operations import find_all_paths

from aibs_informatics_aws_utils.efs import get_local_path
from aibs_informatics_aws_utils.s3 import (
    Config,
    TransferConfig,
    delete_s3_path,
    is_folder,
    is_object,
    sync_paths,
)

logger = get_logger(__name__)


MAX_LOCK_WAIT_TIME_IN_SECS = 60 * 60 * 6  # 6 hours


LocalPath = Union[Path, EFSPath]


@dataclass
class DataSyncOperations(LoggingMixin):
    config: DataSyncConfig

    @property
    def s3_transfer_config(self) -> TransferConfig:
        return TransferConfig(max_concurrency=self.config.max_concurrency)

    @property
    def botocore_config(self) -> Config:
        return Config(max_pool_connections=self.config.max_concurrency)

    def sync_local_to_s3(self, source_path: LocalPath, destination_path: S3URI):
        source_path = self.sanitize_local_path(source_path)
        if not source_path.exists():
            if self.config.fail_if_missing:
                raise FileNotFoundError(f"Local path {source_path} does not exist")
            self.logger.warning(f"Local path {source_path} does not exist")
            return
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
        if not self.config.retain_source_data:
            remove_path(source_path)

    def sync_s3_to_local(self, source_path: S3URI, destination_path: LocalPath):
        self.logger.info(f"Downloading s3 content from {source_path} -> {destination_path}")
        start_time = datetime.now(tz=timezone.utc)

        if not is_object(source_path) and not is_folder(source_path):
            message = f"S3 path {source_path} does not exist as object or folder"
            if self.config.fail_if_missing:
                raise FileNotFoundError(message)
            self.logger.warning(message)
            return

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
                with PathLock(destination_path) as lock:
                    response = sync_paths(*args, **kwargs)
                return response

            _sync_paths = sync_paths_with_lock

        destination_path = self.sanitize_local_path(destination_path)

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

    def sync_local_to_local(self, source_path: LocalPath, destination_path: LocalPath):
        source_path = self.sanitize_local_path(source_path)
        destination_path = self.sanitize_local_path(destination_path)
        self.logger.info(f"Copying local content from {source_path} -> {destination_path}")
        start_time = datetime.now(tz=timezone.utc)

        if not source_path.exists():
            if self.config.fail_if_missing:
                raise FileNotFoundError(f"Local path {source_path} does not exist")
            self.logger.warning(f"Local path {source_path} does not exist")
            return

        if self.config.retain_source_data:
            copy_path(source_path=source_path, destination_path=destination_path, exists_ok=True)
        else:
            move_path(source_path=source_path, destination_path=destination_path, exists_ok=True)
        self.logger.info(f"Updating last modified time on local files to at least {start_time}")
        refresh_local_path__mtime(destination_path, start_time.timestamp())

    def sync_s3_to_s3(
        self,
        source_path: S3URI,
        destination_path: S3URI,
        source_path_prefix: Optional[S3KeyPrefix] = None,
    ):
        self.logger.info(f"Syncing s3 content from {source_path} -> {destination_path}")

        if not is_object(source_path) and not is_folder(source_path):
            message = f"S3 path {source_path} does not exist as object or folder"
            if self.config.fail_if_missing:
                raise FileNotFoundError(message)
            self.logger.warning(message)
            return

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

    def sync(
        self,
        source_path: Union[LocalPath, S3URI],
        destination_path: Union[LocalPath, S3URI],
        source_path_prefix: Optional[str] = None,
    ):
        if isinstance(source_path, S3URI) and isinstance(destination_path, S3URI):
            self.sync_s3_to_s3(
                source_path=source_path,
                destination_path=destination_path,
                source_path_prefix=S3KeyPrefix(source_path_prefix) if source_path_prefix else None,
            )

        elif isinstance(source_path, S3URI):
            self.sync_s3_to_local(
                source_path=source_path,
                destination_path=cast(LocalPath, destination_path),
            )
        elif isinstance(destination_path, S3URI):
            self.sync_local_to_s3(
                source_path=cast(LocalPath, source_path),
                destination_path=destination_path,
            )
        else:
            self.sync_local_to_local(
                source_path=source_path,
                destination_path=destination_path,
            )

    def sync_task(self, task: DataSyncTask):
        return self.sync(
            source_path=task.source_path,
            destination_path=task.destination_path,
            source_path_prefix=task.source_path_prefix,
        )

    def sanitize_local_path(self, path: Union[EFSPath, Path]) -> Path:
        if isinstance(path, EFSPath):
            self.logger.info(f"Sanitizing efs path {path}")
            new_path = get_local_path(path, raise_if_unmounted=True)
            self.logger.info(f"Sanitized efs path -> {new_path}")
            return new_path
        return path

    @classmethod
    def sync_request(cls, request: DataSyncRequest):
        sync_operations = cls(request)
        sync_operations.sync_task(task=request)


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
    )
    return DataSyncOperations.sync_request(request=request)


def refresh_local_path__mtime(path: Path, min_mtime: Union[int, float]):
    paths = find_all_paths(path, include_dirs=False, include_files=True)
    for subpath in paths:
        path_stats = os.stat(subpath)
        if path_stats.st_mtime < min_mtime:
            os.utime(subpath, times=(path_stats.st_atime, min_mtime))
