import functools
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

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

from aibs_informatics_aws_utils.efs import get_efs_mount_path
from aibs_informatics_aws_utils.s3 import Config, TransferConfig, delete_s3_path, sync_paths

logger = get_logger(__name__)


MAX_LOCK_WAIT_TIME_IN_SECS = 60 * 60 * 6  # 6 hours


@dataclass
class DataSyncOperations(LoggingMixin):
    config: DataSyncConfig

    @property
    def s3_transfer_config(self) -> TransferConfig:
        return TransferConfig(max_concurrency=self.config.max_concurrency)

    @property
    def botocore_config(self) -> Config:
        return Config(max_pool_connections=self.config.max_concurrency)

    def get_local_root(self) -> Optional[Path]:
        try:
            return Path(get_efs_mount_path())
        except ValueError:
            self.logger.info("No local root configured.")
            return None

    def sync_local_to_s3(self, source_path: Path, destination_path: S3URI):
        local_root = self.get_local_root()
        source_path = sanitize_local_path(local_root=local_root, local_path=source_path)
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
            force=False,
            delete=True,
        )
        if not self.config.retain_source_data:
            remove_path(source_path)

    def sync_s3_to_local(self, source_path: S3URI, destination_path: Path):
        local_root = self.get_local_root()
        destination_path = sanitize_local_path(local_root=local_root, local_path=destination_path)

        self.logger.info(f"Downloading s3 content from {source_path} -> {destination_path}")
        start_time = datetime.now(tz=timezone.utc)

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

        _sync_paths(
            source_path=source_path,
            destination_path=destination_path,
            transfer_config=self.s3_transfer_config,
            config=self.botocore_config,
            force=False,
            delete=True,
        )

        self.logger.info(f"Updating last modified time on local files to at least {start_time}")
        refresh_local_path__mtime(destination_path, start_time.timestamp())

        if not self.config.retain_source_data:
            # TODO: maybe tag for deletion
            self.logger.warning(
                "Deleting s3 objects not allowed when downloading them to local file system"
            )

    def sync_local_to_local(self, source_path: Path, destination_path: Path):
        local_root = self.get_local_root()
        source_path = sanitize_local_path(local_root=local_root, local_path=source_path)
        destination_path = sanitize_local_path(local_root=local_root, local_path=destination_path)
        self.logger.info(f"Copying local content from {source_path} -> {destination_path}")
        start_time = datetime.now(tz=timezone.utc)
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
        sync_paths(
            source_path=source_path,
            destination_path=destination_path,
            source_path_prefix=source_path_prefix,
            transfer_config=self.s3_transfer_config,
            config=self.botocore_config,
            force=False,
            delete=True,
        )
        if not self.config.retain_source_data:
            delete_s3_path(s3_path=source_path)

    def sync(
        self,
        source_path: Union[Path, S3URI],
        destination_path: Union[Path, S3URI],
        source_path_prefix: Optional[str] = None,
    ):
        if isinstance(source_path, S3URI) and isinstance(destination_path, S3URI):
            self.sync_s3_to_s3(
                source_path=source_path,
                destination_path=destination_path,
                source_path_prefix=S3KeyPrefix(source_path_prefix) if source_path_prefix else None,
            )

        elif isinstance(source_path, S3URI) and isinstance(destination_path, Path):
            self.sync_s3_to_local(
                source_path=source_path,
                destination_path=destination_path,
            )
        elif isinstance(source_path, Path) and isinstance(destination_path, S3URI):
            self.sync_local_to_s3(
                source_path=source_path,
                destination_path=destination_path,
            )
        elif isinstance(source_path, Path) and isinstance(destination_path, Path):
            self.sync_local_to_local(
                source_path=source_path,
                destination_path=destination_path,
            )
        else:
            raise ValueError(
                f"could not execute transfer from {source_path} -> {destination_path}"
            )

    def sync_task(self, task: DataSyncTask):
        return self.sync(
            source_path=task.source_path,
            destination_path=task.destination_path,
            source_path_prefix=task.source_path_prefix,
        )

    @classmethod
    def sync_request(cls, request: DataSyncRequest):
        sync_operations = cls(request)
        sync_operations.sync_task(task=request)


# We should consider using cloudpathlib[s3] in the future
def sync_data(
    source_path: Union[S3URI, Path],
    destination_path: Union[S3URI, Path],
    source_path_prefix: Optional[str] = None,
    max_concurrency: int = 10,
    retain_source_data: bool = True,
    require_lock: bool = False,
):
    request = DataSyncRequest(
        source_path=source_path,
        destination_path=destination_path,
        source_path_prefix=source_path_prefix,
        max_concurrency=max_concurrency,
        retain_source_data=retain_source_data,
        require_lock=require_lock,
    )
    return DataSyncOperations.sync_request(request=request)


def sanitize_local_path(local_path: Path, local_root: Optional[Path]) -> Path:
    """Sanitize local path based on specification of optional local root directory

    If local root is provided, local path is prefixed.

    Notes:

    - If local path is absolute, local root MUST MATCH local path prefix.

    Args:
        local_path (Path): _description_
        local_root (Optional[Path]): optionally specified local root

    Returns:
        Path: _description_
    """

    if local_root:
        logger.info(f"Sanitizing local path {local_path} using local root {local_root}")
        if local_path.is_absolute():
            logger.info(f"local path is absolute. finding relative path to local root")
            local_path = local_path.relative_to(local_root)

        local_path = local_root / local_path
        logger.info(f"Sanitized local path -> {local_path}")
    return local_path


def refresh_local_path__mtime(path: Path, min_mtime: Union[int, float]):
    paths = find_all_paths(path, include_dirs=False, include_files=True)
    for path in paths:
        path_stats = os.stat(path)
        if path_stats.st_mtime < min_mtime:
            os.utime(path, times=(path_stats.st_atime, min_mtime))
