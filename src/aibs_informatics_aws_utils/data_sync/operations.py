import functools
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Union, cast

from aibs_informatics_core.models.aws.efs import EFSPath
from aibs_informatics_core.models.aws.s3 import S3KeyPrefix, S3Path
from aibs_informatics_core.models.data_sync import (
    DataSyncConfig,
    DataSyncFilterConfig,
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
from aibs_informatics_core.utils.filters import filter_paths
from aibs_informatics_core.utils.logging import LoggingMixin, get_logger
from aibs_informatics_core.utils.os_operations import find_all_paths
from botocore.client import Config

from aibs_informatics_aws_utils.data_sync._filters import extract_filter_patterns
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

    def sync_local_to_s3(
        self,
        source_path: LocalPath,
        destination_path: S3Path,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
    ) -> DataSyncResult:
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
            destination_path = S3Path.build(
                bucket_name=destination_path.bucket_name,
                key=destination_path.key_with_folder_suffix,
            )
        self.logger.info(f"Uploading local content from {source_path} -> {destination_path}")
        include, exclude = extract_filter_patterns(filter_config)
        sync_paths(
            source_path=source_path,
            destination_path=destination_path,
            include=include,
            exclude=exclude,
            filter_root=filter_root,
            transfer_config=self.s3_transfer_config,
            config=self.botocore_config,
            force=self.config.force,
            size_only=self.config.size_only,
            delete=self.config.delete,
        )
        result = DataSyncResult()
        if self.config.include_detailed_response:
            # Counted over the *filtered* source. Measuring the whole source here
            # would report everything the caller pointed at rather than what the
            # sync actually moved, which with filters is usually far less.
            transferred_paths = filter_paths(
                find_all_paths(source_path, include_dirs=False),
                root=filter_root if filter_root is not None else str(source_path),
                include=include,
                exclude=exclude,
            )
            result.files_transferred = len(transferred_paths)
            result.bytes_transferred = sum(get_path_size_bytes(Path(p)) for p in transferred_paths)
        if not self.config.retain_source_data:
            remove_path(source_path)
        return result

    def sync_s3_to_local(
        self,
        source_path: S3Path,
        destination_path: LocalPath,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
    ) -> DataSyncResult:
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

        include, exclude = extract_filter_patterns(filter_config)

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
                    include=include,
                    exclude=exclude,
                    filter_root=filter_root,
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
                include=include,
                exclude=exclude,
                filter_root=filter_root,
                transfer_config=self.s3_transfer_config,
                config=self.botocore_config,
                force=self.config.force,
                size_only=self.config.size_only,
                delete=self.config.delete,
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
        self,
        source_path: LocalPath,
        destination_path: LocalPath,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
    ) -> DataSyncResult:
        source_path = self.sanitize_local_path(source_path)
        destination_path = self.sanitize_local_path(destination_path)

        # TODO: implement include/exclude filtering for local -> local sync.
        #   Unlike the other three directions, this one does not go through
        #   `sync_paths`; it delegates to `copy_path`/`move_path`, which copy a
        #   tree wholesale and take no patterns. Supporting filters here means
        #   either teaching those helpers about patterns or walking the source
        #   and transferring file by file. Until then a filtered local -> local
        #   sync copies *everything*, so warn loudly rather than let a silently
        #   complete full copy look like a successful filtered one.
        if filter_config is not None and (filter_config.include or filter_config.exclude):
            self.logger.warning(
                f"Include/exclude filters are NOT supported for local -> local sync "
                f"({source_path} -> {destination_path}). "
                f"include={filter_config.include}, exclude={filter_config.exclude} "
                f"will be IGNORED and the full source will be copied."
            )

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
        source_path: S3Path,
        destination_path: S3Path,
        source_path_prefix: S3KeyPrefix | None = None,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
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

        include, exclude = extract_filter_patterns(filter_config)
        sync_paths(
            source_path=source_path,
            destination_path=destination_path,
            source_path_prefix=source_path_prefix,
            include=include,
            exclude=exclude,
            filter_root=filter_root,
            transfer_config=self.s3_transfer_config,
            config=self.botocore_config,
            force=self.config.force,
            size_only=self.config.size_only,
            delete=self.config.delete,
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
        source_path: LocalPath | S3Path,
        destination_path: LocalPath | S3Path,
        source_path_prefix: str | None = None,
        filter_config: DataSyncFilterConfig | None = None,
        filter_root: str | None = None,
    ) -> DataSyncResult:
        if isinstance(source_path, S3Path) and isinstance(destination_path, S3Path):
            return self.sync_s3_to_s3(
                source_path=source_path,
                destination_path=destination_path,
                source_path_prefix=S3KeyPrefix(source_path_prefix) if source_path_prefix else None,
                filter_config=filter_config,
                filter_root=filter_root,
            )

        elif isinstance(source_path, S3Path):
            return self.sync_s3_to_local(
                source_path=source_path,
                destination_path=cast(LocalPath, destination_path),
                filter_config=filter_config,
                filter_root=filter_root,
            )
        elif isinstance(destination_path, S3Path):
            return self.sync_local_to_s3(
                source_path=cast(LocalPath, source_path),
                destination_path=destination_path,
                filter_config=filter_config,
                filter_root=filter_root,
            )
        else:
            return self.sync_local_to_local(
                source_path=source_path,
                destination_path=destination_path,
                filter_config=filter_config,
                filter_root=filter_root,
            )

    def sync_task(self, task: DataSyncTask) -> DataSyncResult:
        return self.sync(
            source_path=task.source_path,
            destination_path=task.destination_path,
            source_path_prefix=task.source_path_prefix,
            filter_config=task.filter_config,
            filter_root=task.filter_root,
        )

    @classmethod
    def sync_request(cls, request: DataSyncRequest) -> DataSyncResult:
        sync_operations = cls(config=request.config)
        return sync_operations.sync_task(task=request.task)

    # -----------------------------------
    # Helper methods
    # -----------------------------------

    def sanitize_local_path(self, path: EFSPath | Path) -> Path:
        if isinstance(path, EFSPath):
            self.logger.info(f"Sanitizing efs path {path}")
            new_path = get_local_path(path, raise_if_unmounted=True)
            self.logger.info(f"Sanitized efs path -> {new_path}")
            return new_path
        return path


# We should consider using cloudpathlib[s3] in the future
def sync_data(
    source_path: S3Path | LocalPath,
    destination_path: S3Path | LocalPath,
    source_path_prefix: str | None = None,
    filter_config: DataSyncFilterConfig | None = None,
    filter_root: str | None = None,
    max_concurrency: int = 10,
    retain_source_data: bool = True,
    delete: bool = True,
    require_lock: bool = False,
    force: bool = False,
    size_only: bool = False,
    fail_if_missing: bool = True,
    remote_to_local_config: RemoteToLocalConfig | None = None,
    include_detailed_response: bool = False,
):
    """Sync data from a source path to a destination path.

    Args:
        source_path: Path to sync data from.
        destination_path: Path to sync data to.
        source_path_prefix: Optional S3 key prefix scoping the source.
        filter_config: Optional include/exclude filters describing what to move.
            Not supported for local -> local syncs, which warn and copy in full.
        filter_root: Root that filter patterns are matched relative to. Defaults
            to the source path; set explicitly when syncing a sub-prefix of the
            root the patterns were written against.
        max_concurrency: Maximum number of concurrent transfer operations.
        retain_source_data: Whether to keep source data after syncing.
        delete: Whether to delete destination paths absent from the (filtered)
            source. Note that this combines destructively with ``filter_config``
            -- see ``DataSyncConfig.delete``.
        require_lock: Whether to acquire a lock on the destination path.
        force: Whether to transfer regardless of existing destination content.
        size_only: Whether to compare only file sizes when deciding to transfer.
        fail_if_missing: Whether to raise if the source path does not exist.
        remote_to_local_config: Options specific to remote-to-local syncs.
        include_detailed_response: Whether to compute detailed transfer metrics.

    Returns:
        The sync result.
    """
    request = DataSyncRequest(
        source_path=source_path,
        destination_path=destination_path,
        source_path_prefix=S3KeyPrefix(source_path_prefix) if source_path_prefix else None,
        filter_config=filter_config,
        filter_root=filter_root,
        max_concurrency=max_concurrency,
        retain_source_data=retain_source_data,
        delete=delete,
        require_lock=require_lock,
        force=force,
        size_only=size_only,
        fail_if_missing=fail_if_missing,
        remote_to_local_config=remote_to_local_config or RemoteToLocalConfig(),
        include_detailed_response=include_detailed_response,
    )
    return DataSyncOperations.sync_request(request=request)


def refresh_local_path__mtime(path: Path, min_mtime: int | float):
    paths = find_all_paths(path, include_dirs=False, include_files=True)
    for subpath in paths:
        path_stats = os.stat(subpath)
        if path_stats.st_mtime < min_mtime:
            os.utime(subpath, times=(path_stats.st_atime, min_mtime))
