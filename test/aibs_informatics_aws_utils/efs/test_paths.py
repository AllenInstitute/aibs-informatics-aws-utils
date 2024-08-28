from pathlib import Path
from test.aibs_informatics_aws_utils.efs.base import EFSTestsBase
from typing import Optional, Tuple, Union

from aibs_informatics_core.models.aws.efs import EFSPath

from aibs_informatics_aws_utils.constants.efs import (
    EFS_MOUNT_POINT_ID_VAR,
    EFS_MOUNT_POINT_PATH_VAR,
)
from aibs_informatics_aws_utils.efs import MountPointConfiguration, get_efs_path, get_local_path


class EFSPathTests(EFSTestsBase):
    def test__get_efs_path__local_path_maps_to_single_mount_point(self):
        mp = self.get_mount_point()
        local_path = mp.mount_point / "file.txt"
        local_path.touch()

        actual_efs_path = get_efs_path(local_path, mount_points=[mp])
        assert actual_efs_path.file_system_id == mp.file_system["FileSystemId"]
        assert actual_efs_path.path == Path("/file.txt")

    def test__get_efs_path__no_matching_mount_point(self):
        mp = self.get_mount_point()
        local_path = self.tmp_file(content="test")

        with self.assertRaises(ValueError):
            get_efs_path(local_path, raise_if_unresolved=True, mount_points=[mp])

        assert get_efs_path(local_path, raise_if_unresolved=False, mount_points=[mp]) is None

    def test__get_efs_path__local_path_maps_to_multiple_mount_points(self):
        mp1 = self.get_mount_point(access_point_name="ap1", access_point_path="/A")
        mp2 = self.get_mount_point(access_point_name="ap2", access_point_path="/A/B")

        local_path = mp2.mount_point / "file.txt"
        local_path.touch()

        actual_efs_path = get_efs_path(
            local_path, raise_if_unresolved=True, mount_points=[mp1, mp2]
        )
        assert actual_efs_path.file_system_id == mp2.file_system["FileSystemId"]
        assert actual_efs_path.path == Path("/A/B/file.txt")

    def test__get_local_path__efs_path_maps_to_single_mount_point(self):
        mp = self.get_mount_point()

        efs_path = EFSPath.build(resource_id=mp.file_system["FileSystemId"], path="/file.txt")

        local_path = mp.mount_point / "file.txt"
        local_path.touch()

        actual_local_path = get_local_path(efs_path, mount_points=[mp])
        assert actual_local_path == local_path

    def test__get_local_path__no_matching_mount_point(self):
        mp = self.get_mount_point(access_point_name="ap", access_point_path="/B")
        efs_path = EFSPath.build(resource_id=mp.file_system["FileSystemId"], path="/A/file.txt")

        with self.assertRaises(ValueError):
            get_local_path(efs_path, raise_if_unmounted=True, mount_points=[mp])

        assert get_local_path(efs_path, raise_if_unmounted=False, mount_points=[mp]) is None

    def get_fs_id(self, fs_name: str) -> str:
        return self.create_file_system(fs_name)

    def get_efs_path(self, path: Union[str, Path], fs_name: str) -> EFSPath:
        fs_id = self.get_fs_id(fs_name)
        return EFSPath.build(resource_id=fs_id, path=path)

    def get_mount_point(
        self,
        mount_point: Optional[Union[Path, str]] = None,
        access_point_name: Optional[str] = None,
        access_point_path: Optional[Union[Path, str]] = None,
    ) -> MountPointConfiguration:
        file_system = self.create_file_system()
        if access_point_name:
            access_point = self.create_access_point(
                access_point_name=access_point_name,
                access_point_path=access_point_path,
            )
        else:
            access_point = None
        return MountPointConfiguration.build(
            mount_point=mount_point or self.tmp_path(),
            file_system=file_system,
            access_point=access_point,
        )
