from aibs_informatics_aws_utils.efs import (
    get_efs_access_point,
    get_efs_file_system,
    list_efs_access_points,
    list_efs_file_systems,
)
from test.aibs_informatics_aws_utils.efs.base import EFSTestsBase


class EFSTests(EFSTestsBase):
    def test__list_efs_file_systems__filters_based_on_tag(self):
        file_system_id1 = self.create_file_system("fs1", env="dev")
        self.create_file_system("fs2", env="prod")
        file_systems = list_efs_file_systems(tags=dict(env="dev"))
        self.assertEqual(len(file_systems), 1)
        self.assertEqual(file_systems[0]["FileSystemId"], file_system_id1)

    def test__list_efs_file_systems__filters_based_on_name(self):
        file_system_id1 = self.create_file_system("fs1", env="dev")
        file_system_id2 = self.create_file_system("fs2", env="dev")

        file_systems = list_efs_file_systems(name="fs1")
        self.assertEqual(len(file_systems), 1)
        self.assertEqual(file_systems[0]["FileSystemId"], file_system_id1)
        self.assertNotEqual(file_systems[0]["FileSystemId"], file_system_id2)

    def test__get_efs_file_system__happy_case(self):
        file_system_id1 = self.create_file_system("fs1", env="dev")
        self.create_file_system("fs2", env="dev")

        file_system = get_efs_file_system(name="fs1")
        self.assertEqual(file_system["FileSystemId"], file_system_id1)

    def test__get_efs_file_system__raises_value_error_if_no_file_systems(self):
        with self.assertRaises(ValueError):
            get_efs_file_system()

    def test__get_efs_file_system__raises_value_error_if_multiple_file_systems(self):
        self.create_file_system("fs1", env="dev")
        self.create_file_system("fs2", env="dev")
        with self.assertRaises(ValueError):
            get_efs_file_system(tags=dict(env="dev"))

    def test__list_efs_access_points__filters_based_on_ap_tag(self):
        file_system_id = self.create_file_system("fs1", env="dev")
        access_point_id1 = self.create_access_point(
            file_system_id=file_system_id, access_point_name="ap1", env="dev"
        )
        self.create_access_point(
            file_system_id=file_system_id, access_point_name="ap2", env="prod"
        )
        access_points = list_efs_access_points(
            file_system_name="fs1", access_point_tags=dict(env="dev")
        )
        self.assertEqual(len(access_points), 1)
        self.assertEqual(access_points[0].get("AccessPointId"), access_point_id1)

    def test__list_efs_access_points__all_filters_provided(self):
        file_system_id = self.create_file_system("fs1", env="dev")
        access_point_id1 = self.create_access_point(
            file_system_id=file_system_id, access_point_name="ap1", env="dev"
        )
        self.create_access_point(
            file_system_id=file_system_id, access_point_name="ap2", env="prod"
        )
        access_points = list_efs_access_points(
            file_system_id=file_system_id,
            file_system_name="fs1",
            access_point_id=access_point_id1,
            access_point_name="ap1",
            access_point_tags=dict(env="dev"),
        )
        self.assertEqual(len(access_points), 1)
        self.assertEqual(access_points[0].get("AccessPointId"), access_point_id1)

    def test__list_efs_access_points__filters_based_on_name(self):
        file_system_id = self.create_file_system("fs1", env="dev")
        access_point_id1 = self.create_access_point(
            file_system_id=file_system_id, access_point_name="ap1", env="dev"
        )
        self.create_access_point(file_system_id=file_system_id, access_point_name="ap2", env="dev")
        access_points = list_efs_access_points(
            file_system_tags=dict(env="dev"), access_point_name="ap1"
        )
        self.assertEqual(len(access_points), 1)
        self.assertEqual(access_points[0].get("AccessPointId"), access_point_id1)

    def test__get_efs_access_point__filters_based_on_ap_tag(self):
        file_system_id = self.create_file_system("fs1", env="dev")
        access_point_id1 = self.create_access_point(file_system_id, "ap1", env="dev")
        self.create_access_point(file_system_id, "ap2", env="prod")
        access_point = get_efs_access_point(access_point_tags=dict(env="dev"))
        self.assertEqual(access_point.get("AccessPointId"), access_point_id1)

    def test__get_efs_access_point__raises_value_error_if_no_access_points(self):
        with self.assertRaises(ValueError):
            get_efs_access_point()

    def test__get_efs_access_point__raises_value_error_if_multiple_access_points(self):
        file_system_id = self.create_file_system("fs1", env="dev")
        self.create_access_point(file_system_id, "ap1", env="dev")
        self.create_access_point(file_system_id, "ap2", env="dev")
        with self.assertRaises(ValueError):
            get_efs_access_point(access_point_tags=dict(env="dev"))
