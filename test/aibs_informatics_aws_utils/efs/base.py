from pathlib import Path
from test.aibs_informatics_aws_utils.base import AwsBaseTest
from typing import Dict, Optional, Union

import boto3
import moto


class EFSTestsBase(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()

        self.mock_efs = moto.mock_aws()
        self.mock_efs.start()

        self.set_aws_credentials()
        self._file_store_name_id_map: Dict[str, str] = {}

    def tearDown(self) -> None:
        self.mock_efs.stop()
        return super().tearDown()

    @property
    def efs_client(self):
        return boto3.client("efs")

    def create_file_system(self, file_system_name: Optional[str] = None, **tags):
        file_system_name = file_system_name or "fs"
        if file_system_name not in self._file_store_name_id_map:
            tags = [{"Key": k, "Value": v} for k, v in tags.items()]
            tags.insert(0, {"Key": "Name", "Value": file_system_name})
            fs_response = self.efs_client.create_file_system(
                CreationToken=file_system_name,
                Tags=tags,
            )
            self._file_store_name_id_map[file_system_name] = fs_response["FileSystemId"]
        return self._file_store_name_id_map[file_system_name]

    def create_access_point(
        self,
        access_point_name: str,
        access_point_path: Optional[Union[str, Path]] = None,
        file_system_id: Optional[str] = None,
        file_system_name: Optional[str] = None,
        **tags,
    ):
        file_system_id = file_system_id or self.create_file_system(file_system_name)

        tags = [{"Key": k, "Value": v} for k, v in tags.items()]
        tags.insert(0, {"Key": "Name", "Value": access_point_name})
        response = self.efs_client.create_access_point(
            FileSystemId=file_system_id,
            PosixUser={"Uid": 1000, "Gid": 1000},
            RootDirectory={"Path": str(access_point_path or "/")},
            Tags=tags,
        )
        return response["AccessPointId"]
