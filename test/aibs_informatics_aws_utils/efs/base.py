from pathlib import Path

import boto3
import moto
import moto.core
import moto.core.config
import moto.core.decorator

from test.aibs_informatics_aws_utils.base import AwsBaseTest


class EFSTestsBase(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()

        # HACK: We must define moto.mock_aws here instead of as a decorator because moto gives us
        #       issues when we try to use the moto.mock_aws decorator in the child classes
        #       (as https://github.com/getmoto/moto/issues/7063 suggests).
        #       We previously double decorated - decorating the child and parent class - but this
        #       now fails in python 3.12 (perhaps cleanup of mock collisions leniency).
        #
        #       This is a workaround until we can find a better solution. If configs need to be
        #       overridden, they can be passed in via the mock_aws_config property on the child
        #       class.
        self.mock_efs = moto.mock_aws(config=self.mock_aws_config)
        self.mock_efs.start()

        self.set_aws_credentials()
        self._file_store_name_id_map: dict[str, str] = {}

    def tearDown(self) -> None:
        super().tearDown()
        self.mock_efs.stop()

    @property
    def mock_aws_config(self) -> moto.core.config.DefaultConfig | None:
        return None

    @property
    def efs_client(self):
        return boto3.client("efs")

    def create_file_system(self, file_system_name: str | None = None, **tags):
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
        access_point_path: str | Path | None = None,
        file_system_id: str | None = None,
        file_system_name: str | None = None,
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
