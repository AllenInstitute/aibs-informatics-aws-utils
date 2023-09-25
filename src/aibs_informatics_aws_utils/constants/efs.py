from dataclasses import dataclass

EFS_MOUNT_PATH_VAR = "EFS_MOUNT_PATH"


EFS_GWO_FILE_SYSTEM_NAME = "gwo-file-system"

# fmt: off
EFS_ROOT_PATH           = "/"
EFS_SHARED_PATH         = "/shared"
EFS_SCRATCH_PATH        = "/scratch"
EFS_TMP_PATH            = "/tmp"
# fmt: on


@dataclass
class EFSTag:
    key: str
    value: str


EFS_ROOT_ACCESS_POINT_TAG = EFSTag("Name", "root")
EFS_SHARED_ACCESS_POINT_TAG = EFSTag("Name", "shared")
EFS_SCRATCH_ACCESS_POINT_TAG = EFSTag("Name", "scratch")
EFS_TMP_ACCESS_POINT_TAG = EFSTag("Name", "tmp")
