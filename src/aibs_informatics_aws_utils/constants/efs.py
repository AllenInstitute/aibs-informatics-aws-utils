from dataclasses import dataclass


@dataclass
class EFSTag:
    key: str
    value: str


## EFS Environment Variable Constants
EFS_MOUNT_POINT_PATH_VAR = "EFS_MOUNT_POINT_PATH"
EFS_MOUNT_POINT_ID_VAR = "EFS_MOUNT_POINT_ID"


EFS_MOUNT_POINT_PATH_VAR_PREFIX = "EFS_MOUNT_POINT_PATH_"
EFS_MOUNT_POINT_ID_VAR_PREFIX = "EFS_MOUNT_POINT_ID_"


# ------------------------------------
# Standard Names and Paths for EFS


# fmt: off
EFS_ROOT_PATH           = "/"
EFS_SHARED_PATH         = "/shared"
EFS_SCRATCH_PATH        = "/scratch"
EFS_TMP_PATH            = "/tmp"
# fmt: on


# fmt: off
EFS_ROOT_ACCESS_POINT_NAME      = "root"
EFS_SHARED_ACCESS_POINT_NAME    = "shared"
EFS_SCRATCH_ACCESS_POINT_NAME   = "scratch"
EFS_TMP_ACCESS_POINT_NAME       = "tmp"
# fmt: on


# fmt: off
EFS_ROOT_ACCESS_POINT_TAG       = EFSTag("Name", EFS_ROOT_ACCESS_POINT_NAME)
EFS_SHARED_ACCESS_POINT_TAG     = EFSTag("Name", EFS_SHARED_ACCESS_POINT_NAME)
EFS_SCRATCH_ACCESS_POINT_TAG    = EFSTag("Name", EFS_SCRATCH_ACCESS_POINT_NAME)
EFS_TMP_ACCESS_POINT_TAG        = EFSTag("Name", EFS_TMP_ACCESS_POINT_NAME)
# fmt: on
