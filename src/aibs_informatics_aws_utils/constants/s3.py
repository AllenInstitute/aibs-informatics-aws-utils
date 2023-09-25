from enum import Enum


class S3BucketName(str, Enum):
    ASSETS = "assets"
    COST_REPORTING = "cost-reporting"
    FILE_STORE = "file-store"
    COST_INVENTORY = "cost-inventory"
    LANDING = "landing"
    STAGING = "staging"


S3_LANDING_BUCKET = S3BucketName.LANDING.value
S3_ASSETS_BUCKET = S3BucketName.ASSETS.value
S3_FILE_STORE_BUCKET = S3BucketName.FILE_STORE.value
S3_COST_REPORTING_BUCKET = S3BucketName.COST_REPORTING.value
S3_STAGING_BUCKET = S3BucketName.STAGING.value


S3_SOURCE_PATH_VAR = "S3_SOURCE_PATH"
S3_SOURCE_PATH_PREFIX_VAR = "S3_SOURCE_PATH_PREFIX"
S3_DESTINATION_PATH_VAR = "S3_DESTINATION_PATH"


# scratch file constants

S3_SCRATCH_KEY_PREFIX = "scratch/"
S3_SCRATCH_TAGGING_KEY = "time-to-live"
S3_SCRATCH_TAGGING_VALUE = "scratch"
