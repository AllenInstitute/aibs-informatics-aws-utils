# Configuration

This guide covers configuration options for the AIBS Informatics AWS Utils library.

## AWS Configuration

### Region Configuration

You can specify the AWS region in multiple ways:

```python
# Via environment variable
import os
os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'

# Or when creating clients
from aibs_informatics_aws_utils.s3 import get_s3_client
client = get_s3_client(region_name='us-west-2')
```

### Credentials

The library uses boto3's credential chain. Credentials are resolved in this order:

1. Explicit credentials passed to client functions
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. Shared credentials file (`~/.aws/credentials`)
4. AWS config file (`~/.aws/config`)
5. IAM role (for EC2, Lambda, ECS tasks)

## Service-Specific Configuration

### S3

```python
from aibs_informatics_aws_utils.s3 import get_s3_client

# Custom endpoint (for S3-compatible services)
client = get_s3_client(endpoint_url='http://localhost:9000')
```

### DynamoDB

```python
from aibs_informatics_aws_utils.dynamodb import DynamoDBTable

# Local DynamoDB for development
table = DynamoDBTable(
    table_name="my-table",
    endpoint_url="http://localhost:8000"
)
```

### EFS Mount Points

```python
from aibs_informatics_aws_utils.efs import EFSMountPoint

# Configure EFS mount
mount_point = EFSMountPoint(
    file_system_id="fs-12345678",
    mount_path="/mnt/efs"
)
```

## Constants

The library provides constants for common configurations:

```python
from aibs_informatics_aws_utils.constants import (
    S3_MAX_KEYS,
    LAMBDA_MAX_PAYLOAD_SIZE,
    EFS_MAX_PATH_LENGTH
)
```

## Logging

Configure logging to see AWS operations:

```python
import logging

# Enable boto3 debug logging
logging.getLogger('boto3').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.DEBUG)

# Or just for this library
logging.getLogger('aibs_informatics_aws_utils').setLevel(logging.INFO)
```

## Error Handling

The library provides custom exceptions for AWS-specific errors:

```python
from aibs_informatics_aws_utils.exceptions import (
    AWSError,
    S3Error,
    DynamoDBError
)

try:
    # AWS operation
    pass
except S3Error as e:
    print(f"S3 operation failed: {e}")
except AWSError as e:
    print(f"AWS operation failed: {e}")
```
