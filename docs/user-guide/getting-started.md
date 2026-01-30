# Getting Started

This guide will help you get started with the AIBS Informatics AWS Utils library.

## Installation

### Using pip

```bash
pip install aibs-informatics-aws-utils
```

### Using uv

```bash
uv add aibs-informatics-aws-utils
```

## Prerequisites

### AWS Credentials

This library requires valid AWS credentials to interact with AWS services. You can configure credentials using:

1. **Environment Variables**
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-west-2
   ```

2. **AWS Credentials File** (`~/.aws/credentials`)
   ```ini
   [default]
   aws_access_key_id = your_access_key
   aws_secret_access_key = your_secret_key
   ```

3. **IAM Roles** (recommended for EC2/Lambda/ECS)

## Basic Examples

### Working with S3

```python
from aibs_informatics_aws_utils.s3 import get_s3_client

# Get an S3 client
s3_client = get_s3_client()

# List buckets
response = s3_client.list_buckets()
for bucket in response['Buckets']:
    print(bucket['Name'])
```

### Working with DynamoDB

```python
from aibs_informatics_aws_utils.dynamodb import DynamoDBTable

# Create a table wrapper
table = DynamoDBTable("my-table")

# Query items
items = table.query(key_condition="pk = :pk", expression_values={":pk": "my-key"})
```

### Working with ECR

```python
from aibs_informatics_aws_utils.ecr import get_ecr_client

# Get ECR client
ecr_client = get_ecr_client()

# List repositories
repos = ecr_client.describe_repositories()
```

### Working with Lambda

```python
from aibs_informatics_aws_utils.lambda_ import invoke_lambda

# Invoke a Lambda function
response = invoke_lambda(
    function_name="my-function",
    payload={"key": "value"}
)
```

## Next Steps

- Explore the [API Reference](../api/index.md) for detailed documentation
- Check the [Configuration Guide](configuration.md) for advanced setup options
- See the [Developer Guide](../developer/index.md) for contribution guidelines
