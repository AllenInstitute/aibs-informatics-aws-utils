# AIBS Informatics AWS Utils

[![Build Status](https://github.com/AllenInstitute/aibs-informatics-aws-utils/actions/workflows/build.yml/badge.svg)](https://github.com/AllenInstitute/aibs-informatics-aws-utils/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/AllenInstitute/aibs-informatics-aws-utils/graph/badge.svg?token=I3A5UC9CMX)](https://codecov.io/gh/AllenInstitute/aibs-informatics-aws-utils)

---

## Overview

The AIBS Informatics AWS Utils library provides a collection of utilities and tools for working with AWS services. This library includes functionalities for interacting with AWS S3, ECR, Lambda, Batch, DynamoDB, and many other AWS services, making it easier to integrate AWS capabilities into various projects at the Allen Institute for Brain Science.

## Features

- **AWS Service Wrappers** - Simplified interfaces for S3, Lambda, Batch, ECS, ECR, and more
- **DynamoDB Utilities** - Table management, conditions, and query functions
- **ECR Tools** - Container image management and replication
- **EFS Integration** - Mount point management and path utilities
- **Data Sync** - File system synchronization operations
- **Authentication** - AWS auth and credential management utilities

## Quick Start

### Installation

```bash
pip install aibs-informatics-aws-utils
```

### Basic Usage

```python
from aibs_informatics_aws_utils.s3 import get_s3_client, upload_file

# Upload a file to S3
upload_file("local/path/file.txt", "s3://bucket/remote/path/file.txt")
```

## Supported AWS Services

| Service | Module | Description |
|---------|--------|-------------|
| [API Gateway](api/services/apigateway.md) | `apigateway` | API Gateway utilities |
| [Athena](api/services/athena.md) | `athena` | Athena query utilities |
| [Batch](api/services/batch.md) | `batch` | AWS Batch job utilities |
| [DynamoDB](api/services/dynamodb/index.md) | `dynamodb` | DynamoDB table and query utilities |
| [EC2](api/services/ec2.md) | `ec2` | EC2 instance utilities |
| [ECR](api/services/ecr/index.md) | `ecr` | Container registry utilities |
| [ECS](api/services/ecs.md) | `ecs` | Container service utilities |
| [EFS](api/services/efs/index.md) | `efs` | Elastic File System utilities |
| [FSx](api/services/fsx.md) | `fsx` | FSx file system utilities |
| [Lambda](api/services/lambda.md) | `lambda_` | Lambda function utilities |
| [Logs](api/services/logs.md) | `logs` | CloudWatch Logs utilities |
| [S3](api/services/s3.md) | `s3` | S3 storage utilities |
| [Secrets Manager](api/services/secretsmanager.md) | `secretsmanager` | Secrets management utilities |
| [SES](api/services/ses.md) | `ses` | Email service utilities |
| [SNS](api/services/sns.md) | `sns` | Notification service utilities |
| [SQS](api/services/sqs.md) | `sqs` | Queue service utilities |
| [SSM](api/services/ssm.md) | `ssm` | Systems Manager utilities |
| [Step Functions](api/services/stepfn.md) | `stepfn` | State machine utilities |

## Contributing

Any and all PRs are welcome. Please see [CONTRIBUTING.md](https://github.com/AllenInstitute/aibs-informatics-aws-utils/blob/main/CONTRIBUTING.md) for more information.

## License

This software is licensed under the Allen Institute Software License, which is the 2-clause BSD license plus a third clause that prohibits redistribution and use for commercial purposes without further permission. For more information, please visit [Allen Institute Terms of Use](https://alleninstitute.org/terms-of-use/).
