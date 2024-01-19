from typing import TYPE_CHECKING, List, Optional, Union

from aibs_informatics_core.models.aws.core import AWSAccountId, AWSRegion
from aibs_informatics_core.models.aws.lambda_ import LambdaFunctionName
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService, get_client_error_code

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_lambda.type_defs import FileSystemConfigTypeDef
else:
    FileSystemConfigTypeDef = dict


get_lambda_client = AWSService.LAMBDA.get_client


def get_lambda_function_url(
    function_name: Union[LambdaFunctionName, str], region: AWSRegion = None
) -> Optional[str]:
    function_name = LambdaFunctionName(function_name)

    lambda_client = get_lambda_client(region=region)

    try:
        response = lambda_client.get_function_url_config(FunctionName=function_name)
    except ClientError as e:
        if get_client_error_code(e) == "ResourceNotFoundException":
            return None
        else:
            raise e
    return response["FunctionUrl"]


def get_lambda_function_file_systems(
    function_name: Union[LambdaFunctionName, str], region: AWSRegion = None
) -> List[FileSystemConfigTypeDef]:
    function_name = LambdaFunctionName(function_name)

    lambda_client = get_lambda_client(region=region)

    response = lambda_client.get_function_configuration(FunctionName=function_name)

    fs_configs = response.get("FileSystemConfigs")

    return fs_configs
