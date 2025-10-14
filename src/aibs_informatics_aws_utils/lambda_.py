import json
from typing import TYPE_CHECKING, List, Optional, Union

import requests
from aibs_informatics_core.models.aws.core import AWSRegion
from aibs_informatics_core.models.aws.lambda_ import LambdaFunctionName, LambdaFunctionUrl
from aibs_informatics_core.models.base import ModelProtocol
from botocore.exceptions import ClientError
from requests.auth import AuthBase

from aibs_informatics_aws_utils.auth import IamAWSRequestsAuth
from aibs_informatics_aws_utils.core import AWSService, get_client_error_code

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_lambda.type_defs import FileSystemConfigTypeDef
else:
    FileSystemConfigTypeDef = dict


get_lambda_client = AWSService.LAMBDA.get_client


def get_lambda_function_url(
    function_name: Union[LambdaFunctionName, str], region: Optional[AWSRegion] = None
) -> Optional[LambdaFunctionUrl]:
    function_name = LambdaFunctionName(function_name)

    lambda_client = get_lambda_client(region=region)

    try:
        response = lambda_client.get_function_url_config(FunctionName=function_name)
    except ClientError as e:
        if get_client_error_code(e) == "ResourceNotFoundException":
            return None
        else:
            raise e
    return LambdaFunctionUrl(response["FunctionUrl"])


def get_lambda_function_file_systems(
    function_name: Union[LambdaFunctionName, str], region: Optional[AWSRegion] = None
) -> List[FileSystemConfigTypeDef]:
    function_name = LambdaFunctionName(function_name)

    lambda_client = get_lambda_client(region=region)

    response = lambda_client.get_function_configuration(FunctionName=function_name)

    fs_configs = response.get("FileSystemConfigs")

    return fs_configs or []


def call_lambda_function_url(
    function_name: Union[LambdaFunctionName, LambdaFunctionUrl, str],
    payload: Optional[Union[ModelProtocol, dict, str, bytes]] = None,
    region: Optional[AWSRegion] = None,
    headers: Optional[dict] = None,
    auth: Optional[AuthBase] = None,
    **request_kwargs,
) -> Union[dict, str, None]:
    if LambdaFunctionName.is_valid(function_name):
        function_url = get_lambda_function_url(LambdaFunctionName(function_name), region=region)
        if function_url is None:
            raise ValueError(f"Function {function_name} not found")
        function_url = LambdaFunctionUrl(function_url)
    elif LambdaFunctionUrl.is_valid(function_name):
        function_url = LambdaFunctionUrl(function_name)
    else:
        raise ValueError(f"Invalid function name or url: {function_name}")

    json_payload: Optional[str] = None
    if isinstance(payload, (dict, list)):
        json_payload = json.dumps(payload)
    elif isinstance(payload, ModelProtocol):
        json_payload = json.dumps(payload.to_dict())
    elif isinstance(payload, str):
        json_payload = payload
    elif isinstance(payload, bytes):
        json_payload = payload.decode("utf-8")
    elif payload is None:
        pass
    else:
        raise ValueError(f"Invalid payload type: {type(payload)}")

    if headers is None:
        headers = {}

    if auth is None:
        auth = IamAWSRequestsAuth(service_name="lambda")

    response = requests.request(
        method="POST" if json_payload else "GET",
        url=function_url.base_url + function_url.path,
        json=json_payload,
        params=function_url.query,
        headers=headers,
        auth=auth,
        **request_kwargs,
    )
    if response.ok:
        if response.headers.get("Content-Type") == "application/json":
            return response.json()
        else:
            return response.text
    else:
        response.raise_for_status()
        return None
