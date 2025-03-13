from typing import TYPE_CHECKING, List, Optional

from aibs_informatics_aws_utils.core import AWSService, get_region
from aibs_informatics_aws_utils.exceptions import ResourceNotFoundError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_apigateway.type_defs import RestApiTypeDef
else:
    RestApiTypeDef = dict

get_apigateway_client = AWSService.API_GATEWAY.get_client


def get_rest_api(api_name: str, region: Optional[str] = None) -> RestApiTypeDef:
    apigw = get_apigateway_client(region=region)

    paginator = apigw.get_paginator("get_rest_apis")
    rest_apis: List[RestApiTypeDef] = paginator.paginate(
        PaginationConfig={"MaxItems": 100}
    ).build_full_result()["items"]

    for rest_api in rest_apis:
        # In theory, only one api should be associated with env-base
        if rest_api.get("name") == api_name:
            return rest_api
    else:
        raise ResourceNotFoundError(f"Could not resolve REST Api with {api_name}")


def get_rest_api_endpoint(
    rest_api: RestApiTypeDef, stage: str = "prod", region: Optional[str] = None
) -> str:
    api_id = rest_api["id"]  # type: ignore  # mypy_boto3 TypeDict makes optional, but actually is required
    region = get_region(region)
    return f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage}"
