from typing import TYPE_CHECKING, List, Optional

from aibs_informatics_aws_utils.core import AWSService, get_region
from aibs_informatics_aws_utils.exceptions import ResourceNotFoundError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_apigateway.type_defs import RestApiTypeDef
else:
    RestApiTypeDef = dict

get_apigateway_client = AWSService.API_GATEWAY.get_client


def get_rest_api(api_name: str, region: Optional[str] = None) -> RestApiTypeDef:
    """Get a REST API by name.

    Args:
        api_name (str): The name of the REST API to find.
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Raises:
        ResourceNotFoundError: If no REST API with the given name is found.

    Returns:
        The REST API configuration.
    """
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
    """Get the endpoint URL for a REST API.

    Args:
        rest_api (RestApiTypeDef): The REST API configuration from get_rest_api().
        stage (str): The API Gateway stage name. Defaults to "prod".
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Returns:
        The fully qualified endpoint URL for the REST API.
    """
    api_id = rest_api["id"]  # type: ignore  # mypy_boto3 TypeDict makes optional, but actually is required
    region = get_region(region)
    return f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage}"
