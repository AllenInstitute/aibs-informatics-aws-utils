import json
from typing import Any, Literal, overload

from aibs_informatics_aws_utils.core import AWSService

get_secretsmanager_client = AWSService.SECRETSMANAGER.get_client


@overload
def get_secret_value(
    secret_name: str, as_dict: Literal[False] = False, region: str | None = None
) -> str: ...


@overload
def get_secret_value(
    secret_name: str, as_dict: Literal[True], region: str | None = None
) -> dict[str, Any]: ...


def get_secret_value(
    secret_name: str, as_dict: bool = False, region: str | None = None
) -> str | dict[str, Any]:
    """Retrieves a Secrets Manager secret value

    Args:
        secret_name (str): the Secrets Manager secret name
        as_dict (bool): If True, return the secret as a dictionary. Defaults to False.
        region (Optional[str]): AWS region. Defaults to None.

    Raises:
        ValueError: If there is no such key

    Returns:
        Union[str, dict]: The secret value stored at the key name, as string or dict
    """
    secretsmanager = get_secretsmanager_client(region=region)

    response = secretsmanager.get_secret_value(SecretId=secret_name)

    secret = response["SecretString"]
    if as_dict:
        return json.loads(secret)
    else:
        return secret
