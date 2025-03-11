import json
from typing import Any, Dict, Literal, Optional, overload

from aibs_informatics_aws_utils.core import AWSService

get_secretsmanager_client = AWSService.SECRETSMANAGER.get_client


@overload
def get_secret_value(
    secret_name: str, as_dict: Literal[False] = False, region: Optional[str] = None
) -> str: ...


@overload
def get_secret_value(
    secret_name: str, as_dict: Literal[True], region: Optional[str] = None
) -> Dict[str, Any]: ...


def get_secret_value(secret_name: str, as_dict: bool = False, region: Optional[str] = None):
    """Retrieves a Secrets Manager secret value

    Args:
        secret_name (str): the Secrets Manager secret name

    Raises:
        ValueError: If there is no such key

    Returns:
        str: The secret value stored at the key name
    """
    secretsmanager = get_secretsmanager_client(region=region)

    response = secretsmanager.get_secret_value(SecretId=secret_name)

    secret = response["SecretString"]
    if as_dict:
        return json.loads(secret)
    else:
        return secret
