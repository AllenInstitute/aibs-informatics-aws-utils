import json
from typing import TYPE_CHECKING, Any, Dict, Literal, Union, overload

from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import AWSService

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ssm.literals import ParameterTypeType
else:
    ParameterTypeType = str

get_ssm_client = AWSService.SSM.get_client


def put_ssm_parameter(
    param_name: str,
    param_value: str,
    param_type: ParameterTypeType = "String",
    exists_ok: bool = True,
) -> int:
    """Put or update parameter

    Args:
        param_name (str): Name of parameter
        param_value (str): New value of parameter
        param_type (ParameterTypeType, optional): Type of parameter. Defaults to "String".
        exists_ok (bool, optional): Allow overwrite if value exists. Defaults to True.

    Returns:
        int: The version of the parameter
    """
    ssm = get_ssm_client()
    return ssm.put_parameter(
        Name=param_name, Value=param_value, Type=param_type, Overwrite=exists_ok
    )["Version"]


@overload
def get_ssm_parameter(
    param_name: str, as_dict: Literal[True]
) -> Dict[str, Any]: ...  # pragma: no cover


@overload
def get_ssm_parameter(
    param_name: str, as_dict: Literal[False] = False
) -> str: ...  # pragma: no cover


def get_ssm_parameter(param_name: str, as_dict: bool = False) -> Union[str, Dict[str, Any]]:
    """Retrieves a SSM parameter value

    Args:
        param_name (str): the SSM parameter key name
        as_dict (bool, optional): Return as dict. Defaults to False.

    Raises:
        ValueError: If there is no such key

    Returns:
        str|dict: The parameter value stored at the key name
    """
    ssm = get_ssm_client()

    response = ssm.get_parameter(Name=param_name, WithDecryption=True)

    param = response["Parameter"]

    param_value = param.get("Value", None)

    if param_value is None:
        raise ValueError(f"Error obtaining param {param_name} from parameter store.")

    if as_dict:
        return json.loads(param_value)
    return param_value


def has_ssm_parameter(param_name: str) -> bool:
    try:
        get_ssm_parameter(param_name)
    except (ValueError, ClientError):
        return False
    else:
        return True
