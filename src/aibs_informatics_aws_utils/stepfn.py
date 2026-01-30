import hashlib
import logging
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union

from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.models.aws.sfn import ExecutionArn as _ExecutionArn
from aibs_informatics_core.models.aws.sfn import StateMachineArn as _StateMachineArn
from aibs_informatics_core.utils.time import get_current_time
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import (
    AWSService,
    get_account_id,
    get_client_error_code,
    get_client_error_message,
    get_region,
)
from aibs_informatics_aws_utils.exceptions import (
    AWSError,
    InvalidAmazonResourceNameError,
    ResourceNotFoundError,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_stepfunctions.literals import ExecutionStatusType, IncludedDataType
    from mypy_boto3_stepfunctions.type_defs import (
        DescribeExecutionOutputTypeDef,
        ExecutionListItemTypeDef,
        HistoryEventTypeDef,
        StateMachineListItemTypeDef,
        TaskFailedEventDetailsTypeDef,
        TaskSucceededEventDetailsTypeDef,
    )
else:
    IncludedDataType = str
    ExecutionStatusType = str
    DescribeExecutionOutputTypeDef = dict
    HistoryEventTypeDef = dict
    DescribeExecutionOutputTypeDef = dict
    ExecutionStatusType = str
    ExecutionListItemTypeDef = dict
    HistoryEventTypeDef = dict
    StateMachineListItemTypeDef = dict
    TaskFailedEventDetailsTypeDef = dict
    TaskSucceededEventDetailsTypeDef = dict


get_sfn_client = AWSService.STEPFUNCTIONS.get_client


class StateMachineArn(_StateMachineArn):
    """ARN representation for Step Functions state machines."""

    @classmethod
    def from_components(  # type: ignore
        cls,
        state_machine_name: str,
        region: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> "StateMachineArn":
        """Create a StateMachineArn from components.

        Args:
            state_machine_name (str): The name of the state machine.
            region (Optional[str]): AWS region. Defaults to None (uses default region).
            account_id (Optional[str]): AWS account ID. Defaults to None (uses current).

        Returns:
            A StateMachineArn constructed from the components.
        """
        return StateMachineArn(
            ":".join(
                [
                    "arn",
                    "aws",
                    "states",
                    get_region(region),
                    account_id or get_account_id(),
                    "stateMachine",
                    state_machine_name,
                ]
            )
        )


class ExecutionArn(_ExecutionArn):
    """ARN representation for Step Functions executions."""

    @classmethod
    def from_components(  # type: ignore[override]
        cls,
        state_machine_name: str,
        execution_name: str,
        region: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> "ExecutionArn":
        """Create an ExecutionArn from components.

        Args:
            state_machine_name (str): The name of the state machine.
            execution_name (str): The name of the execution.
            region (Optional[str]): AWS region. Defaults to None (uses default region).
            account_id (Optional[str]): AWS account ID. Defaults to None (uses current).

        Returns:
            An ExecutionArn constructed from the components.
        """
        return ExecutionArn(
            ":".join(
                [
                    "arn",
                    "aws",
                    "states",
                    get_region(region),
                    account_id or get_account_id(),
                    "execution",
                    state_machine_name,
                    execution_name,
                ]
            )
        )


# Note: Stepfunctions API start_execution is idempotent operation,
# so with same name and payload always returns the same execution, instead
# of duplicating for demand. This also means the same payload can only be
# executed once in an environment and the name cant be reused for 90 days.
def build_execution_name(payload: str, date: Optional[datetime] = None) -> str:
    """Build a unique execution name from a payload string.

    Creates a SHA256 hash of the payload (optionally combined with a date)
    to generate a deterministic execution name.

    Args:
        payload (str): The payload string to hash.
        date (Optional[datetime]): Optional datetime to include for uniqueness.

    Raises:
        ValueError: If serialization or encoding fails.

    Returns:
        A SHA256 hex digest suitable for use as an execution name.
    """
    try:
        str_to_encode = payload + date.isoformat() if date else ""
        return hashlib.sha256(str_to_encode.encode()).hexdigest()
    except TypeError as e:
        raise ValueError(f"JSON serialization failed for {payload}: {e}")
    except UnicodeDecodeError as e:
        raise ValueError(f"String Encoding failed for {payload}: {e}")
    except Exception as e:
        raise ValueError(f"Exception {e} raised for {payload}")


def get_execution_arn(
    state_machine_name: str,
    execution_name: str,
    env_base: Optional[EnvBase] = None,
    region: Optional[str] = None,
) -> ExecutionArn:
    """Get an execution ARN by state machine and execution name.

    Args:
        state_machine_name (str): The name of the state machine.
        execution_name (str): The name of the execution.
        env_base (Optional[EnvBase]): Environment base for filtering state machines.
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Raises:
        InvalidAmazonResourceNameError: If no matching execution is found.

    Returns:
        The ExecutionArn for the matching execution.
    """
    sfn = get_sfn_client(region)
    state_machine = get_state_machine(name=state_machine_name, env_base=env_base, region=region)
    paginator = sfn.get_paginator("list_executions")
    iterator = paginator.paginate(
        stateMachineArn=state_machine["stateMachineArn"],
    )
    for list_executions_response in iterator:
        for execution in list_executions_response["executions"]:
            if execution["name"] == execution_name:
                return ExecutionArn(execution["executionArn"])
    else:
        raise InvalidAmazonResourceNameError(
            f"Could not find an execution ARN with "
            f"execution_name={execution_name}, "
            f"state_machine_name={state_machine['name']}"
        )


def describe_execution(
    execution_arn: str, included_data: IncludedDataType = "ALL_DATA", region: Optional[str] = None
) -> DescribeExecutionOutputTypeDef:
    """Describe a Step Functions execution.

    Args:
        execution_arn (str): The ARN of the execution to describe.
        included_data (IncludedDataType): Data to include. Defaults to "ALL_DATA".
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Returns:
        The execution description including status, input, and output.
    """
    sfn = get_sfn_client(region=get_region(region=region))

    execution_description = sfn.describe_execution(
        executionArn=execution_arn, includedData=included_data
    )
    return execution_description


def get_execution_history(
    execution_arn: Union[ExecutionArn, str],
    reverse_order: bool = False,
    include_execution_data: bool = False,
    region: Optional[str] = None,
) -> List[HistoryEventTypeDef]:
    """Get the execution history for a Step Functions execution.

    Args:
        execution_arn (Union[ExecutionArn, str]): The ARN of the execution.
        reverse_order (bool): Return events in reverse chronological order.
        include_execution_data (bool): Include input/output data in events.
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Returns:
        List of history events for the execution.
    """
    sfn = get_sfn_client(region=get_region(region=region))
    execution_arn = ExecutionArn(execution_arn)
    paginator = sfn.get_paginator("get_execution_history")
    return [
        event
        for response in paginator.paginate(
            executionArn=execution_arn,
            reverseOrder=reverse_order,
            includeExecutionData=include_execution_data,
        )
        for event in response.get("events", [])
    ]


def start_execution(
    state_machine_name: str,
    state_machine_input: str,
    reuse_existing_execution: bool = False,
    execution_name: Optional[str] = None,
    env_base: Optional[EnvBase] = None,
    region: Optional[str] = None,
) -> ExecutionArn:
    """Starts a StepFn Execution

    Notes:
        * The state machine Arn is resolved using the name.
        * If ExecutionAlreadyExists and `reuse_existing_execution`=False,
          a unique execution name will be generated using
          aibs_informatics_core.utils.time.get_current_time().

    Args:
        state_machine_name (str): Name of state machine to execute
        state_machine_input (str): Serialized payload input for execution.
        reuse_existing_execution (bool, optional): Skip if existing execution submitted.
            Defaults to False.
        env_base (EnvBase, optional): environment base. Defaults to None.
        region (str, optional): ergion. Defaults to None.

    Returns:
        ExecutionArn: the execution arn
    """
    region = get_region(region=region)
    sfn = get_sfn_client()

    state_machine = get_state_machine(name=state_machine_name, env_base=env_base, region=region)
    state_machine_full_name = state_machine["name"]
    state_machine_arn = state_machine["stateMachineArn"]

    def _start_execution(execution_name: str):
        try:
            logger.info(
                f"Starting execution of step function {state_machine_arn} [{execution_name}]",
            )

            response = sfn.start_execution(
                stateMachineArn=state_machine_arn,
                name=execution_name,
                input=state_machine_input,
            )

        except ClientError as e:
            if get_client_error_code(e) == "ExecutionAlreadyExists":
                if reuse_existing_execution:
                    logger.info(f"ExecutionAlreadyExists for {state_machine_input} so skipping.")
                    return ExecutionArn.findall(get_client_error_message(e))[0]
                else:
                    return _start_execution(
                        build_execution_name(state_machine_input, date=get_current_time())
                    )
            elif get_client_error_code(e) == "StateMachineDoesNotExist":
                msg = (
                    f"State machine {state_machine_full_name} [{state_machine_arn}] does not exist"
                )
                logger.error(msg)
                raise ResourceNotFoundError(msg) from e
            else:
                raise AWSError(
                    f"Start StepFn failed with unknown ClientError for {state_machine_arn}: {e}"
                ) from e
        except Exception as e:
            raise AWSError(f"Unknown failure for {state_machine_arn}") from e
        return ExecutionArn(response["executionArn"])

    execution_name = execution_name or build_execution_name(state_machine_input)
    return _start_execution(execution_name)


def stop_execution(
    execution_arn: Union[ExecutionArn, str],
    error: Optional[str] = None,
    cause: Optional[str] = None,
    region: Optional[str] = None,
) -> datetime:
    """Stop a running Step Functions execution.

    Args:
        execution_arn (Union[ExecutionArn, str]): The ARN of the execution to stop.
        error (Optional[str]): Optional error code to record.
        cause (Optional[str]): Optional cause description to record.
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Returns:
        The datetime when the execution was stopped.
    """
    sfn = get_sfn_client(region=get_region(region=region))
    response = sfn.stop_execution(
        executionArn=ExecutionArn(execution_arn), error=error or "", cause=cause or ""
    )
    return response["stopDate"]


def get_state_machine(
    name: str, env_base: Optional[EnvBase] = None, region: Optional[str] = None
) -> StateMachineListItemTypeDef:
    """Get a state machine by name suffix.

    Args:
        name (str): The name suffix to match against state machine names.
        env_base (Optional[EnvBase]): Optional environment base to filter by prefix.
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Raises:
        ResourceNotFoundError: If no matching state machine is found.
        AttributeError: If multiple state machines match the criteria.

    Returns:
        The matching state machine metadata.
    """
    region = get_region(region=region)
    # env_base = env_base or get_env_base()

    matching_state_machines = [
        state_machine
        for state_machine in get_state_machines(env_base=env_base, region=region)
        if state_machine["name"].endswith(name)
    ]
    if len(matching_state_machines) == 0:
        raise ResourceNotFoundError(
            f"No state machines with env_base={env_base} and name suffix={name} exist"
        )
    elif len(matching_state_machines) > 1:
        raise AttributeError(
            f"More than 1 state machines with env_base={env_base} "
            f"and name suffix={name} exist: {matching_state_machines}"
        )
    return matching_state_machines[0]


def get_state_machines(
    env_base: Optional[EnvBase] = None, region: Optional[str] = None
) -> List[StateMachineListItemTypeDef]:
    """List all state machines, optionally filtered by environment base.

    Args:
        env_base (Optional[EnvBase]): Optional environment base to filter by prefix.
        region (Optional[str]): AWS region. Defaults to None (uses default region).

    Returns:
        List of state machine metadata items.
    """
    region = get_region(region=region)
    sfn = get_sfn_client(region=region)
    paginator = sfn.get_paginator("list_state_machines")

    return [
        state_machine_info
        for response in paginator.paginate()
        for state_machine_info in response["stateMachines"]
        if not env_base or state_machine_info["name"].startswith(env_base)
    ]
