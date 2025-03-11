from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Tuple

from aibs_informatics_core.env import EnvBase
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from aibs_informatics_test_resources import does_not_raise
from pytest import mark, param, raises

from aibs_informatics_aws_utils.exceptions import (
    AWSError,
    InvalidAmazonResourceNameError,
    ResourceNotFoundError,
)
from aibs_informatics_aws_utils.stepfn import (
    ExecutionArn,
    StateMachineArn,
    build_execution_name,
    describe_execution,
    get_execution_arn,
    get_sfn_client,
    get_state_machine,
    get_state_machines,
    start_execution,
)
from test.aibs_informatics_aws_utils.base import AwsBaseTest

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_stepfunctions import SFNClient
    from mypy_boto3_stepfunctions.type_defs import (
        ExecutionListItemTypeDef,
        StateMachineListItemTypeDef,
    )
else:
    SFNClient = object
    ExecutionListItemTypeDef = dict
    StateMachineListItemTypeDef = dict


@mark.parametrize(
    "arn,expected,raises_error",
    [
        param(
            "arn:aws:states:us-west-2:123456789012:stateMachine:sm-name",
            ("us-west-2", "123456789012", "sm-name"),
            does_not_raise(),
            id="happy case",
        ),
        param(
            "arn:aws:states:us-west-2:123456789012:execution:sm-name:exec-id",
            ("", "", ""),
            raises(ValueError),
            id="invalid execution arn",
        ),
        param(
            "arn:aws:states:us-west-2:126789012:stateMachine:sm-name",
            ("", "", ""),
            raises(ValueError),
            id="invalid account",
        ),
        param(
            "arn:aws:states:us-west-2:126789012:stateMachine:sm/name",
            ("", "", ""),
            raises(ValueError),
            id="invalid name",
        ),
    ],
)
def test_StateMachineArn_validation(arn: str, expected: Tuple[str, ...], raises_error):
    region, account, state_machine_name = expected
    with raises_error:
        sm_arn = StateMachineArn(arn)

        assert sm_arn.region == region
        assert sm_arn.account_id == account
        assert sm_arn.state_machine_name == state_machine_name


@mark.parametrize(
    "arn,expected,raises_error",
    [
        param(
            "arn:aws:states:us-west-2:123456789012:execution:sm-name:exec-id",
            ("us-west-2", "123456789012", "sm-name", "exec-id"),
            does_not_raise(),
            id="happy case",
        ),
        param(
            "arn:aws:states:us-west-2:126789012:execution:sm-name:exec-id",
            ("", "", "", ""),
            raises(ValueError),
            id="invalid account",
        ),
        param(
            "arn:aws:states:us-west-2:126789012:execution:sm/name:exec-id",
            ("", "", "", ""),
            raises(ValueError),
            id="invalid name",
        ),
        param(
            "arn:aws:states:us-west-2:126789012:execution:sm-name:exec/id",
            ("", "", "", ""),
            raises(ValueError),
            id="invalid name",
        ),
    ],
)
def test_ExecutionArn_validation(arn: str, expected: Tuple[str, ...], raises_error):
    region, account, state_machine_name, exec_name = expected
    with raises_error:
        exec_arn = ExecutionArn(arn)

        assert exec_arn.region == region
        assert exec_arn.account_id == account
        assert exec_arn.state_machine_name == state_machine_name
        assert exec_arn.execution_name == exec_name


# SFN CALLS
DESCRIBE_EXECUTION = "describe_execution"
LIST_STATE_MACHINES = "list_state_machines"
START_EXECUTION = "start_execution"


class StepFnTests(AwsBaseTest):
    # SFN CALLS
    DESCRIBE_EXECUTION = "describe_execution"
    LIST_EXECUTIONS = "list_executions"
    LIST_STATE_MACHINES = "list_state_machines"
    START_EXECUTION = "start_execution"

    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.DEFAULT_REGION)
        self._sfn = get_sfn_client(region=self.DEFAULT_REGION)

        self._get_sfn_client = self.create_patch(
            "aibs_informatics_aws_utils.stepfn.get_sfn_client", autospec=SFNClient
        )
        self._get_sfn_client.return_value = self._sfn

        self._get_account_id = self.create_patch(
            "aibs_informatics_aws_utils.stepfn.get_account_id"
        )
        self._get_account_id.return_value = self.ACCOUNT_ID

    @property
    def sfn(self):
        return self._sfn

    def test_describe_execution(self):
        with self.stub(self.sfn) as sfn_stubber:
            exec_arn = self.create_execution_arn("name", "exec_name")
            sfn_stubber.add_response(
                DESCRIBE_EXECUTION, self.construct_execution("name", "exec_name")
            )
            execution = describe_execution(execution_arn=exec_arn, region=self.DEFAULT_REGION)
            self.assertEqual(execution["executionArn"], exec_arn)
            sfn_stubber.assert_no_pending_responses()

    def test__get_execution_arn__finds_execution_arn_from_execution_name(self):
        sm1 = self.construct_state_machine_item("blah-blah-state_machine1")
        exec1 = self.construct_execution("blah-blah-state_machine1", exec_name="1234")
        exec2 = self.construct_execution("blah-blah-state_machine1", exec_name="4321")
        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1]})
            sfn_stubber.add_response(self.LIST_EXECUTIONS, {"executions": [exec1, exec2]})
            execution_arn = get_execution_arn(
                state_machine_name="state_machine1",
                execution_name="1234",
                region=self.DEFAULT_REGION,
            )
            self.assertEqual(execution_arn, exec1["executionArn"])

    def test__get_execution_arn__fails_to_find_execution_arn_from_execution_name(self):
        sm1 = self.construct_state_machine_item("blah-blah-state_machine1")
        exec1 = self.construct_execution("blah-blah-state_machine1", exec_name="1234")
        exec2 = self.construct_execution("blah-blah-state_machine1", exec_name="4321")
        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1]})
            sfn_stubber.add_response(self.LIST_EXECUTIONS, {"executions": [exec1, exec2]})
            with self.assertRaises(InvalidAmazonResourceNameError):
                get_execution_arn(
                    state_machine_name="state_machine1",
                    execution_name="12345",
                    region=self.DEFAULT_REGION,
                )

    def test__get_state_machine__finds_matching_state_machine(self):
        sm1 = self.construct_state_machine_item("blah-blah-state_machine1")
        sm2 = self.construct_state_machine_item("blah-blah-state_machine2")

        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            state_machine = get_state_machine(name="state_machine1", region=self.DEFAULT_REGION)
            self.assertEqual(state_machine, sm1)

    def test__get_state_machine__throws_error_for_two_matching_state_machines(self):
        sm1 = self.construct_state_machine_item("blah-blah-1state_machine")
        sm2 = self.construct_state_machine_item("blah-blah-21state_machine")

        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            with self.assertRaises(AttributeError):
                get_state_machine(name="1state_machine", region=self.DEFAULT_REGION)

    def test__get_state_machine__throws_error_for_no_matching_state_machine(self):
        sm1 = self.construct_state_machine_item("blah-blah-state_machine1")
        sm2 = self.construct_state_machine_item("blah-blah-state_machine2")

        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})

            with self.assertRaises(ResourceNotFoundError):
                get_state_machine(name="state_machine3", region=self.DEFAULT_REGION)

    def test__get_state_machines__returns_all(self):
        sm1 = self.construct_state_machine_item("sm1")
        sm2 = self.construct_state_machine_item("sm2")

        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            state_machines = get_state_machines(region=self.DEFAULT_REGION)
            self.assertListEqual(state_machines, [sm1, sm2])

    def test__get_state_machines__returns_only_env_base_prefixed_state_machines(self):
        sm1 = self.construct_state_machine_item("dev-marmot-sm1")
        sm2 = self.construct_state_machine_item("dev-marmot-sm2")
        sm3 = self.construct_state_machine_item("prod-marmot-sm3")
        sm4 = self.construct_state_machine_item("sm4-dev-marmot")

        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(
                self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2, sm3, sm4]}
            )
            state_machines = get_state_machines(
                env_base=EnvBase("dev-marmot"), region=self.DEFAULT_REGION
            )
            self.assertListEqual(state_machines, [sm1, sm2])

    def test__start_execution__starts_execution(self):
        sm_name = "state_machine1"
        sm1 = self.construct_state_machine_item(sm_name)
        sm2 = self.construct_state_machine_item("state_machine2")
        sm_input = "{}"
        execution_name = build_execution_name(sm_input)
        expected_exec_arn = self.create_execution_arn(sm_name, execution_name)
        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            sfn_stubber.add_response(
                self.START_EXECUTION,
                {"executionArn": expected_exec_arn, "startDate": datetime.now()},
            )
            execution_arn = start_execution(
                state_machine_name=sm_name,
                state_machine_input="{}",
                region=self.DEFAULT_REGION,
            )
            self.assertEqual(execution_arn, expected_exec_arn)

    def test__start_execution__starts_execution__existing_execution_arn_used(self):
        sm_name = "state_machine1"
        sm1 = self.construct_state_machine_item(sm_name)
        sm2 = self.construct_state_machine_item("state_machine2")
        sm_input = "{}"
        execution_name = build_execution_name(sm_input)
        expected_exec_arn = self.create_execution_arn(sm_name, execution_name)
        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            sfn_stubber.add_client_error(
                self.START_EXECUTION,
                "ExecutionAlreadyExists",
                f"Execution Already Exists: '{expected_exec_arn}'",
            )
            execution_arn = start_execution(
                state_machine_name=sm_name,
                state_machine_input="{}",
                region=self.DEFAULT_REGION,
                reuse_existing_execution=True,
            )
            self.assertEqual(execution_arn, expected_exec_arn)

    def test__start_execution__starts_execution_forced(self):
        sm_name = "state_machine1"
        sm1 = self.construct_state_machine_item(sm_name)
        sm2 = self.construct_state_machine_item("state_machine2")
        sm_input = "{}"
        execution_name = build_execution_name(sm_input)
        expected_exec_arn = self.create_execution_arn(sm_name, execution_name)
        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            sfn_stubber.add_client_error(
                self.START_EXECUTION,
                "ExecutionAlreadyExists",
                f"Execution Already Exists: '{expected_exec_arn}'",
            )
            sfn_stubber.add_response(
                self.START_EXECUTION,
                {
                    "executionArn": expected_exec_arn + "1234",
                    "startDate": datetime.now(),
                },
            )
            execution_arn = start_execution(
                state_machine_name=sm_name,
                state_machine_input="{}",
                region=self.DEFAULT_REGION,
            )
            self.assertNotEqual(execution_arn, expected_exec_arn)

    def test__start_execution__fails_due_to_unrelated_failures(self):
        sm_name = "state_machine1"
        sm1 = self.construct_state_machine_item(sm_name)
        sm2 = self.construct_state_machine_item("state_machine2")
        with self.stub(self.sfn) as sfn_stubber:
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            sfn_stubber.add_client_error(self.START_EXECUTION, "StateMachineDoesNotExist")
            sfn_stubber.add_response(self.LIST_STATE_MACHINES, {"stateMachines": [sm1, sm2]})
            sfn_stubber.add_client_error(self.START_EXECUTION, "AnotherError")
            with self.assertRaises(ResourceNotFoundError):
                start_execution(
                    state_machine_name=sm_name,
                    state_machine_input="{}",
                    region=self.DEFAULT_REGION,
                )
            with self.assertRaises(AWSError):
                start_execution(
                    state_machine_name=sm_name,
                    state_machine_input="{}",
                    region=self.DEFAULT_REGION,
                )

    def construct_execution(
        self,
        name: str,
        exec_name: str,
        status: str = "RUNNING",
        start_date: datetime = datetime(2022, 3, 22, 0, 0),
    ):
        return ExecutionListItemTypeDef(
            **remove_null_values(
                dict(
                    stateMachineArn=self.create_state_machine_arn(name),
                    executionArn=self.create_execution_arn(name, exec_name),
                    name=exec_name,
                    status=status,
                    startDate=start_date,
                    stopDate=start_date + timedelta(minutes=30) if status != "RUNNING" else None,
                )
            )
        )

    def construct_state_machine_item(self, name: str):
        return StateMachineListItemTypeDef(
            stateMachineArn=self.create_state_machine_arn(name),
            name=name,
            type="STANDARD",
            creationDate=datetime.now(),
        )

    def create_state_machine_arn(self, name: str):
        return StateMachineArn.from_components(
            state_machine_name=name,
            region=self.DEFAULT_REGION,
            account_id=self.ACCOUNT_ID,
        )

    def create_execution_arn(self, name: str, exec_name: str):
        return ExecutionArn.from_components(
            state_machine_name=name,
            execution_name=exec_name,
            region=self.DEFAULT_REGION,
            account_id=self.ACCOUNT_ID,
        )
