import json
from test.aibs_informatics_aws_utils.base import AwsBaseTest

import moto

from aibs_informatics_aws_utils.ssm import (
    get_ssm_client,
    get_ssm_parameter,
    has_ssm_parameter,
    put_ssm_parameter,
)


class SSMTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.US_WEST_2)

    @property
    def ssm_client(self):
        return get_ssm_client()

    def test__get_ssm_parameter__fetches_valid_param(self):
        with moto.mock_aws():
            ssm = self.ssm_client
            param_name = "my-param-name"
            param_value = "my-value"
            param_type = "String"
            ssm.put_parameter(Name=param_name, Value=param_value, Type=param_type)

            actual_param_value = get_ssm_parameter(param_name)
            self.assertEqual(param_value, actual_param_value)

    def test__get_ssm_parameter__fetches_valid_param_as_dict(self):
        with moto.mock_aws():
            ssm = self.ssm_client
            param_name = "my-param-name"
            param_value = '{"a": 1, "b": 2}'
            param_type = "String"
            ssm.put_parameter(Name=param_name, Value=param_value, Type=param_type)

            actual_param_value = get_ssm_parameter(param_name, as_dict=True)
            self.assertDictEqual(json.loads(param_value), actual_param_value)

    def test__get_ssm_parameter__raises_error_on_missing(self):
        with moto.mock_aws():
            param_name = "my-param-name"
            with self.assertRaises(Exception):
                get_ssm_parameter(param_name)

    def test__put_ssm_parameter__sets_valid_param(self):
        with moto.mock_aws():
            ssm = self.ssm_client
            param_name = "my-param-name"
            param_value = "my-value"

            put_ssm_parameter(param_name, param_value=param_value)
            actual_param_value = ssm.get_parameter(Name=param_name)["Parameter"]["Value"]
            self.assertEqual(param_value, actual_param_value)

    def test__has_ssm_parameter__works_properly(self):
        with moto.mock_aws():
            # setup
            ssm = self.ssm_client

            param_name = "my-param-name"
            param_value = "my-value"
            param_type = "String"

            self.assertFalse(has_ssm_parameter(param_name))
            ssm.put_parameter(Name=param_name, Value=param_value, Type=param_type)
            self.assertTrue(has_ssm_parameter(param_name))
