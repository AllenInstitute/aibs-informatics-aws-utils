from test.aibs_informatics_aws_utils.base import AwsBaseTest

import moto

from aibs_informatics_aws_utils.secretsmanager import get_secret_value, get_secretsmanager_client


class SecretsManagerTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.US_WEST_2)

    @property
    def secretsmanager_client(self):
        return get_secretsmanager_client()

    def test__get_secret_value__fetches_valid_param(self):
        with moto.mock_aws():
            sm = self.secretsmanager_client
            name = "my-param-name"
            value = "my-value"
            sm.create_secret(Name=name, SecretString=value)

            actual = get_secret_value(name)
            self.assertEqual(value, actual)

    def test__get_secret_value__fetches_valid_value_as_dict(self):
        with moto.mock_aws():
            sm = self.secretsmanager_client
            name = "my-param-name"
            value = '{"a": 1, "b": 2}'
            expected = {"a": 1, "b": 2}
            sm.create_secret(Name=name, SecretString=value)

            actual = get_secret_value(name, as_dict=True)
            self.assertDictEqual(expected, actual)

    def test__get_secret_value__raises_error_on_missing(self):
        with moto.mock_aws():
            name = "my-name"

            with self.assertRaises(Exception):
                get_secret_value(name)
