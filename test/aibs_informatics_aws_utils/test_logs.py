from aibs_informatics_aws_utils.logs import build_log_stream_url
from test.aibs_informatics_aws_utils.base import AwsBaseTest


class LogsTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_env_base_env_var()
        self.set_region(self.DEFAULT_REGION)

    def test__build_log_stream_url__works(self):
        log_stream_name = "weird-_)(name"
        log_group_name = "/aws/batch/job"
        obtained = build_log_stream_url(
            log_group_name=log_group_name, log_stream_name=log_stream_name
        )
        expected = "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Faws$252Fbatch$252Fjob/log-events/weird-_$2529$2528name"  # noqa: E501
        assert obtained == expected
