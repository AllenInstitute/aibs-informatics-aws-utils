import urllib.parse
from typing import Optional

from aibs_informatics_core.utils.logging import get_logger

from aibs_informatics_aws_utils.core import AWSService, get_region

logger = get_logger(__name__)


get_logs_client = AWSService.LOGS.get_client


def build_log_stream_url(
    log_group_name: str,
    log_stream_name: str,
    region: Optional[str] = None,
) -> str:
    def _special_escape(string: str) -> str:
        return urllib.parse.quote(string, safe="").replace("%", "$25")

    region = get_region(region)
    log_group_name = _special_escape(log_group_name)
    log_stream_name = _special_escape(log_stream_name)

    log_url_prefix = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group/"
    return f"{log_url_prefix}{log_group_name}/log-events/{log_stream_name}"
