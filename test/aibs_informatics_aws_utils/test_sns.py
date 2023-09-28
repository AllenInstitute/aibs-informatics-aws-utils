from test.aibs_informatics_aws_utils.base import AwsBaseTest

from aibs_informatics_core.models.aws.sns import SNSTopicArn
from moto import mock_sns, mock_sts

from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_aws_utils.sns import get_sns_client, publish, publish_to_topic


@mock_sns
@mock_sts
class SNSTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_aws_credentials()

    def test__publish_to_topic__publishes_message(self):
        sns = get_sns_client()
        topic_arn = sns.create_topic(Name="test_topic")["TopicArn"]
        response = publish_to_topic(
            topic_arn=SNSTopicArn(topic_arn),
            message="test_message",
            subject="test_subject",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test__publish_to_topic__fails_for_invalid_topic(self):
        sns = get_sns_client()
        topic_arn = sns.create_topic(Name="test_topic")["TopicArn"]
        sns.delete_topic(TopicArn=topic_arn)

        with self.assertRaises(AWSError):

            publish_to_topic(
                topic_arn=SNSTopicArn(topic_arn),
                message="test_message",
                subject="test_subject",
            )

    def test__publish__fails_if_no_target_specified(self):
        with self.assertRaises(ValueError):
            publish(
                topic_arn=None,
                message="test_message",
                subject="test_subject",
            )
