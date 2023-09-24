import json
from hashlib import md5
from test.aibs_informatics_aws_utils.base import AwsBaseTest

import moto

from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_aws_utils.sqs import (
    delete_from_queue,
    get_sqs_client,
    send_to_dispatch_queue,
)


class SqsTests(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_region(self.US_WEST_2)

    @property
    def sqs_client(self):
        return get_sqs_client()

    def test__delete_from_queue__deletes_from_queue(self):
        with moto.mock_sqs():
            # setup
            sqs = self.sqs_client
            queue_name = "test-queue"
            queue = sqs.create_queue(QueueName=queue_name)
            queue_url = queue["QueueUrl"]
            sqs.send_message(QueueUrl=queue_url, MessageBody="message")

            receipt_response = sqs.receive_message(QueueUrl=queue_url)
            receipt_handle = receipt_response["Messages"][0]["ReceiptHandle"]

            delete_from_queue(
                queue_name=queue_name,
                receipt_handle=receipt_handle,
                region=self.US_WEST_2,
            )

    def test__send_to_dispatch_queue__succeeds(self):
        with moto.mock_sqs():
            sqs = self.sqs_client
            queue_name = f"{self.env_base}-demand_request_queue"
            sqs.create_queue(QueueName=queue_name)
            payload = dict(test="message")

            response = send_to_dispatch_queue(payload, self.env_base)

            self.assertEqual(md5(json.dumps(payload).encode()).hexdigest(), response)

    def test__send_to_dispatch_queue__fails_targeting_non_existent_queue(self):
        with moto.mock_sqs(), self.assertRaises(AWSError):
            payload = dict(test="message")
            send_to_dispatch_queue(payload, self.env_base)
