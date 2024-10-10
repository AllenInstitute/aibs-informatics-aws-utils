import decimal
import json
from hashlib import md5
from test.aibs_informatics_aws_utils.base import AwsBaseTest

import moto
import pytest
from aibs_informatics_test_resources import does_not_raise

from aibs_informatics_aws_utils.exceptions import AWSError
from aibs_informatics_aws_utils.sqs import (
    delete_from_queue,
    get_sqs_client,
    send_sqs_message,
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
        with moto.mock_aws():
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
        with moto.mock_aws():
            sqs = self.sqs_client
            queue_name = f"{self.env_base}-demand_request_queue"
            sqs.create_queue(QueueName=queue_name)
            payload = dict(test="message")

            response = send_to_dispatch_queue(payload, self.env_base)

            self.assertEqual(md5(json.dumps(payload).encode()).hexdigest(), response)

    def test__send_to_dispatch_queue__fails_targeting_non_existent_queue(self):
        with moto.mock_aws(), self.assertRaises(AWSError):
            payload = dict(test="message")
            send_to_dispatch_queue(payload, self.env_base)


@pytest.mark.parametrize(
    "queue_create_args, queue_name, payload, message_deduplication_id, "
    "message_group_id, raise_expectation, expected_response",
    [
        pytest.param(
            # queue_to_create
            {
                "QueueName": "normal-sqs-queue",
                "Attributes": {
                    "FifoQueue": "false",
                    "ContentBasedDeduplication": "false",
                },
            },
            # queue_name
            "normal-sqs-queue",
            # payload
            {"test": "message"},
            # message_deduplication_id
            None,
            # message_group_id
            None,
            # raise_expectation
            does_not_raise(),
            # expected_response
            md5(json.dumps({"test": "message"}).encode()).hexdigest(),
            id="Test send sqs message to basic queue",
        ),
        pytest.param(
            # queue_to_create
            {
                "QueueName": "normal-sqs-queue",
                "Attributes": {
                    "FifoQueue": "false",
                    "ContentBasedDeduplication": "false",
                },
            },
            # queue_name
            "normal-sqs-queue",
            # payload
            {"test": decimal.Decimal(1)},
            # message_deduplication_id
            None,
            # message_group_id
            None,
            # raise_expectation
            does_not_raise(),
            # expected_response
            md5(json.dumps({"test": "1"}).encode()).hexdigest(),
            id="Test send sqs message can handle payload with Decimal values",
        ),
        pytest.param(
            # queue_to_create
            {
                "QueueName": "normal-sqs-queue",
                "Attributes": {
                    "FifoQueue": "false",
                    "ContentBasedDeduplication": "false",
                },
            },
            # queue_name
            "non-existent-queue",
            # payload
            None,
            # message_deduplication_id
            None,
            # message_group_id
            None,
            # raise_expectation
            pytest.raises(AWSError, match=r"Could not find SQS queue .+"),
            # expected_response
            None,
            id="Test error raised if non-existent queue_name specified",
        ),
        pytest.param(
            # queue_to_create
            {
                "QueueName": "test-fifo-queue.fifo",
                "Attributes": {
                    "FifoQueue": "true",
                    "ContentBasedDeduplication": "false",
                },
            },
            # queue_name
            "test-fifo-queue.fifo",
            # payload
            {"test": "message"},
            # message_deduplication_id
            None,
            # message_group_id
            None,
            # raise_expectation
            pytest.raises(RuntimeError, match=r".+ FIFO queue \*must\* include .+"),
            # expected_response
            None,
            id="Test error raised if message_group_id not provided for fifo queue",
        ),
    ],
)
def test__send_sqs_message(
    aws_credentials_fixture,
    queue_create_args,
    queue_name,
    payload,
    message_deduplication_id,
    message_group_id,
    raise_expectation,
    expected_response,
):
    with moto.mock_aws():
        # First setup test by creating our mock queue
        sqs = get_sqs_client()
        sqs.create_queue(**queue_create_args)

        with raise_expectation:
            obt = send_sqs_message(
                queue_name=queue_name,
                payload=payload,
                message_deduplication_id=message_deduplication_id,
                message_group_id=message_group_id,
            )

        if expected_response is not None:
            assert expected_response == obt
