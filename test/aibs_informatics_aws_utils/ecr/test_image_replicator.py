import json

import boto3
import moto

from aibs_informatics_aws_utils.ecr.core import ECRImage, ECRRepository
from aibs_informatics_aws_utils.ecr.image_replicator import (
    ECRImageReplicator,
    ReplicateImageRequest,
)
from test.aibs_informatics_aws_utils.base import AwsBaseTest
from test.aibs_informatics_aws_utils.ecr.base import ECRTestBase


class ECRImageReplicatorTestsStubbing(AwsBaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_aws_credentials()

    def test__process_request__happy_case(self):
        # Create a fake ECR client and stub it using the provided stub method
        fake_client = boto3.client("ecr", region_name="us-west-2")
        stubber = self.stub(fake_client)

        # Define a minimal valid image manifest
        manifest = json.dumps(
            {
                "schemaVersion": 2,
                "config": {
                    "digest": "sha256:configdigest",
                    "size": 123,
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                },
                "layers": [],
            }
        )

        # Stub the batch_check_layer_availability call invoked during upload_layers
        expected_params = {
            "repositoryName": "destination-repo",
            "layerDigests": ["sha256:configdigest"],
        }
        response1 = {"layers": [{"layerAvailability": "AVAILABLE"}]}
        stubber.add_response("batch_check_layer_availability", response1, expected_params)

        # Stub the put_image call for dest_image.put_image(None)
        expected_params2 = {
            "registryId": "222222222222",
            "repositoryName": "destination-repo",
            "imageManifest": manifest,
            "imageDigest": "sha256:" + "1234" * 16,
        }
        stubber.add_response("put_image", {}, expected_params2)

        # Stub the put_image call for adding the image tag (dest_image.add_image_tags)
        expected_params3 = {
            "registryId": "222222222222",
            "repositoryName": "destination-repo",
            "imageManifest": manifest,
            "imageDigest": "sha256:" + "1234" * 16,
            "imageTag": "latest",
        }
        stubber.add_response("put_image", {}, expected_params3)

        # Stub the batch_get_image call invoked in the destination ECRImage constructor
        expected_params_batch_get = {
            "repositoryName": "destination-repo",
            "registryId": "222222222222",
            "imageIds": [{"imageDigest": "sha256:" + "1234" * 16}],
        }
        response_batch_get = {"images": [{"imageManifest": manifest}]}
        stubber.add_response("batch_get_image", response_batch_get, expected_params_batch_get)

        stubber.activate()

        # Create dummy source image and destination repository with client passed in at init
        source_image = ECRImage(
            account_id="111111111111",
            region="us-west-2",
            repository_name="source-repo",
            image_digest="sha256:" + "1234" * 16,
            image_manifest=manifest,
            client=fake_client,
        )

        destination_repository = ECRRepository(
            account_id="222222222222",
            region="us-west-2",
            repository_name="destination-repo",
            client=fake_client,
        )

        # Create the replicator and build the request
        replicator = ECRImageReplicator()
        request = ReplicateImageRequest(
            source_image=source_image,
            destination_repository=destination_repository,
            destination_image_tags=["latest"],
        )

        # Act: process the replication request
        response = replicator.process_request(request)

        # Assert: verify the destination image has the expected attributes
        self.assertEqual(response.destination_image.account_id, "222222222222")
        self.assertEqual(response.destination_image.region, "us-west-2")
        self.assertEqual(response.destination_image.repository_name, "destination-repo")
        self.assertEqual(response.destination_image.image_digest, "sha256:" + "1234" * 16)

        stubber.assert_no_pending_responses()


@moto.mock_aws
class ECRImageReplicatorTests(ECRTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.replicator = ECRImageReplicator()

    def test__process_request__happy_case(self):
        repo = self.create_repository("source")
        image = self.put_image(repo.repository_name, image_tag="latest")
        destination_repo = self.create_repository("destination")

        # Unfortunately, moto doesn't support the ECR API call to batch check layer availability
        # so we will catch that it fails with NotImplementedError exception
        with self.assertRaises(NotImplementedError):
            self.replicator.process_request(
                ReplicateImageRequest(
                    source_image=image,
                    destination_repository=destination_repo,
                )
            )

    def test___upload_layers__happy_case(self):
        repo = self.create_repository("source")
        image = self.put_image(repo.repository_name, image_tag="latest")
        destination_repo = self.create_repository("destination")
        image_layers = image.get_image_layers()

        # Unfortunately, moto doesn't support the ECR API call to get download layer
        # so we will catch that it fails with NotImplementedError exception
        with self.assertRaises(NotImplementedError):
            self.replicator._upload_layers(
                repo, destination_repo, image_layers, check_if_exists=False
            )
