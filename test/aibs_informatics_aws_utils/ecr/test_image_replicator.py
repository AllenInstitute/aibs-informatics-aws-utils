from test.aibs_informatics_aws_utils.ecr.base import ECRTestBase

from moto import mock_ecr, mock_sts

from aibs_informatics_aws_utils.ecr import ECRImageReplicator
from aibs_informatics_aws_utils.ecr.image_replicator import ReplicateImageRequest


@mock_sts
@mock_ecr
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
