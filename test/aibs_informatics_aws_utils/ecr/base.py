import hashlib
import json
import uuid
from test.aibs_informatics_aws_utils.base import AwsBaseTest
from typing import TYPE_CHECKING, List, Optional, Union

import boto3
from aibs_informatics_core.utils.tools.dicttools import remove_null_values
from moto import mock_ecr, mock_sts

from aibs_informatics_aws_utils.core import get_client
from aibs_informatics_aws_utils.ecr import ECRImage, ECRRepository, ResourceTag
from aibs_informatics_aws_utils.ecr.core import get_ecr_client

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ecr import ECRClient
    from mypy_boto3_ecr.type_defs import ImageDetailTypeDef, ImageIdentifierTypeDef, ImageTypeDef
else:
    ECRClient = object
    ImageDetailTypeDef = dict
    ImageIdentifierTypeDef = dict
    ImageTypeDef = dict


@mock_sts
@mock_ecr
class ECRTestBase(AwsBaseTest):
    IMAGE_MANIFEST_MEDIA_TYPE = "application/vnd.docker.distribution.manifest.v2+json"
    IMAGE_MANIFEST_CONFIG_MEDIA_TYPE = "application/vnd.docker.container.image.v1+json"
    IMAGE_MANIFEST_LAYER_MEDIA_TYPE = "application/vnd.docker.image.rootfs.diff.tar.gzip"

    def setUp(self) -> None:
        super().setUp()
        self.set_credentials()
        self.set_region()
        self.set_account_id()
        self.REGION = self.DEFAULT_REGION
        get_client.cache_clear()

    @property
    def ecr(self):
        return boto3.client("ecr", region_name=self.REGION)

    def construct_image_digest(self, value: Optional[str] = None) -> str:
        value = value or str(uuid.uuid4())
        return f"sha256:{hashlib.sha256(value.encode()).hexdigest()}"

    def construct_image_detail(
        self,
        repository_name: str,
        image_digest: str,
        image_tags: list = None,
    ) -> ImageDetailTypeDef:
        return ImageDetailTypeDef(
            registryId=self.ACCOUNT_ID,
            repositoryName=repository_name,
            imageTags=image_tags,
            imageDigest=image_digest,
            imageSizeInBytes=123,
            imageManifestMediaType=self.IMAGE_MANIFEST_MEDIA_TYPE,
        )

    def construct_image_manifest(
        self,
        layer_sizes: List[int] = None,
        config_size: int = 123,
        config_digest: str = None,
    ) -> str:
        return json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": self.IMAGE_MANIFEST_MEDIA_TYPE,
                "config": {
                    "mediaType": self.IMAGE_MANIFEST_CONFIG_MEDIA_TYPE,
                    "size": config_size,
                    "digest": config_digest or self.construct_image_digest(str(config_size)),
                },
                "layers": [
                    {
                        "mediaType": self.IMAGE_MANIFEST_LAYER_MEDIA_TYPE,
                        "size": layer_size,
                        "digest": self.construct_image_digest(str(layer_size)),
                    }
                    for layer_size in layer_sizes or []
                ],
            }
        )

    def construct_image(
        self,
        repository_name: str,
        image_id: ImageIdentifierTypeDef,
        image_manifest: Union[str, dict] = None,
    ) -> ImageTypeDef:
        if image_manifest is None:
            image_manifest = self.construct_image_manifest([123])

        if isinstance(image_manifest, dict):
            image_manifest = json.dumps(image_manifest)

        return ImageTypeDef(
            registryId=self.ACCOUNT_ID,
            repositoryName=repository_name,
            imageId=image_id,
            imageManifest=image_manifest,
            imageManifestMediaType=self.IMAGE_MANIFEST_MEDIA_TYPE,
        )

    def construct_image_identifier(
        self, image_digest: str = None, image_tag: str = None
    ) -> ImageIdentifierTypeDef:
        return ImageIdentifierTypeDef(
            remove_null_values(dict(imageDigest=image_digest, imageTag=image_tag))
        )

    def create_repository(self, repository_name: str, *repository_tags: ResourceTag):
        self.ecr.create_repository(
            repositoryName=repository_name,
            registryId=self.ACCOUNT_ID,
            tags=[_ for _ in repository_tags],
        )
        return ECRRepository(self.ACCOUNT_ID, self.REGION, repository_name)

    def put_image(
        self,
        repository_name: str,
        image_manifest: Optional[str] = None,
        image_digest: Optional[str] = None,
        image_tag: Optional[str] = None,
        seed: int = 123,
    ):
        image_digest = image_digest or self.construct_image_digest(str(seed))
        if not image_manifest:
            image_manifest = self.construct_image_manifest(
                [seed, seed + 1, seed + 2],
                config_size=seed,
                config_digest=image_digest,
            )
        image = self.construct_image(
            repository_name=repository_name,
            image_id=self.construct_image_identifier(
                image_tag=image_tag,
                image_digest=image_digest,
            ),
            image_manifest=image_manifest,
        )

        response = self.ecr.put_image(
            **remove_null_values(
                dict(
                    repositoryName=repository_name,
                    imageDigest=image.get("imageId", {}).get("imageDigest"),
                    imageManifest=image.get("imageManifest"),
                    imageTag=image.get("imageId", {}).get("imageTag"),
                    registryId=self.ACCOUNT_ID,
                )
            )
        )
        return ECRImage(
            account_id=response["image"].get("registryId"),
            region=self.REGION,
            repository_name=response["image"].get("repositoryName"),
            image_digest=response["image"].get("imageId", {}).get("imageDigest"),
            image_manifest=response["image"].get("imageManifest"),
        )
