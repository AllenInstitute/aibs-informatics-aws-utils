import base64
import json
import logging
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypedDict,
    TypeVar,
    cast,
)

import requests
from aibs_informatics_core.collections import StrEnum, ValidatedStr
from aibs_informatics_core.models.base import PydanticBaseModel
from aibs_informatics_core.utils.logging import LoggingMixin
from botocore.exceptions import ClientError
from pydantic import Field, PrivateAttr, model_validator

from aibs_informatics_aws_utils.core import (
    AWSService,
    get_account_id,
    get_client_error_code,
    get_region,
)
from aibs_informatics_aws_utils.exceptions import (
    AWSError,
    InvalidAmazonResourceNameError,
    ResourceNotFoundError,
)

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ecr import ECRClient
    from mypy_boto3_ecr.literals import ImageTagMutabilityType, TagStatusType
    from mypy_boto3_ecr.type_defs import (
        BatchGetImageResponseTypeDef,
        ImageDetailTypeDef,
        ImageIdentifierTypeDef,
        LayerTypeDef,
        ListImagesFilterTypeDef,
        ListImagesRequestPaginateTypeDef,
        TagTypeDef,
    )
else:
    ECRClient = object
    ImageTagMutabilityType = object
    TagStatusType = object
    BatchGetImageResponseTypeDef = dict
    ImageDetailTypeDef = dict
    ImageIdentifierTypeDef = dict
    LayerTypeDef = dict
    ListImagesFilterTypeDef = dict
    ListImagesRequestPaginateTypeDef = dict
    TagTypeDef = dict

logger = logging.getLogger(__name__)


get_ecr_client = AWSService.ECR.get_client

ECR_REPO_ARN_PATTERN = re.compile(
    r"arn:aws:ecr:([\w-]*):([\d]{10,12}):repository/((?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*)"
)

ECR_REGISTRY_PATTERN_STR = r"([\d]{12}).dkr.ecr.([\w-]*).amazonaws.com"
ECR_REPO_PATTERN_STR = r"/((?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*)"
ECR_IMAGE_PATTERN_STR = r"(:([\w.\-_]{1,127})|@(sha256:[A-Fa-f0-9]{64}))"

ECR_REGISTRY_URI_PATTERN = re.compile(ECR_REGISTRY_PATTERN_STR)
ECR_REPO_URI_PATTERN = re.compile(ECR_REGISTRY_PATTERN_STR + ECR_REPO_PATTERN_STR)
ECR_IMAGE_URI_PATTERN = re.compile(
    ECR_REGISTRY_PATTERN_STR + ECR_REPO_PATTERN_STR + ECR_IMAGE_PATTERN_STR
)


class ECRRegistryUri(ValidatedStr):
    regex_pattern = ECR_REGISTRY_URI_PATTERN

    @property
    def account_id(self) -> str:
        return self.get_match_groups()[0]

    @property
    def region(self) -> str:
        return self.get_match_groups()[1]

    @classmethod
    def from_components(
        cls, account_id: str | None = None, region: str | None = None, **kwargs
    ) -> "ECRRegistryUri":
        """Generate a Registry URI.

        If account ID not provided, account Id of credentials is used.
        If region is not provided, region of credentials is used.

        Args:
            account_id: The registry ID. Defaults to None.
            region: AWS region. Defaults to None.
            **kwargs: Additional keyword arguments (unused).

        Returns:
            AWS Registry URI.
        """
        account_id = account_id or get_account_id()
        region = get_region(region)
        return ECRRegistryUri(f"{account_id}.dkr.ecr.{region}.amazonaws.com")


class ECRRepositoryUri(ECRRegistryUri):
    regex_pattern = ECR_REPO_URI_PATTERN

    @property
    def repository_name(self) -> str:
        return self.get_match_groups()[2]

    @classmethod
    def from_components(  # type: ignore[override]
        cls,
        repository_name: str,
        account_id: str | None = None,
        region: str | None = None,
    ) -> "ECRRepositoryUri":
        """Generate a Repository URI.

        If account ID not provided, account Id of credentials is used.
        If region is not provided, region of credentials is used.

        Args:
            repository_name: Name of ECR repository.
            account_id: The registry ID. Defaults to None.
            region: AWS region. Defaults to None.

        Returns:
            AWS Repository URI.
        """
        registry_uri = ECRRegistryUri.from_components(account_id=account_id, region=region)
        return ECRRepositoryUri(f"{registry_uri}/{repository_name}")


class ECRImageUri(ECRRepositoryUri):
    regex_pattern = ECR_IMAGE_URI_PATTERN

    @property
    def image_tag(self) -> str | None:
        return self.get_match_groups()[-2]

    @property
    def image_digest(self) -> str | None:
        return self.get_match_groups()[-1]

    @classmethod
    def from_components(  # type: ignore[override]
        cls,
        repository_name: str,
        image_tag: str | None = None,
        image_digest: str | None = None,
        account_id: str | None = None,
        region: str | None = None,
    ) -> "ECRImageUri":
        """Generate an Image URI.

        If account ID not provided, account Id of credentials is used.
        If region is not provided, region of credentials is used.

        Args:
            repository_name: Name of ECR repository.
            image_tag: Tag associated with image. Defaults to None.
            image_digest: The image digest. Defaults to None.
            account_id: The registry ID. Defaults to None.
            region: AWS region. Defaults to None.

        Raises:
            ValueError: If both or neither image tag / image digest are provided.

        Returns:
            ECR Image URI.
        """
        if (image_tag and image_digest) or (not image_tag and not image_digest):
            raise ValueError(
                "Must provide EITHER image tag OR image digest. "
                f"image_tag={image_tag}, image_digest={image_digest}"
            )
        repo_uri = ECRRepositoryUri.from_components(
            repository_name=repository_name, account_id=account_id, region=region
        )
        image_id = f"{'@' if image_digest else ':'}{image_digest or image_tag}"
        return ECRImageUri(f"{repo_uri}{image_id}")


@dataclass
class ECRLogin:
    username: str
    password: str
    registry: str

    @property
    def auth_token(self) -> str:
        decoded_auth_token = f"{self.username}:{self.password}"
        return base64.b64encode(decoded_auth_token.encode()).decode()


class LifecyclePolicySelection(PydanticBaseModel):
    tag_status: Literal["tagged", "untagged", "any"]
    tag_prefix_list: list[str] | None
    count_type: Literal["imageCountMoreThan", "sinceImagePushed"]
    count_number: int
    count_unit: Literal["days"] | None = None

    @model_validator(mode="after")
    def _validate_fields(self):
        if self.count_type != "sinceImagePushed" and self.count_unit is not None:
            # https://docs.aws.amazon.com/AmazonECR/latest/userguide/LifecyclePolicies.html#lp_count_unit
            raise ValueError(f"Cannot specify 'countUnit' when countType={self.count_type}")
        if self.tag_status == "tagged" and not self.tag_prefix_list:
            # https://docs.aws.amazon.com/AmazonECR/latest/userguide/LifecyclePolicies.html#lp_tag_prefix_list
            raise ValueError(f"Must specify 'tagPrefixList' when tagStatus={self.tag_status}")
        return self

    def to_dict(self, **kwargs):
        kwargs.setdefault("by_alias", True)
        return super().to_dict(**kwargs)


class LifecyclePolicyAction(PydanticBaseModel):
    type: Literal["expire"] = "expire"

    def to_dict(self, **kwargs):
        kwargs.setdefault("by_alias", True)
        return super().to_dict(**kwargs)


class LifecyclePolicyRule(PydanticBaseModel):
    rule_priority: int = Field(serialization_alias="rulePriority", validation_alias="rulePriority")
    description: str
    selection: LifecyclePolicySelection
    action: LifecyclePolicyAction = Field(default_factory=LifecyclePolicyAction)

    @classmethod
    def REMOVE_UNTAGGED(cls, rule_priority: int = 1, days: int = 14) -> "LifecyclePolicyRule":
        return LifecyclePolicyRule(
            rule_priority=rule_priority,
            description=f"Remove untagged images after {days} days",
            selection=LifecyclePolicySelection(
                tag_status="untagged",
                tag_prefix_list=None,  # type: ignore[arg-type]
                count_type="sinceImagePushed",
                count_number=days,
                count_unit="days",
            ),
            action=LifecyclePolicyAction(type="expire"),
        )

    def to_dict(self, **kwargs):
        kwargs.setdefault("by_alias", True)
        return super().to_dict(**kwargs)


class LifecyclePolicy(PydanticBaseModel):
    rules: list[LifecyclePolicyRule]

    def model_post_init(self, __context):
        self.rules = self.reprioritize_rules(self.rules, in_place=True)

    @staticmethod
    def reprioritize_rules(
        rules: list[LifecyclePolicyRule], in_place: bool = False
    ) -> list[LifecyclePolicyRule]:
        rule_priority = 0

        sorted_rules: list[LifecyclePolicyRule]

        def sort_key(_):
            return _.rule_priority

        if in_place:
            rules.sort(key=sort_key)
            sorted_rules = rules
        else:
            sorted_rules = sorted(deepcopy(rules), key=sort_key)

        for rule in sorted_rules:
            if rule.rule_priority <= rule_priority:
                rule_priority += 1
                rule.rule_priority = rule_priority
            else:
                rule_priority = rule.rule_priority
        return sorted_rules

    @classmethod
    def from_rules(cls, *rules: LifecyclePolicyRule) -> "LifecyclePolicy":
        return LifecyclePolicy(rules=list(map(deepcopy, rules)))

    def to_dict(self, **kwargs):
        kwargs.setdefault("by_alias", True)
        return super().to_dict(**kwargs)


class ResourceTag(TypedDict):
    Key: str
    Value: str


class TagMode(StrEnum):
    OVERWRITE = "overwrite"
    APPEND = "append"


T = TypeVar("T", bound="ECRMixins")


class ECRMixins(LoggingMixin):

    @property
    def client(self) -> ECRClient:
        if self._client is None:
            self._client = get_ecr_client(getattr(self, "region"))
        return self._client

    @client.setter
    def client(self, value: ECRClient):
        self._client = value  # pragma: no cover

    @property
    def uri(self) -> str:
        raise NotImplementedError("")  # pragma: no cover

    @classmethod
    def from_uri(cls: type[T], uri: str) -> T:
        raise NotImplementedError("")  # pragma: no cover


class ECRImage(PydanticBaseModel, ECRMixins):
    account_id: str
    region: str
    repository_name: str
    image_digest: str
    # https://distribution.github.io/distribution/spec/manifest-v2-2/#image-manifest-field-descriptions
    image_manifest: str | None = Field(default=None, repr=False)
    _client: ECRClient | None = PrivateAttr(default=None)

    # NOTE: __init__ override is used instead of model_post_init because
    #       ResourceNotFoundError is a ValueError subclass, which model_post_init
    #       would wrap in a ValidationError.
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        if self.image_manifest is None:
            response = self.client.batch_get_image(
                repositoryName=self.repository_name,
                registryId=self.account_id,
                imageIds=[ImageIdentifierTypeDef(imageDigest=self.image_digest)],
            )
            if len(response["images"]) == 0 or "imageManifest" not in response["images"][0]:
                raise ResourceNotFoundError(f"Could not resolve image manifest for {self.uri}")

            self.image_manifest = response["images"][0]["imageManifest"]

    @property
    def image_pushed_at(self) -> datetime | None:
        return self.get_image_detail().get("imagePushedAt")

    @property
    def image_tags(self) -> list[str]:
        image_detail = self.get_image_detail()
        return image_detail.get("imageTags", [])

    @property
    def uri(self) -> str:
        return ECRImageUri.from_components(
            repository_name=self.repository_name,
            image_digest=self.image_digest,
            account_id=self.account_id,
            region=self.region,
        )

    def get_repository(self) -> "ECRRepository":
        return ECRRepository(
            account_id=self.account_id,
            region=self.region,
            repository_name=self.repository_name,
        )

    def get_image_detail(self) -> ImageDetailTypeDef:
        """Get image detail of this image from ECR.

        Returns:
            Image detail dictionary.
        """
        response = self.client.describe_images(
            repositoryName=self.repository_name,
            registryId=self.account_id,
            imageIds=[ImageIdentifierTypeDef(imageDigest=self.image_digest)],
        )
        image_details = response["imageDetails"]
        if len(image_details) == 0:
            raise ResourceNotFoundError(
                f"Could not resolve image detail for {self}"
            )  # pragma: no cover
        return image_details[0]

    def get_image_layers(self) -> list[LayerTypeDef]:
        """Get layers from image manifest into ECR Layer objects.

        The schema of the image manifest layers is defined here:
        https://distribution.github.io/distribution/spec/manifest-v2-2/#image-manifest-field-descriptions

        Note:
            While docker image manifests can have multiple formats, ECR only supports
            the schema defined in the link above, a v2 single image manifest. There is
            a manifest list, that describes multiple architectures, but ECR does not support
            this. This method assumes the image manifest is in the correct format.

        Returns:
            List of ECR Image layers.
        """
        image_manifest = json.loads(self.image_manifest)

        return [
            LayerTypeDef(
                layerDigest=layer["digest"],
                layerAvailability="AVAILABLE",
                layerSize=layer["size"],
                mediaType=layer["mediaType"],
            )
            for layer in image_manifest["layers"]
        ]

    def get_image_config_layer(self) -> LayerTypeDef:
        """Get the image config layer from image manifest.

        The schema of the image manifest config layer is defined here:
        https://distribution.github.io/distribution/spec/manifest-v2-2/#image-manifest-field-descriptions

        Note:
            While docker image manifests can have multiple formats, ECR only supports
            the schema defined in the link above, a v2 single image manifest. There is
            a manifest list, that describes multiple architectures, but ECR does not support
            this. This method assumes the image manifest is in the correct format.

        Returns:
            Layer Type dict of the config object.
        """
        image_manifest = json.loads(self.image_manifest)
        layer = image_manifest["config"]
        return LayerTypeDef(
            layerDigest=layer["digest"],
            layerAvailability="AVAILABLE",
            layerSize=layer["size"],
            mediaType=layer["mediaType"],
        )

    def get_image_config(self) -> dict[str, Any]:
        """Get ECR or docker image configuration json metadata.

        Returns:
            Dictionary with image configuration.
        """
        # get image_manifest (sha256: hash)
        config_digest = json.loads(self.image_manifest)["config"]["digest"]
        registry = ECRRegistry(account_id=self.account_id, region=self.region)
        ecr_login = registry.get_ecr_login()

        response = requests.get(
            url=f"https://{registry.uri}/v2/{self.repository_name}/blobs/{config_digest}",
            headers={"Authorization": f"Basic {ecr_login.auth_token}"},
        )
        response.raise_for_status()
        return response.json()

    def add_image_tags(self, *image_tags: str):
        """Add tags to image.

        Args:
            *image_tags: Tags to add to image.
        """
        self.logger.info(f"Adding tags={image_tags} to {self.uri}")
        for tag in image_tags:
            self.put_image(image_tag=tag)

    def put_image(self, image_tag: str | None):
        """Make a call to put_image API to add image to ECR repository.

        This method will add an image to the ECR repository. If the image already exists,
        it will not raise an error. Instead, it will log that the image already
        exists with the given tag.

        Note:
            This operation does not push an image to the repository. It only adds
            the image manifest to the repository.

        Args:
            image_tag: Tag to associate with image. If None, image is untagged.
        """
        try:
            if image_tag is None:
                self.client.put_image(
                    registryId=self.account_id,
                    repositoryName=self.repository_name,
                    imageManifest=self.image_manifest,
                    imageDigest=self.image_digest,
                )
            else:
                self.client.put_image(
                    registryId=self.account_id,
                    repositoryName=self.repository_name,
                    imageManifest=self.image_manifest,
                    imageDigest=self.image_digest,
                    imageTag=image_tag,
                )
        except ClientError as e:
            # IF we receive a ImageAlreadyExistsException,
            # then we have nothing to worry about. Otherwise, raise error.
            if get_client_error_code(e) != "ImageAlreadyExistsException":
                raise e
            self.logger.info(f"Image already exists with tag={image_tag}")
        else:
            self.logger.info(f"Added new image with tag={image_tag}")

    @classmethod
    def from_repository_uri(cls, repository_uri: str, image_digest: str) -> "ECRImage":
        repo_uri = ECRRepositoryUri(repository_uri)
        return ECRImage.from_uri(f"{repo_uri}@{image_digest}")

    @classmethod
    def from_uri(cls, uri: str) -> "ECRImage":
        image_uri = ECRImageUri(uri)

        account_id = image_uri.account_id
        region = image_uri.region
        repo_name = image_uri.repository_name
        image_tag = image_uri.image_tag
        image_digest = image_uri.image_digest

        ecr = get_ecr_client(region)
        if image_digest is None:
            image_details = ecr.describe_images(
                repositoryName=repo_name,
                registryId=account_id,
                imageIds=[ImageIdentifierTypeDef(imageTag=image_tag)] if image_tag else [],
            )["imageDetails"][0]
            assert "imageDigest" in image_details
            image_digest = image_details["imageDigest"]

        return ECRImage(
            account_id=account_id,
            region=region,
            repository_name=repo_name,
            image_digest=image_digest,
        )

    def __repr__(self) -> str:
        return (
            f"ECRImage("
            f"account_id='{self.account_id}', "
            f"region='{self.region}', "
            f"repository_name='{self.repository_name}', "
            f"image_digest={self.image_digest[7:15]}..{self.image_digest[-4:]})"
        )


class ECRResource(PydanticBaseModel, ECRMixins):
    account_id: str
    region: str
    _client: ECRClient | None = PrivateAttr(default=None)

    @property
    def arn(self) -> str:
        return f"arn:aws:ecr:{self.region}:{self.account_id}"

    def get_resource_tags(self) -> list[ResourceTag]:
        """Gets the tags for this ECR Resource.

        Returns:
            List of tags.
        """
        return [
            ResourceTag(Key=tag["Key"], Value=tag["Value"])
            for tag in self.client.list_tags_for_resource(resourceArn=self.arn)["tags"]
            if "Key" in tag and "Value" in tag
        ]

    def update_resource_tags(
        self, *tags: ResourceTag, mode: TagMode = cast(TagMode, TagMode.APPEND)
    ):
        """Updates the tags for an ECR Resource.

        An update can either append or overwrite the existing tags.

        Args:
            *tags: Resource tags to update.
            mode: Either append or overwrite tags of resource.
                Defaults to TagMode.APPEND.
        """
        tag_dict = {tag["Key"]: tag["Value"] for tag in tags}
        if mode == TagMode.OVERWRITE:
            existing_tags = self.get_resource_tags()
            tag_keys_to_remove = [
                existing_tag["Key"]
                for existing_tag in existing_tags
                if existing_tag["Key"] not in tag_dict
            ]
            self.client.untag_resource(resourceArn=self.arn, tagKeys=tag_keys_to_remove)
        self.client.tag_resource(
            resourceArn=self.arn,
            tags=[TagTypeDef(Key=key, Value=value) for key, value in tag_dict.items()],
        )


class ECRRepository(ECRResource):
    repository_name: str

    @property
    def arn(self) -> str:
        return f"{super().arn}:repository/{self.repository_name}"

    @property
    def uri(self) -> str:
        return ECRRepositoryUri.from_components(
            repository_name=self.repository_name,
            account_id=self.account_id,
            region=self.region,
        )

    def create(
        self,
        tags: list[ResourceTag] | None = None,
        image_tag_mutability: ImageTagMutabilityType = "MUTABLE",
        exists_ok: bool = True,
    ):
        """Create an ECR Repository.

        Args:
            tags: List of repo tags to add. Defaults to None.
            image_tag_mutability: Whether image tag is immutable. Defaults to "MUTABLE".
            exists_ok: Suppress error if repository already exists. Defaults to True.
        """
        if tags is None:
            tags = []

        try:
            self.client.create_repository(
                registryId=self.account_id,
                repositoryName=self.repository_name,
                tags=tags,
                imageTagMutability=image_tag_mutability,
            )
        except ClientError as e:
            # If the repo already exists, just move on.
            if get_client_error_code(e) == "RepositoryAlreadyExistsException" and exists_ok:
                # update tags
                if tags:
                    self.update_resource_tags(*tags)
            else:
                raise e

    def exists(self) -> bool:
        """Check if repository exists.

        Returns:
            True if repository exists.
        """
        try:
            self.client.describe_repositories(
                registryId=self.account_id, repositoryNames=[self.repository_name]
            )
        except ClientError as e:
            if get_client_error_code(e) == "RepositoryNotFoundException":
                return False
            else:
                raise e
        else:
            return True

    def put_lifecycle_policy(self, lifecycle_policy: LifecyclePolicy):
        self.client.put_lifecycle_policy(
            repositoryName=self.repository_name,
            lifecyclePolicyText=lifecycle_policy.to_json(sort_keys=True),
            registryId=self.account_id,
        )

    def delete(self, force: bool):
        """Delete the ECR repository described by this instance.

        Args:
            force: Ignore if images in repository.
        """
        self.client.delete_repository(
            registryId=self.account_id, repositoryName=self.repository_name, force=force
        )

    def get_image(
        self, image_tag: str | None = None, image_digest: str | None = None
    ) -> "ECRImage":
        """Get the image associated with the following tag or digest.

        Args:
            image_tag: Image tag. Defaults to None.
            image_digest: Image digest. Defaults to None.

        Returns:
            The image with the image tag or digest.
        """
        if (image_tag is None) == (image_digest is None):
            raise ValueError(
                f"Must provide image tag XOR digest. "
                f"provided image_tag={image_tag}, image_digest={image_digest}"
            )
        images = self.get_images()
        for image in images:
            if image_tag and image_tag in image.image_tags:
                return image
            elif image_digest and image_digest == image.image_digest:
                return image
        else:
            raise ResourceNotFoundError(
                f"Could not find an image in {self.uri} with tag={image_tag}"
            )

    def get_images(self, tag_status: TagStatusType = "ANY") -> list["ECRImage"]:
        """Fetches all images in a given repository.

        Args:
            tag_status: Filter non-tagged images. Defaults to "ANY".

        Returns:
            List of images in repository.
        """

        # To fetch necessary image info, we need to make two calls:
        #   1. Call the `list_images` API which will return imageDigest/imageTag.
        #       - This is a lightweight response which gives us digests
        #   2. Call the `batch_get_image` API which returns imageManifest info.
        #       - this is consolidated all in one go

        # Call 1: list_images
        paginator = self.client.get_paginator("list_images")
        list_image_request = ListImagesRequestPaginateTypeDef(
            repositoryName=self.repository_name,
            filter=ListImagesFilterTypeDef(tagStatus=tag_status),
        )
        image_digests: list[str] = sorted(
            list(
                {
                    image_id["imageDigest"]
                    for list_images_response in paginator.paginate(**list_image_request)
                    for image_id in list_images_response["imageIds"]
                    if "imageDigest" in image_id
                }
            )
        )
        if len(image_digests) == 0:
            return []

        # Call 2: batch_get_image
        response: BatchGetImageResponseTypeDef = self.client.batch_get_image(
            repositoryName=self.repository_name,
            registryId=self.account_id,
            imageIds=[ImageIdentifierTypeDef(imageDigest=digest) for digest in image_digests],
        )

        # Next we consolidate the results, ensuring that the image manifests
        # are all the same. If an image digest has differing manifests,
        # we should throw an error.
        digest_to_manifest_map: dict[str, str] = {}
        for image in response["images"]:
            image_digest = image["imageId"]["imageDigest"]  # type: ignore
            image_manifest = image["imageManifest"]  # type: ignore
            if image_digest in digest_to_manifest_map:
                if image_manifest != digest_to_manifest_map[image_digest]:
                    raise ValueError(
                        f"Not all image manifests are equivalent for {image_digest} in {self.uri}"
                    )
            else:
                digest_to_manifest_map[image_digest] = image_manifest

        return [
            ECRImage(
                account_id=self.account_id,
                region=self.region,
                repository_name=self.repository_name,
                image_digest=image_digest,
                image_manifest=image_manifest,
            )
            for image_digest, image_manifest in digest_to_manifest_map.items()
        ]

    @classmethod
    def from_uri(cls, uri: str) -> "ECRRepository":
        repo_uri = ECRRepositoryUri(uri)
        return ECRRepository(
            account_id=repo_uri.account_id,
            region=repo_uri.region,
            repository_name=repo_uri.repository_name,
        )

    @classmethod
    def from_arn(cls, arn: str) -> "ECRRepository":
        match = ECR_REPO_ARN_PATTERN.match(arn)
        if not match:
            raise InvalidAmazonResourceNameError(
                f"resource ARN = '{arn}' does not match {ECR_REPO_ARN_PATTERN} pattern"
            )
        return ECRRepository(
            account_id=match.group(2),
            region=match.group(1),
            repository_name=match.group(3),
        )

    @classmethod
    def from_name(
        cls,
        repository_name: str,
        account_id: str | None = None,
        region: str | None = None,
    ) -> "ECRRepository":
        region = get_region(region)
        account_id = account_id or get_account_id()
        return ECRRepository(account_id=account_id, region=region, repository_name=repository_name)


class ECRRegistry(ECRResource):
    @property
    def uri(self) -> str:
        return ECRRegistryUri.from_components(account_id=self.account_id, region=self.region)

    def get_repositories(
        self,
        repository_name: str | re.Pattern | None = None,
        repository_tags: list[ResourceTag] | None = None,
    ) -> list[ECRRepository]:
        """Filter repositories based on resource tags specified.

        Args:
            repository_name: Repository name or pattern to filter by.
            repository_tags: List of resource tags to filter by.

        Returns:
            Filtered list of repositories with resource tags.
        """
        repositories = self.list_repositories()
        filtered_repos = []
        for repo in repositories:
            if repository_tags:
                repo_tags = repo.get_resource_tags()
                if not all([filter_tag in repo_tags for filter_tag in repository_tags]):
                    continue
            if repository_name:
                if isinstance(repository_name, re.Pattern):
                    if not repository_name.match(repo.repository_name):
                        continue
                elif repository_name not in repo.repository_name:
                    continue
            filtered_repos.append(repo)
        return filtered_repos

    def list_repositories(self) -> list[ECRRepository]:
        """List all repositories in the Registry.

        Returns:
            List of repositories in the registry.
        """

        paginator = self.client.get_paginator("describe_repositories")
        repositories: list[ECRRepository] = []
        for describe_repos_response in paginator.paginate(registryId=self.account_id):
            for repository in describe_repos_response["repositories"]:
                assert "repositoryName" in repository
                repositories.append(
                    ECRRepository(
                        account_id=self.account_id,
                        region=self.region,
                        repository_name=repository["repositoryName"],
                    )
                )
        return repositories

    def get_ecr_login(self) -> ECRLogin:
        auth = self.client.get_authorization_token(registryIds=[self.account_id])
        if len(auth["authorizationData"]) != 1:
            raise AWSError(f"Could not resolve authorization token. Reponse: {auth}")
        auth_data_entry = auth["authorizationData"][0]
        auth_token = base64.b64decode(auth_data_entry["authorizationToken"]).decode()  # type: ignore
        username, password = auth_token.split(":")
        registry = auth_data_entry["proxyEndpoint"].replace("https://", "")  # type: ignore
        self.logger.debug(f"Registry (proxy endpoint): {registry}")
        assert "expiresAt" in auth_data_entry
        expires_at = auth_data_entry["expiresAt"].isoformat()
        self.logger.debug(f"Token expires at: {expires_at}")

        return ECRLogin(username=username, password=password, registry=registry)

    @classmethod
    def from_uri(cls, uri: str) -> "ECRRegistry":
        registry_uri = ECRRegistryUri(uri)
        return ECRRegistry(account_id=registry_uri.account_id, region=registry_uri.region)

    @classmethod
    def from_env(cls, region: str | None = None) -> "ECRRegistry":
        return ECRRegistry(account_id=get_account_id(), region=get_region(region))


def resolve_image_uri(name: str, default_tag: str | None = None) -> str:
    """Resolve full image URI from input name.

    Args:
        name: Partial or fully qualified uri, name of image or repository.
        default_tag: Default tag to use if not specified. Defaults to None.

    Returns:
        Fully qualified image URI.
    """

    try:
        uri = name

        if not ECRRegistryUri.is_prefixed(uri):
            uri = f"{ECRRegistryUri.from_components()}/{uri}"

        if ECRImageUri.is_valid(uri):
            return uri
        elif ECRRepositoryUri.is_valid(uri):
            repo = ECRRepository.from_uri(uri)

            if default_tag:
                image = repo.get_image(image_tag=default_tag)
                return image.uri
            else:
                # Fetch latest tagged
                def get_image_push_time(image: ECRImage) -> datetime:
                    if image.image_pushed_at is None:
                        raise RuntimeError(f"Couldn't get 'image_pushed_at' for: {image}")
                    return image.image_pushed_at

                images = sorted(repo.get_images("TAGGED"), key=get_image_push_time)
                return images[-1].uri
        else:
            raise ValueError(f"Could not resolve full URI for image {uri} (raw={name})")
    except Exception as e:
        msg = f"Couldn't resolve ECR image URI from {name} with error: {e}"
        logger.exception(msg)
        raise ResourceNotFoundError(msg) from e
