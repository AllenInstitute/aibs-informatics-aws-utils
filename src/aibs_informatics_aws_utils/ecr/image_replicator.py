import re
from typing import TYPE_CHECKING, List, Optional

import requests
from aibs_informatics_core.utils.logging import LoggingMixin
from botocore.exceptions import ClientError

from aibs_informatics_aws_utils.core import get_client_error_code
from aibs_informatics_aws_utils.ecr.core import ECRImage, ECRRepository

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ecr import ECRClient
    from mypy_boto3_ecr.type_defs import LayerTypeDef
else:
    ECRClient = object
    LayerTypeDef = dict


from dataclasses import dataclass, field

from aibs_informatics_core.models.base.model import SchemaModel


@dataclass
class ReplicateImageRequest(SchemaModel):
    source_image: ECRImage
    destination_repository: ECRRepository
    destination_image_tags: List[str] = field(default_factory=list)


@dataclass
class ReplicateImageResponse(SchemaModel):
    destination_image: ECRImage


class ECRImageReplicator(LoggingMixin):
    def process_request(self, request: ReplicateImageRequest) -> ReplicateImageResponse:
        self.log.info(f"processing request='{request}")
        dest_image = self.replicate(
            source_image=request.source_image,
            destination_repository=request.destination_repository,
            destination_image_tags=request.destination_image_tags,
        )
        self.log.info(f"Replicated image to repo with uri={dest_image.uri}")
        return ReplicateImageResponse(destination_image=dest_image)

    def replicate(
        self,
        source_image: ECRImage,
        destination_repository: ECRRepository,
        destination_image_tags: Optional[List[str]] = None,
    ) -> ECRImage:
        """Copies the source image into the destination repository.

        This allows the user to facilitate replication:
            * between AWS accounts,
            * to a new repository,
            * with new tags.

        Note:   This has exactly the same effect as
                `docker pull; docker tag; docker push`,
                but is done with the AWS ECR SDK so we can run
                this in a lambda when our stack updates

        Args:
            source_image (ECRImage): configuration of image to pull
            destination_repository (ECRRepository): repository to push to.
        Returns:
            (ECRImage) configuration of destination image pushed.
        """
        self.log.info(
            f"Starting Image Replication "
            f"[source={source_image.uri}, destination={destination_repository.uri}] "
            f"with tags={destination_image_tags}"
        )

        self.upload_layers(
            source_image=source_image, destination_repository=destination_repository
        )

        self.logger.info(f"Putting image to {destination_repository.uri}")

        self.put_image(
            source_image=source_image,
            destination_repository=destination_repository,
            destination_image_tags=destination_image_tags,
        )
        self.logger.info(f"Completed putting image to {destination_repository.uri}.")

        return ECRImage(
            account_id=destination_repository.account_id,
            region=destination_repository.region,
            repository_name=destination_repository.repository_name,
            image_digest=source_image.image_digest,
            client=destination_repository.client,
        )

    def upload_layers(self, source_image: ECRImage, destination_repository: ECRRepository):
        """Upload image layers of the source image to the destination repository

        Args:
            source_image (ECRImage): source image that has been copied
            destination_repository (ECRRepository): destination repo
        """
        self.logger.info(
            f"Uploading layers from {source_image.uri} to {destination_repository.uri}."
        )

        layers = source_image.get_image_layers()
        config_layer = source_image.get_image_config_layer()

        all_layers = layers + [config_layer]

        self.logger.info(
            f"Source image {source_image.uri} has {len(layers)} layers and "
            f"1 config layer totalling {len(all_layers)} layers"
        )

        self._upload_layers(
            source_repository=source_image.get_repository(),
            destination_repository=destination_repository,
            layers=all_layers,
            check_if_exists=True,
        )

    def put_image(
        self,
        source_image: ECRImage,
        destination_repository: ECRRepository,
        destination_image_tags: Optional[List[str]] = None,
    ) -> ECRImage:
        """Put image manifest and tags for an image

        This is the final step in copying an ECR image. It must be done
        once all the layers of the image have been uploaded.

        If the put_image request fails with a 'LayersNotFoundException',
        this will attempt to upload the missing layers and retry
        the put_image request.

        Args:
            source_image (ECRImage): source image that has been copied
            destination_repository (ECRRepository): destination repo
            destination_image_tags (List[str]): destination image tags. Optional.
                If not provided, source image's tags are added.

        Returns:
            ECRImage: destination ECR Image

        """
        tags = (
            source_image.image_tags if destination_image_tags is None else destination_image_tags
        )

        self.logger.info(f"putting image to {destination_repository.uri} with {tags} tags")
        dest_image = ECRImage(
            account_id=destination_repository.account_id,
            region=destination_repository.region,
            repository_name=destination_repository.repository_name,
            image_digest=source_image.image_digest,
            image_manifest=source_image.image_manifest,
            client=destination_repository.client,
        )
        dest_image.client = destination_repository.client
        try:
            dest_image.put_image(None)
            if tags:
                dest_image.add_image_tags(*tags)

        except ClientError as e:
            # IF we have a LayersNotFoundException, then we will retry to upload layers
            if get_client_error_code(e) != "LayersNotFoundException":
                self.log_stacktrace(
                    f"Error putting image to {destination_repository.uri} with {tags}",
                    e,
                )
                raise e
            self.logger.warning(
                f"Received 'LayerNotFoundException' while putting image. "
                f"Resolving missing layers of {source_image} specified in {e}"
            )
            missing_layers = self._get_missing_layers(
                client=source_image.client,
                repository_name=source_image.repository_name,
                put_image_error=e,
            )
            self.logger.info(f"Identified {len(missing_layers)} missing layers. Uploading layers")
            self._upload_layers(
                source_repository=source_image.get_repository(),
                destination_repository=destination_repository,
                layers=missing_layers,
                check_if_exists=False,
            )

            self.logger.info("uploaded all missing layers, retrying put_image request")
            self.put_image(source_image, destination_repository, destination_image_tags)
        return dest_image

    def _upload_layers(
        self,
        source_repository: ECRRepository,
        destination_repository: ECRRepository,
        layers: List[LayerTypeDef],
        check_if_exists: bool = True,
    ):
        """(internal) Upload image layers of the source image to the destination repository

        Args:
            source_repository (ECRRepository): source repository of layers
            destination_repository (ECRRepository): destination repo
            layers (List[LayerTypeDef]): layers from source repository
            check_if_exists (bool, optional): _description_. Defaults to True.
        """
        for i, layer in enumerate(layers):
            self.logger.info(f"Starting upload of layer {i + 1} / {len(layers)}")
            if check_if_exists:
                layer_exists = self._check_layer_exists(
                    client=destination_repository.client,
                    repository_name=destination_repository.repository_name,
                    layer_digest=layer.get("layerDigest", ""),
                )
                if layer_exists:
                    self.logger.info(
                        f"layer {layer} already exists in {destination_repository.uri}"
                    )
                    continue

            self._upload_layer(
                source_repository=source_repository,
                destination_repository=destination_repository,
                layer=layer,
            )

    def _upload_layer(
        self,
        source_repository: ECRRepository,
        destination_repository: ECRRepository,
        layer: LayerTypeDef,
    ):
        self.logger.info(
            f"uploading layer {layer} from {source_repository.uri} to {destination_repository.uri}"
        )
        layer_digest = layer["layerDigest"]  # type: ignore
        layer_size = layer["layerSize"]  # type: ignore

        self.logger.info(f"getting private download url for layer {layer_digest}")
        download_url = self._get_download_url_for_layer(
            client=source_repository.client,
            repository_name=source_repository.repository_name,
            layer_digest=layer_digest,
        )

        self.logger.info(f"initiating upload to destination {destination_repository.uri}")
        initiate_layer_upload_response = destination_repository.client.initiate_layer_upload(
            registryId=destination_repository.account_id,
            repositoryName=destination_repository.repository_name,
        )

        upload_id = initiate_layer_upload_response["uploadId"]
        part_size = initiate_layer_upload_response["partSize"]

        current_source_layer_size = layer_size
        transfer_bytes_remaining = current_source_layer_size
        self.logger.info(
            f"uploading all {current_source_layer_size:,} bytes of layer {layer_digest} "
            f"in chunks of {part_size:,} with upload id {upload_id}"
        )

        while transfer_bytes_remaining > 0:
            part_first_byte = current_source_layer_size - transfer_bytes_remaining
            part_last_byte = min(current_source_layer_size, part_first_byte + part_size) - 1

            self.logger.info(
                f"uploading bytes {part_first_byte:,} - {part_last_byte:,} of layer {layer_digest}"
            )
            last_byte_received = self._upload_layer_part(
                client=destination_repository.client,
                repository_name=destination_repository.repository_name,
                download_url=download_url,
                upload_id=upload_id,
                part_first_byte=part_first_byte,
                part_last_byte=part_last_byte,
            )
            transfer_bytes_remaining = current_source_layer_size - last_byte_received - 1
            self.logger.info(
                f"successfully uploaded bytes {part_first_byte:,} to {last_byte_received:,} "
                f"completed {last_byte_received / layer_size:.2%}"
            )

        self.logger.info(
            f"Successfully uploaded all layer parts for {layer_digest}. Completing layer upload"
        )
        self._complete_layer_upload(
            client=destination_repository.client,
            repository_name=destination_repository.repository_name,
            upload_id=upload_id,
            layer_digest=layer_digest,
        )

    # ------------------------------------------------------------------------
    # Wrapper methods for layer related API calls
    # ------------------------------------------------------------------------

    def _check_layer_exists(
        self, client: ECRClient, repository_name: str, layer_digest: str
    ) -> bool:
        """
        Checks if a layer exists in the provided ECR Repository
        """
        self.logger.info(f"checking if layer {layer_digest} exists in {repository_name}")
        response = client.batch_check_layer_availability(
            repositoryName=repository_name, layerDigests=[layer_digest]
        )
        layers = response["layers"]
        if len(layers) != 1:
            raise AttributeError(
                f"size of layers list from batch_check_layer_availability"
                f"response {response} must equal 1 "
                f"repository={repository_name}, "
                f"and layerDigest={layer_digest}"
            )

        return response["layers"][0].get("layerAvailability") == "AVAILABLE"

    def _get_download_url_for_layer(
        self, client: ECRClient, repository_name: str, layer_digest: str
    ) -> str:
        """Gets authenticated download_url for the provided layer"""

        get_download_url_for_layer_response = client.get_download_url_for_layer(
            repositoryName=repository_name, layerDigest=layer_digest
        )
        return get_download_url_for_layer_response["downloadUrl"]

    def _upload_layer_part(
        self,
        client: ECRClient,
        repository_name: str,
        download_url: str,
        upload_id: str,
        part_first_byte: int,
        part_last_byte: int,
    ) -> int:
        """Uploads the range of bytes from the provided download url to destination repository

        Args:
            client (ECRClient): ECR client
            repository_name (str): destination repository to which layer part uploaded
            download_url: s3 url for the layer from '_get_download_url_for_layer'
            upload_id: upload id from 'initiate_layer_upload'
            part_first_byte: first byte to upload
            part_last_byte: last byte to upload

        Returns:
            int: the last byte received from the 'upload_layer_part' request
                 (not necessarily the part_last_byte)
        """

        http_response = requests.request(
            "GET",
            download_url,
            headers={"Range": "bytes={}-{}".format(part_first_byte, part_last_byte)},
            stream=False,
        )

        layer_part_bytes = b""
        for b in http_response.iter_content(chunk_size=None):
            layer_part_bytes += b

        upload_layer_part_response = client.upload_layer_part(
            repositoryName=repository_name,
            uploadId=upload_id,
            partFirstByte=part_first_byte,
            partLastByte=part_last_byte,
            layerPartBlob=layer_part_bytes,
        )

        return upload_layer_part_response["lastByteReceived"]

    def _complete_layer_upload(
        self, client: ECRClient, repository_name: str, upload_id: str, layer_digest: str
    ):
        """
        Tells ECR that layer upload has completed

        If the layer already exists in the AWS account / repository
        will return with success

        Args:
            client (ECRClient): ECR client
            repository_name (str): destination repository to which layer part uploaded
            upload_id: upload id for the layer from 'initiate_layer_upload'
            layer_digest: sha of the layer

        """

        try:
            complete_layer_upload_response = client.complete_layer_upload(
                repositoryName=repository_name,
                uploadId=upload_id,
                layerDigests=[layer_digest],
            )
            assert complete_layer_upload_response["layerDigest"] == layer_digest

        except ClientError as e:
            if get_client_error_code(e) != "LayerAlreadyExistsException":
                self.logger.error(
                    f"Unexpected Error completing layer upload "
                    f"with upload id: {upload_id} "
                    f"for repository: {repository_name} "
                )
                raise e

            self.logger.info(
                f"Layer with digest {layer_digest} exits in repository: {repository_name}. "
                "returning with success."
            )

    def _get_missing_layers(
        self, client: ECRClient, repository_name: str, put_image_error: ClientError
    ) -> List[LayerTypeDef]:
        """Gets missing layers from a ClientError while putting image

        Args:
            client (ECRClient): ECR client
            repository_name (str): source repository containing layer of layer digest
            source_image (ECRImage): source image wih missing layers
            put_image_error (ClientError): error thrown after putting image

        Returns:
            List[LayerTypeDef]: list of missing layers
        """

        error_message = put_image_error.response.get("Error", {}).get("Message", "Unknown")

        match = re.search(r".*\[(sha.*)\].*", error_message)
        if not match:
            raise ValueError(f"unable to extract missing shas from error message: {error_message}")
        missing_layer_digests = [item.strip() for item in match.group(1).split(",")]
        return [
            self._get_layer_from_digest(
                client=client,
                repository_name=repository_name,
                layer_digest=missing_layer_digest,
            )
            for missing_layer_digest in missing_layer_digests
        ]

    def _get_layer_from_digest(
        self, client: ECRClient, repository_name: str, layer_digest: str
    ) -> LayerTypeDef:
        """Get the layer info of an image's layer

        Args:
            client (ECRClient): ECR client
            repository_name (str): source repository containing layer of layer digest
            layer_digest (str): layer sha256 identifier

        Returns:
            LayerTypeDef: image layer digest and size in bytes
        """
        response = client.batch_check_layer_availability(
            repositoryName=repository_name, layerDigests=[layer_digest]
        )
        layers = response["layers"]
        if len(layers) != 1:
            raise AttributeError(
                f"size of layers list from batch_check_layer_availability"
                f"response {response} must equal 1 "
                f"for repository {repository_name} and layerDigest {layer_digest}"
            )
        if layers[0].get("layerAvailability") == "UNAVAILABLE":
            raise AttributeError(
                f"Layer with digest {layer_digest} from repository {repository_name} "
                f"is unavailable! Unable to extract layer size."
            )
        return layers[0]
