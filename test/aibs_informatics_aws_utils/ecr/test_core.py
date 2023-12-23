import datetime
import json
import re
from contextlib import nullcontext as does_not_raise
from test.aibs_informatics_aws_utils.ecr.base import ECRTestBase
from time import sleep
from typing import TYPE_CHECKING, Tuple
from unittest import mock

from moto import mock_ecr, mock_sts
from pytest import mark, param, raises
from requests.exceptions import HTTPError

from aibs_informatics_aws_utils.ecr.core import (
    ECRImage,
    ECRImageUri,
    ECRRegistry,
    ECRRegistryUri,
    ECRRepository,
    ECRRepositoryUri,
    LifecyclePolicy,
    LifecyclePolicyAction,
    LifecyclePolicyRule,
    LifecyclePolicySelection,
    ResourceTag,
    TagMode,
    resolve_image_uri,
)
from aibs_informatics_aws_utils.exceptions import ResourceNotFoundError


@mark.parametrize(
    "input, expected, raises_error",
    [
        param(
            LifecyclePolicyRule(
                1,
                "",
                LifecyclePolicySelection("untagged", [], "sinceImagePushed", 7, "days"),
                LifecyclePolicyAction("expire"),
            ),
            {
                "action": {"type": "expire"},
                "description": "",
                "rulePriority": 1,
                "selection": {
                    "countNumber": 7,
                    "countType": "sinceImagePushed",
                    "countUnit": "days",
                    "tagPrefixList": [],
                    "tagStatus": "untagged",
                },
            },
            does_not_raise(),
            id="valid Lifecycle policy with empty fields is serialized",
        ),
    ],
)
def test__LifecyclePolicyRule__to_dict(input, expected, raises_error):
    with raises_error:
        actual = input.to_dict()

    assert actual == expected


@mark.parametrize(
    "input, expected, raises_error",
    [
        param(
            {
                "action": {"type": "expire"},
                "description": "",
                "rulePriority": 1,
                "selection": {
                    "countNumber": 7,
                    "countType": "sinceImagePushed",
                    "countUnit": "days",
                    "tagPrefixList": [],
                    "tagStatus": "untagged",
                },
            },
            LifecyclePolicyRule(
                1,
                "",
                LifecyclePolicySelection("untagged", [], "sinceImagePushed", 7, "days"),
                LifecyclePolicyAction("expire"),
            ),
            does_not_raise(),
            id="valid Lifecycle policy with empty fields deserialized",
        ),
        param(
            {
                "action": {"type": "expire"},
                "description": "",
                "rulePriority": 1,
                "selection": {
                    "countNumber": 7,
                    "countType": "imageCountMoreThan",
                    "countUnit": "days",
                    "tagPrefixList": [],
                    "tagStatus": "untagged",
                },
            },
            None,
            raises(ValueError),
            id="invalid Lifecycle policy with conflicting selection countType/countUnit fields",
        ),
        param(
            {
                "action": {"type": "expire"},
                "description": "",
                "rulePriority": 1,
                "selection": {
                    "countNumber": 7,
                    "countType": "sinceImagePushed",
                    "countUnit": "days",
                    "tagPrefixList": [],
                    "tagStatus": "tagged",
                },
            },
            None,
            raises(ValueError),
            id="invalid Lifecycle policy with conflicting selection tagStatus/tagPrefixList fields",
        ),
    ],
)
def test__LifecyclePolicyRule__from_dict(input, expected, raises_error):
    with raises_error:
        actual = LifecyclePolicyRule.from_dict(input)
    if expected is not None:
        assert actual == expected


rule_1a = LifecyclePolicyRule(
    1,
    "",
    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 11, "days"),
    LifecyclePolicyAction("expire"),
)
rule_1b = LifecyclePolicyRule(
    1,
    "",
    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 12, "days"),
    LifecyclePolicyAction("expire"),
)
rule_2 = LifecyclePolicyRule(
    2,
    "",
    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 2, "days"),
    LifecyclePolicyAction("expire"),
)
rule_3 = LifecyclePolicyRule(
    3,
    "",
    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 3, "days"),
    LifecyclePolicyAction("expire"),
)


@mark.parametrize(
    "rules, expected, raises_error",
    [
        param(
            [
                LifecyclePolicyRule(
                    3,
                    "",
                    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 3, "days"),
                ),
                LifecyclePolicyRule(
                    2,
                    "",
                    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 2, "days"),
                ),
                LifecyclePolicyRule(
                    1,
                    "",
                    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 1, "days"),
                ),
            ],
            LifecyclePolicy(
                rules=[
                    LifecyclePolicyRule(
                        1,
                        "",
                        LifecyclePolicySelection("untagged", [], "sinceImagePushed", 1, "days"),
                    ),
                    LifecyclePolicyRule(
                        2,
                        "",
                        LifecyclePolicySelection("untagged", [], "sinceImagePushed", 2, "days"),
                    ),
                    LifecyclePolicyRule(
                        3,
                        "",
                        LifecyclePolicySelection("untagged", [], "sinceImagePushed", 3, "days"),
                    ),
                ]
            ),
            does_not_raise(),
            id="valid Lifecycle policy rule priorities not amended",
        ),
        param(
            [
                LifecyclePolicyRule(
                    3,
                    "",
                    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 3, "days"),
                ),
                LifecyclePolicyRule(
                    2,
                    "",
                    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 2, "days"),
                ),
                LifecyclePolicyRule(
                    3,
                    "",
                    LifecyclePolicySelection("untagged", [], "sinceImagePushed", 4, "days"),
                ),
            ],
            LifecyclePolicy(
                rules=[
                    LifecyclePolicyRule(
                        2,
                        "",
                        LifecyclePolicySelection("untagged", [], "sinceImagePushed", 2, "days"),
                    ),
                    LifecyclePolicyRule(
                        3,
                        "",
                        LifecyclePolicySelection("untagged", [], "sinceImagePushed", 3, "days"),
                    ),
                    LifecyclePolicyRule(
                        4,
                        "",
                        LifecyclePolicySelection("untagged", [], "sinceImagePushed", 4, "days"),
                    ),
                ]
            ),
            does_not_raise(),
            id="valid Lifecycle policy rule priorities amended",
        ),
    ],
)
def test__LifecyclePolicy__sorts_rules(rules, expected, raises_error):
    with raises_error:
        actual = LifecyclePolicy.from_rules(*rules)
    if expected is not None:
        assert actual == expected


@mark.parametrize(
    "uri,expected,raises_error",
    [
        param(
            "123456789012.dkr.ecr.us-west-2.amazonaws.com",
            ("123456789012", "us-west-2"),
            does_not_raise(),
            id="Repo URI validated and parsed",
        ),
        param(
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/repo_name",
            ("", ""),
            raises(ValueError),
            id="URI for Repo fails validation",
        ),
        param(
            "123412.dkr.ecr.us-west-2.amazonaws.com",
            ("", ""),
            raises(ValueError),
            id="URI with invalid account ID fails validation",
        ),
        param(
            "123456789012.dkr.ecr.us-west!-2.amazonaws.com",
            ("", ""),
            raises(ValueError),
            id="URI with invalid region fails validation",
        ),
    ],
)
def test_ECRRegistryUri_validation(uri: str, expected: Tuple[str, ...], raises_error):
    expected_account, expected_region = expected
    with raises_error:
        ecr_registry_uri = ECRRegistryUri(uri)
        assert ecr_registry_uri.account_id == expected_account
        assert ecr_registry_uri.region == expected_region


@mark.parametrize(
    "uri,expected,raises_error",
    [
        param(
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/repo_name",
            ("123456789012", "us-west-2", "repo_name"),
            does_not_raise(),
            id="Repo URI validated and parsed",
        ),
        param(
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/repo_name:latest",
            ("", "", ""),
            raises(ValueError),
            id="URI for Image fails validation",
        ),
        param(
            "12349012.dkr.ecr.us-west-2.amazonaws.com/repo_name",
            ("", "", ""),
            raises(ValueError),
            id="Repo URI with invalid Account ID fails validation",
        ),
        param(
            "123456789012.dkr.ecr.us-west!-2.amazonaws.com/repo_name",
            ("", "", ""),
            raises(ValueError),
            id="Repo URI with invalid region fails validation",
        ),
        param(
            "123412.dkr.ecr.us-west-2.amazonaws.com/repo!name",
            ("", "", ""),
            raises(ValueError),
            id="Repo URI with invalid repo name fails validation",
        ),
    ],
)
def test_ECRRepositoryUri_validation(uri: str, expected: Tuple[str, ...], raises_error):
    expected_account, expected_region, expected_repository_name = expected

    with raises_error:
        ecr_repo_uri = ECRRepositoryUri(uri)

        assert ecr_repo_uri.account_id == expected_account
        assert ecr_repo_uri.region == expected_region
        assert ecr_repo_uri.repository_name == expected_repository_name


@mark.parametrize(
    "uri,expected,raises_error",
    [
        param(
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/repo_name:latest",
            ("123456789012", "us-west-2", "repo_name", "latest", None),
            does_not_raise(),
            id="Image URI with tag validated and parsed",
        ),
        param(
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/repo_name@sha256:e512dc77426a01390b2e9bdf08748080c5ead456384850ac84532f97d069e99f",
            (
                "123456789012",
                "us-west-2",
                "repo_name",
                None,
                "sha256:e512dc77426a01390b2e9bdf08748080c5ead456384850ac84532f97d069e99f",
            ),
            does_not_raise(),
            id="Image URI with digest validated and parsed",
        ),
        param(
            "123412.dkr.ecr.us-west-2.amazonaws.com/repo_name:latest",
            ("", "", "", "", ""),
            raises(ValueError),
            id="Image URI with invalid account ID fails validation",
        ),
        param(
            "123456789012.dkr.ecr.us-west!-2.amazonaws.com/repo_name:latest",
            ("", "", "", "", ""),
            raises(ValueError),
            id="Image URI with invalid region fails validation",
        ),
        param(
            "123412.dkr.ecr.us-west-2.amazonaws.com/repo!name:latest",
            ("", "", "", "", ""),
            raises(ValueError),
            id="Image URI with invalid repo name fails validation",
        ),
        param(
            "123412.dkr.ecr.us-west-2.amazonaws.com/repo_name",
            ("", "", "", "", ""),
            raises(ValueError),
            id="Image URI with no image identifier fails validation",
        ),
        param(
            "123412.dkr.ecr.us-west-2.amazonaws.com/repo_name@sha256:asdfasdfa",
            ("", "", "", "", ""),
            raises(ValueError),
            id="Image URI with invalid image digest fails validation",
        ),
    ],
)
def test_ECRImageUri_validation(
    uri: str,
    expected: Tuple[str, ...],
    raises_error,
):
    (
        expected_account,
        expected_region,
        expected_repository_name,
        expected_image_tag,
        expected_image_digest,
    ) = expected

    with raises_error:
        ecr_image_uri = ECRImageUri(uri)

        assert ecr_image_uri.account_id == expected_account
        assert ecr_image_uri.region == expected_region
        assert ecr_image_uri.repository_name == expected_repository_name
        assert ecr_image_uri.image_tag == expected_image_tag
        assert ecr_image_uri.image_digest == expected_image_digest


@mock_sts
@mock_ecr
class ECRImageTests(ECRTestBase):
    def test__init__with_manifest__does_not_call_ecr_for_info(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        ecr_image = ECRImage(
            account_id=self.ACCOUNT_ID,
            region=self.US_WEST_2,
            repository_name="repository_name",
            image_digest=image.image_digest,
            image_manifest=image.image_manifest,
        )
        self.assertStringPattern(ECRImageUri.regex_pattern, ecr_image.uri)

    def test__init__without_manifest__resolves_image_details_from_ecr(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        ecr_image = ECRImage(
            account_id=self.ACCOUNT_ID,
            region=self.US_WEST_2,
            repository_name=image.repository_name,
            image_digest=image.image_digest,
        )
        self.assertEqual(json.loads(ecr_image.image_manifest), json.loads(image.image_manifest))

    def test__init__without_manifest__fails_to_resolve_image_details_from_ecr(self):
        repo = self.create_repository("repository_name")
        with self.assertRaises(ResourceNotFoundError):
            ECRImage(
                account_id=self.ACCOUNT_ID,
                region=self.US_WEST_2,
                repository_name=repo.repository_name,
                image_digest=self.construct_image_digest("1234"),
            )

    def test__from_uri__with_image_digest_succeeds(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")

        actual = ECRImage.from_uri(image.uri)
        self.assertEqual(actual, image)

    def test__from_uri__with_image_tag_succeeds(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")

        actual = ECRImage.from_uri(repo.uri + ":latest")
        self.assertEqual(actual, image)

    def test__from_repository_uri__succeeds(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")

        actual = ECRImage.from_repository_uri(repo.uri, image.image_digest)
        self.assertEqual(actual, image)

    def test__get_repository__constructs_repository_object(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        actual = image.get_repository()
        self.assertEqual(actual, repo)

    def test__get_image_tags__returns_tags(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        actual = image.image_tags
        self.assertListEqual(actual, ["latest"])

    def test__add_image_tags__adds_tags(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        self.assertListEqual(image.image_tags, ["latest"])
        image.add_image_tags("latest", "v2")

        self.assertListEqual(image.image_tags, ["latest", "v2"])

    def test__add_image_tags__fails(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        self.assertListEqual(image.image_tags, ["latest"])
        with self.assertRaises(Exception):
            repo.delete(True)
            image.add_image_tags("latest")

    def test__get_image_layers__gets_layers_from_manifest(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        image_layers = image.get_image_layers()
        manifest = json.loads(image.image_manifest)
        manifest_layers = manifest["layers"]
        self.assertEqual(len(image_layers), len(manifest_layers))

    @mock.patch("requests.get")
    def test__get_image_config__returns_dict(self, mock_get):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")

        # setup
        class MockResponse:
            raise_for_status = mock.Mock()
            json = mock.Mock(return_value={"key1": "value1"})

        mock_get.return_value = MockResponse

        image_config = image.get_image_config()

        self.assertEqual(image_config, {"key1": "value1"})

    @mock.patch("requests.get")
    def test__get_image_config__raises_HTTPError(self, mock_get):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")

        # setup
        class MockResponse:
            raise_for_status = mock.Mock(side_effect=HTTPError("Request Failed"))
            json = mock.Mock()

        mock_get.return_value = MockResponse

        with self.assertRaises(HTTPError):
            image.get_image_config()


@mock_sts
@mock_ecr
class ECRRegistryTests(ECRTestBase):
    def test__get_ecr_login__works(self):
        registry = ECRRegistry(self.ACCOUNT_ID, self.REGION)
        login = registry.get_ecr_login()
        self.assertEqual(login.registry, registry.uri)
        self.assertTrue(login.username)
        self.assertTrue(login.password)

    def test__from_uri__succeeds(self):
        registry = ECRRegistry(self.ACCOUNT_ID, self.REGION)
        new_registry = ECRRegistry.from_uri(registry.uri)
        self.assertEqual(registry, new_registry)

    def test__from_env__succeeds(self):
        registry = ECRRegistry(self.ACCOUNT_ID, self.REGION)
        new_registry = ECRRegistry.from_env(registry.region)
        self.assertEqual(registry, new_registry)

    def test__get_repositories__no_filters(self):
        tag1 = ResourceTag(Key="key1", Value="value")
        tag2a = ResourceTag(Key="key2", Value="a")
        tag2b = ResourceTag(Key="key2", Value="b")

        self.create_repository("repo1", tag1, tag2a)
        self.create_repository("repo2", tag2a)
        self.create_repository("repo3", tag2b)

        registry = ECRRegistry(self.ACCOUNT_ID, self.REGION)
        actual = registry.get_repositories()
        self.assertEqual(len(actual), 3)

        expected = [
            ECRRepository(
                account_id=self.ACCOUNT_ID,
                region=self.REGION,
                repository_name="repo1",
            ),
            ECRRepository(
                account_id=self.ACCOUNT_ID,
                region=self.REGION,
                repository_name="repo2",
            ),
            ECRRepository(
                account_id=self.ACCOUNT_ID,
                region=self.REGION,
                repository_name="repo3",
            ),
        ]
        self.assertListEqual(actual, expected)

    def test__get_repositories__filters_work(self):
        tag1 = ResourceTag(Key="key1", Value="value")
        tag2a = ResourceTag(Key="key2", Value="a")
        tag2b = ResourceTag(Key="key2", Value="b")

        self.create_repository("repoxyz-1", tag1, tag2a)
        self.create_repository("repoxyz-2", tag1, tag2b)
        self.create_repository("repoxyz-3", tag1, tag2a)
        self.create_repository("repoabc", tag1, tag2a)

        registry = ECRRegistry(self.ACCOUNT_ID, self.REGION)
        actual = registry.get_repositories(
            repository_name=re.compile(r"^repoxyz"), repository_tags=[tag1, tag2a]
        )
        self.assertEqual(len(actual), 2)

        expected = [
            ECRRepository(
                account_id=self.ACCOUNT_ID,
                region=self.REGION,
                repository_name="repoxyz-1",
            ),
            ECRRepository(
                account_id=self.ACCOUNT_ID,
                region=self.REGION,
                repository_name="repoxyz-3",
            ),
        ]
        self.assertListEqual(actual, expected)

        actual = registry.get_repositories(
            repository_name="repoxyz", repository_tags=[tag1, tag2a]
        )
        self.assertEqual(len(actual), 2)

        expected = [
            ECRRepository(
                account_id=self.ACCOUNT_ID,
                region=self.REGION,
                repository_name="repoxyz-1",
            ),
            ECRRepository(
                account_id=self.ACCOUNT_ID,
                region=self.REGION,
                repository_name="repoxyz-3",
            ),
        ]
        self.assertListEqual(actual, expected)


@mock_sts
@mock_ecr
class ECRRepositoryTests(ECRTestBase):
    def test__from_uri__succeeds(self):
        ecr_repo = ECRRepository(
            account_id=self.ACCOUNT_ID,
            region=self.REGION,
            repository_name="repository_name",
        )
        new_ecr_repo = ECRRepository.from_uri(ecr_repo.uri)
        self.assertEqual(ecr_repo, new_ecr_repo)

    def test__from_arn__succeeds(self):
        ecr_repo = ECRRepository(
            account_id=self.ACCOUNT_ID,
            region=self.REGION,
            repository_name="repository_name",
        )
        new_ecr_repo = ECRRepository.from_arn(ecr_repo.arn)
        self.assertEqual(ecr_repo, new_ecr_repo)

    def test__from_name__succeeds(self):
        ecr_repo = ECRRepository(
            account_id=self.ACCOUNT_ID,
            region=self.REGION,
            repository_name="repository_name",
        )
        new_ecr_repo = ECRRepository.from_name(
            ecr_repo.repository_name, ecr_repo.account_id, ecr_repo.region
        )
        self.assertEqual(ecr_repo, new_ecr_repo)

    def test__create__creates_new_repo(self):
        repo = ECRRepository(
            account_id=self.ACCOUNT_ID,
            region=self.REGION,
            repository_name="repository_name",
        )
        self.assertFalse(repo.exists())
        tag = ResourceTag(Key="key", Value="value")
        repo.create(tags=[tag])
        self.assertTrue(repo.exists())
        self.assertListEqual(repo.get_resource_tags(), [tag])

    def test__create__handles_existing_repo(self):
        repo = ECRRepository(
            account_id=self.ACCOUNT_ID,
            region=self.REGION,
            repository_name="repo",
        )
        self.assertFalse(repo.exists())

        tag1 = ResourceTag(Key="key1", Value="value1")
        tag2 = ResourceTag(Key="key2", Value="value2")

        repo.create(tags=[tag1])

        self.assertTrue(repo.exists())
        self.assertListEqual(repo.get_resource_tags(), [tag1])

        # second call
        repo.create(tags=[tag2], exists_ok=True)
        self.assertListEqual(repo.get_resource_tags(), [tag1, tag2])

        # third call fails explicitly
        with self.assertRaises(Exception):
            repo.create(exists_ok=False)

    def test__get_resource_tags__works(self):
        repo = self.create_repository("repository_name")
        self.assertListEqual(repo.get_resource_tags(), [])

        tag1 = ResourceTag(Key="key1", Value="value1")
        tag2 = ResourceTag(Key="key2", Value="value2")
        repo2 = self.create_repository("repository_name2", tag1, tag2)
        self.assertListEqual(repo2.get_resource_tags(), [tag1, tag2])

    def test__update_resource_tags__appends_tags(self):
        tag1 = ResourceTag(Key="key1", Value="value1")
        tag2 = ResourceTag(Key="key2", Value="value2")
        tag3 = ResourceTag(Key="key3", Value="value3")
        repo = self.create_repository("repository_name", tag1)

        self.assertListEqual(repo.get_resource_tags(), [tag1])

        repo.update_resource_tags(tag2, tag3, mode=TagMode.APPEND)

        self.assertListEqual(repo.get_resource_tags(), [tag1, tag2, tag3])

        tag1_new = ResourceTag(Key="key1", Value="value1_new")
        repo.update_resource_tags(tag1_new, mode=TagMode.APPEND)
        self.assertListEqual(repo.get_resource_tags(), [tag1_new, tag2, tag3])

    def test__update_resource_tags__overwrites_tags(self):
        tag1 = ResourceTag(Key="key1", Value="value1")
        tag2 = ResourceTag(Key="key2", Value="value2")
        tag3 = ResourceTag(Key="key3", Value="value3")
        repo = self.create_repository("repository_name", tag1)

        self.assertListEqual(repo.get_resource_tags(), [tag1])

        tag1_new = ResourceTag(Key="key1", Value="value1_new")
        repo.update_resource_tags(tag2, tag3, mode=TagMode.OVERWRITE)

        self.assertListEqual(repo.get_resource_tags(), [tag2, tag3])

        repo.update_resource_tags(tag1_new, mode=TagMode.OVERWRITE)
        self.assertListEqual(repo.get_resource_tags(), [tag1_new])

    def test__get_images__handles_no_images(self):
        repo = ECRRepository(
            account_id=self.ACCOUNT_ID,
            region=self.REGION,
            repository_name="repository_name",
        )
        repo.create()
        images = repo.get_images()
        self.assertListEqual(images, [])

    def test__get_images__returns_all_images(self):
        repo = self.create_repository("repository_name")
        image1 = self.put_image(repo.repository_name, image_tag="latest", seed=123)
        image2 = self.put_image(repo.repository_name, image_tag="v2", seed=234)
        images = repo.get_images()
        self.assertListEqual(images, [image2, image1])

    def test__get_image__returns_image_from_digest(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        actual = repo.get_image(image_digest=image.image_digest)
        self.assertEqual(actual, image)

    def test__get_image__returns_image_from_tag(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        actual = repo.get_image(image_tag="latest")
        self.assertEqual(actual, image)

    def test__get_image__fails_if_both_arguments_provided(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")
        with self.assertRaises(ValueError):
            repo.get_image(image_tag="latest", image_digest=image.image_digest)

    def test__get_image__fails_if_no_found(self):
        repo = self.create_repository("repository_name")
        with self.assertRaises(ResourceNotFoundError):
            repo.get_image(image_tag="latest")

    def test__resolve_image_uri__returns_latest_image_for_repo_uri(self):
        repo = self.create_repository("repository_name")
        image1 = self.put_image(repo.repository_name, image_tag="latest", seed=123)
        self.assertEqual(resolve_image_uri(repo.repository_name), image1.uri)
        self.assertEqual(resolve_image_uri(repo.uri), image1.uri)

        # wait for 1 second to ensure that the image pushed at time is different
        # Moto does not support time resolution less than 1 second
        sleep(
            (image1.image_pushed_at + datetime.timedelta(seconds=1)).timestamp()
            - datetime.datetime.now().timestamp()
        )
        image2 = self.put_image(repo.repository_name, image_tag="v2", seed=234)
        self.assertEqual(resolve_image_uri(repo.repository_name), image2.uri)
        self.assertEqual(resolve_image_uri(repo.uri), image2.uri)

        self.assertEqual(resolve_image_uri(repo.repository_name, default_tag="latest"), image1.uri)

    def test__resolve_image_uri__works_with_image_uri(self):
        repo = self.create_repository("repository_name")
        image = self.put_image(repo.repository_name, image_tag="latest")

        uri = "123456789012.dkr.ecr.us-west-2.amazonaws.com/repository_name:latest"
        actual = resolve_image_uri(uri)
        self.assertEqual(actual, uri)

    def test__resolve_image_uri__fails_for_invalid_uri(self):
        with self.assertRaises(ResourceNotFoundError):
            resolve_image_uri("invalid@#$uri")

        repo = self.create_repository("repository_name")
        with self.assertRaises(ResourceNotFoundError):
            resolve_image_uri(repo.uri)
