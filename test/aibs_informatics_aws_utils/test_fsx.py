from math import ceil
from time import sleep

import pytest
from aibs_informatics_core.utils.units import BYTE_UNIT_STEP, BYTES_PER_GIBIBYTE
from moto import mock_aws

from aibs_informatics_aws_utils.fsx import (
    FSxFileSystemId,
    calculate_size_required,
    get_file_system,
    get_fsx_client,
    list_file_systems,
    split_name_and_id,
    split_name_and_ids,
)

BYTES_PER_TEBIBYTE = BYTES_PER_GIBIBYTE << BYTE_UNIT_STEP


def convert_tib_to_bytes(tib) -> int:
    return ceil(tib * BYTES_PER_TEBIBYTE)


@pytest.fixture(scope="function")
def fsx_client(aws_credentials_fixture):
    with mock_aws():
        client = get_fsx_client()
        yield client


def test__split_name_and_id():
    assert split_name_and_id("fs-12345678901234567") == (
        None,
        FSxFileSystemId("fs-12345678901234567"),
    )
    assert split_name_and_id("name") == ("name", None)


def test__split_name_and_ids():
    assert split_name_and_ids(["fs-12345678901234567", "name"]) == (
        ["name"],
        ["fs-12345678901234567"],
    )
    assert split_name_and_ids(["fs-11111111111111111", "name", "fs-22222222222222222"]) == (
        ["name"],
        ["fs-11111111111111111", "fs-22222222222222222"],
    )
    assert split_name_and_ids(
        ["fs-11111111111111111", "name", "fs-22222222222222222", "name"]
    ) == (["name", "name"], ["fs-11111111111111111", "fs-22222222222222222"])


@mock_aws
def test__list_file_systems__returns_none(fsx_client):
    assert list_file_systems() == []


@pytest.mark.parametrize(
    "bytes_required, expected_file_size",
    [
        pytest.param(1, ceil(1.2 * BYTES_PER_TEBIBYTE), id="1 -> 1.2 TB"),
        pytest.param(
            convert_tib_to_bytes(1.2) - 1, convert_tib_to_bytes(1.2), id="1.2 TiB -> 1.2 TiB"
        ),
        pytest.param(
            convert_tib_to_bytes(1.2) - 1, convert_tib_to_bytes(1.2), id="1.2 TiB - 1 -> 1.2 TiB"
        ),
        pytest.param(
            convert_tib_to_bytes(1.2), convert_tib_to_bytes(2.4), id="1.2 TiB -> 2.4 TiB"
        ),
        pytest.param(
            convert_tib_to_bytes(2.4), convert_tib_to_bytes(4.8), id="2.4 TiB -> 4.8 TiB"
        ),
    ],
)
def test__calculate_size_required(bytes_required, expected_file_size):
    assert calculate_size_required(bytes_required) == expected_file_size


def test__get_file_system__returns_by_tag(fsx_client):
    result1 = fsx_client.create_file_system(
        FileSystemType="LUSTRE",
        StorageCapacity=convert_tib_to_bytes(1.2),
        SubnetIds=["subnet-12345678901234567"],
        Tags=[
            {"Key": "env", "Value": "dev"},
            {"Key": "Name", "Value": "fs1"},
            {"Key": "constant", "Value": "42"},
        ],
    )
    # NOTE: moto fsx logic has a bug that leads to collisions in fs id creation:
    #       https://github.com/getmoto/moto/issues/8148
    #       https://github.com/zkarpinski/moto/pull/10
    #       To overcome this, we will add a delay between the two create_file_system calls.
    #       This can be removed once the PR is merged.
    sleep(1)
    result2 = fsx_client.create_file_system(
        FileSystemType="LUSTRE",
        StorageCapacity=convert_tib_to_bytes(1.2),
        SubnetIds=["subnet-12345678901234567"],
        Tags=[
            {"Key": "env", "Value": "test"},
            {"Key": "Name", "Value": "fs2"},
            {"Key": "constant", "Value": "42"},
        ],
    )

    assert (
        get_file_system("fs1", tags={"env": "dev"})["FileSystemId"]
        == result1["FileSystem"]["FileSystemId"]
    )
    assert (
        get_file_system("fs2", tags={"env": "test"})["FileSystemId"]
        == result2["FileSystem"]["FileSystemId"]
    )

    with pytest.raises(ValueError):
        get_file_system("fs1", tags={"env": "test"})

    with pytest.raises(ValueError):
        get_file_system(tags={"constant": "42"})
