import moto
from aibs_informatics_test_resources import does_not_raise
from pytest import mark, param, raises

from aibs_informatics_aws_utils.ec2 import (
    describe_instance_type_offerings,
    describe_instance_types,
    describe_instance_types_by_props,
    get_availability_zones,
    get_common_instance_types,
    get_instance_type_spot_price,
    get_instance_types_by_az,
    get_regions,
    instance_type_sort_key,
    network_performance_sort_key,
    normalize_range,
)


# Need to use multiple E501 because ruff ignore blocks aren't a thing yet: https://github.com/astral-sh/ruff/issues/3711
@mark.parametrize(
    "expected, raw_range, min_limit, max_limit, raise_on_invalid, treat_single_value_as_max, raise_expectation",  # noqa: E501
    [
        param((None, None), None, None, None, False, False, does_not_raise(), id="None -> (None, None)"),  # noqa: E501
        param((None, None),None, 0, 10, False, False, does_not_raise(), id="None -> (None, None) with min/max limits"),  # noqa: E501
        param((None, None), (None, None), None, None, False, False, does_not_raise(), id="(None, None) -> (None, None)"),  # noqa: E501
        param((None, None), (None, None), 0, 10, False, False, does_not_raise(), id="(None, None) -> (None, None) with min/max limits"),  # noqa: E501
        param((None, 0), 0, None, None, False, True, does_not_raise(), id="0 -> (None, 0) treated as max"),  # noqa: E501
        param((0, None), 0, None, None, False, False, does_not_raise(), id="0 -> (0, None) treated as min"),  # noqa: E501
        param((5, None), (5, None), None, None, False, False, does_not_raise(), id="(5, None) -> (5, None)"),  # noqa: E501
        param((None, 5), (None, 5), 0, 10, False, False, does_not_raise(), id="(None, 5) -> (None, 5)"),  # noqa: E501
        param((0, 0), (-1, 1), 0, 0, False, False, does_not_raise(), id="(-1, 1) -> (0, 0) for min/max limits"),  # noqa: E501
        param((-1, 1), (1, -1), None, None, False, False, does_not_raise(), id="(1, -1) -> (-1, 1) corrects order"),  # noqa: E501
        # Invalid cases
        param(None, "string", 0, 0, False, False, raises(TypeError), id="string INVALID not a valid type"),  # noqa: E501
        param(None, (1, 2, 3), 0, 0, False, False, raises(ValueError), id="(1, 2, 3) INVALID not a tuple of length 2"),  # noqa: E501
        param( None, (-1, None), 0, 0, True, False, raises(ValueError), id="(-1, None) INVALID breaks max limit"),  # noqa: E501
        param(None, (None, 1), 0, 0, True, False, raises(ValueError), id="(None, 1) INVALID breaks min limit"),  # noqa: E501
    ],
)  # fmt: skip
def test__normalize_range(
    expected,
    raw_range,
    min_limit,
    max_limit,
    raise_on_invalid,
    treat_single_value_as_max,
    raise_expectation,
):
    with raise_expectation:
        actual = normalize_range(
            raw_range=raw_range,
            max_limit=max_limit,
            min_limit=min_limit,
            raise_on_invalid=raise_on_invalid,
            treat_single_value_as_max=treat_single_value_as_max,
        )
    if expected:
        assert actual == expected


@mark.parametrize(
    "instance_type, expected, raise_expectation",
    [
        param("t3.nano", ("t3", 0, 0), does_not_raise()),
        param("t3.micro", ("t3", 1, 0), does_not_raise()),
        param("t3.small", ("t3", 2, 0), does_not_raise()),
        param("t3.medium", ("t3", 3, 0), does_not_raise()),
        param("t3.large", ("t3", 4, 0), does_not_raise()),
        param("t3.metal", ("t3", 5, 0), does_not_raise()),
        param("m7i-flex.xlarge", ("m7i-flex", 4, 1), does_not_raise()),
        param("m7i-flex.32xlarge", ("m7i-flex", 4, 32), does_not_raise()),
        param("m7i-flex.metal", ("m7i-flex", 5, 0), does_not_raise()),
        # Invalid cases
        param("m7_metal", None, raises(ValueError), id="incorrect delimiter"),
        param("m7.wood", None, raises(ValueError), id="incorrect size"),
        param("m.7.large", None, raises(ValueError), id="too many dots"),
        param("m7.xxlarge", None, raises(ValueError), id="incorrect factor"),
        param("m7.x123large", None, raises(ValueError), id="another incorrect factor"),
    ],
)
def test__instance_type_sort_key__works(
    instance_type,
    expected,
    raise_expectation,
):
    with raise_expectation:
        actual = instance_type_sort_key(instance_type=instance_type)
    if expected:
        assert actual == expected


@mark.parametrize(
    "network_performance, expected, raise_expectation",
    [
        param("Low", 0.05, does_not_raise()),
        param("Moderate", 0.3, does_not_raise()),
        param("High", 1.0, does_not_raise()),
        param("Up to 10 Gigabit", 10.0, does_not_raise()),
        param("10 Gigabit", 10.0, does_not_raise()),
        param("37.5 Gigabit", 37.5, does_not_raise()),
    ],
)
def test__network_performance_sort_key__works(network_performance, expected, raise_expectation):
    with raise_expectation:
        actual = network_performance_sort_key(network_performance=network_performance)
    if expected:
        assert actual == expected


@moto.mock_aws
def test__get_availability_zones__no_args_gets_default_region(aws_credentials_fixture):
    azs = get_availability_zones()
    assert len(set([az[: az.rfind("-")] for az in azs])) == 1


@moto.mock_aws
def test__get_availability_zones__all_regions(aws_credentials_fixture):
    azs = get_availability_zones(all_regions=True)
    assert len(set([az[: az.rfind("-")] for az in azs])) > 1


@moto.mock_aws
def test__get_availability_zones__filtered_by_region(aws_credentials_fixture):
    azs = get_availability_zones(regions=["us-east-1", "us-west-2"])
    assert len(set([az[: az.rfind("-")] for az in azs])) == 2


@moto.mock_aws
def test__get_regions(aws_credentials_fixture):
    regions = get_regions()
    assert "us-east-1" in regions
    assert "us-west-2" in regions


@moto.mock_aws
def test__describe_instance_types__no_args_gets_all_instance_types(aws_credentials_fixture):
    instance_types = describe_instance_types()
    assert len(instance_types) > 0


@moto.mock_aws
def test__describe_instance_types__restricts_instance_types(aws_credentials_fixture):
    instance_types = describe_instance_types(instance_types=["t2.micro"])
    assert len(instance_types) == 1


@moto.mock_aws
def test__describe_instance_types__restricts_instance_types_and_filter(aws_credentials_fixture):
    instance_types = describe_instance_types(
        instance_types=["t2.micro"], filters={"supported-usage-class": ["spot"]}
    )
    assert len(instance_types) == 1


@moto.mock_aws
def test__describe_instance_type_offerings__no_args(aws_credentials_fixture):
    it_offerings = describe_instance_type_offerings()
    assert len(it_offerings) > 0


@moto.mock_aws
def test__describe_instance_type_offerings__regions_specified(aws_credentials_fixture):
    it_offerings = describe_instance_type_offerings(regions=["us-east-1"])
    assert len(it_offerings) > 0
    assert all([_.get("Location") in ["us-east-1"] for _ in it_offerings])


@moto.mock_aws
def test__describe_instance_type_offerings__azs_specified(aws_credentials_fixture):
    it_offerings = describe_instance_type_offerings(
        availability_zones=["us-east-1a", "us-west-2a"]
    )
    assert len(it_offerings) > 0
    assert all([_.get("Location") in ["us-east-1a", "us-west-2a"] for _ in it_offerings])
    assert any([_.get("Location") == "us-east-1a" for _ in it_offerings])
    assert any([_.get("Location") == "us-west-2a" for _ in it_offerings])


@moto.mock_aws
def test__describe_instance_type_offerings__regions_override_azs(aws_credentials_fixture):
    it_offerings = describe_instance_type_offerings(
        regions=["us-east-1"], availability_zones=["us-west-2a"]
    )
    assert len(it_offerings) > 0
    assert all([_.get("Location") == "us-east-1" for _ in it_offerings])


@moto.mock_aws
def test__describe_instance_types_by_props__no_args(aws_credentials_fixture):
    instance_types = describe_instance_types_by_props()
    assert len(instance_types) > 0


@moto.mock_aws
def test__describe_instance_types_by_props__all_args(aws_credentials_fixture):
    instance_types = describe_instance_types_by_props(
        architectures=["x86_64"],
        vcpu_limits=(1, 4),
        memory_limits=(1, 1 << 100),
        gpu_limits=(0, 1),
        on_demand_support=True,
        spot_support=True,
        regions=["us-west-2"],
    )
    assert len(instance_types) > 0
    for it in instance_types:
        assert it.get("ProcessorInfo", {}).get("SupportedArchitectures", []) == ["x86_64"]


@moto.mock_aws
def test__describe_instance_types_by_props__all_args_reduce_to_none(aws_credentials_fixture):
    assert len(describe_instance_types_by_props(vcpu_limits=(5000, None))) == 0
    assert len(describe_instance_types_by_props(vcpu_limits=(None, 0))) == 0
    assert len(describe_instance_types_by_props(memory_limits=(20 << (10 * 3), None))) == 0
    assert len(describe_instance_types_by_props(memory_limits=(None, 0))) == 0
    assert len(describe_instance_types_by_props(gpu_limits=(1234567890, None))) == 0
    assert len(describe_instance_types_by_props(gpu_limits=(None, -1))) == 0


@moto.mock_aws
def test__get_instance_types_by_az__no_args(aws_credentials_fixture):
    its_by_az = get_instance_types_by_az()
    assert len(set([az[:-1] for az in its_by_az.keys()])) == 1


@moto.mock_aws
def test__get_instance_types_by_az__regions_specified(aws_credentials_fixture):
    its_by_az = get_instance_types_by_az(regions=["us-east-1", "us-west-2"])
    assert len(set([az[:-1] for az in its_by_az.keys()])) == 2
    assert any([az.startswith("us-east-1") for az in its_by_az.keys()])
    assert any([az.startswith("us-west-2") for az in its_by_az.keys()])


@moto.mock_aws
def test__get_instance_types_by_az__azs_specified(aws_credentials_fixture):
    its_by_az = get_instance_types_by_az(availability_zones=["us-east-1a", "us-west-2a"])
    assert len(its_by_az) == 2
    assert "us-east-1a" in its_by_az.keys()
    assert "us-west-2a" in its_by_az.keys()


@moto.mock_aws
def test__get_common_instance_types__no_args(aws_credentials_fixture):
    common_its = get_common_instance_types()
    assert len(common_its) > 0


@moto.mock_aws
def test__get_common_instance_types__regions_specified(aws_credentials_fixture):
    common_its = get_common_instance_types(regions=["us-east-1", "us-west-2"])
    assert len(common_its) > 0


@moto.mock_aws
def test__get_instance_types_spot_price__no_args(aws_credentials_fixture):
    spot_price = get_instance_type_spot_price(region="us-west-2", instance_type="t2.micro")
    assert spot_price > 0
