__all__ = [
    "describe_availability_zones",
    "describe_instance_type_offerings",
    "describe_instance_types",
    "describe_instance_types_by_props",
    "describe_regions",
    "get_availability_zones",
    "get_common_instance_types",
    "get_instance_types_by_az",
    "get_instance_types_spot_price",
    "get_instance_type_on_demand_price",
    "get_regions",
    "normalize_range",
]

import json
import logging
import re
from collections import defaultdict
from functools import reduce
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from aibs_informatics_aws_utils.core import AWSService, get_client
from aibs_informatics_aws_utils.exceptions import AWSError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ec2.literals import (
        InstanceTypeType,
    )
    from mypy_boto3_ec2.type_defs import (
        AvailabilityZoneTypeDef,
        DescribeAvailabilityZonesResultTypeDef,
        DescribeInstanceTypeOfferingsRequestPaginateTypeDef,
        DescribeInstanceTypeOfferingsRequestTypeDef,
        DescribeInstanceTypeOfferingsResultTypeDef,
        DescribeInstanceTypesRequestPaginateTypeDef,
        DescribeInstanceTypesRequestTypeDef,
        DescribeSpotPriceHistoryRequestPaginateTypeDef,
        InstanceTypeInfoTypeDef,
        InstanceTypeOfferingTypeDef,
        PaginatorConfigTypeDef,
        SpotPriceTypeDef,
    )
else:
    SpotPriceTypeDef = dict
    DescribeInstanceTypeOfferingsRequestPaginateTypeDef = dict
    InstanceTypeType = str
    AvailabilityZoneTypeDef = dict
    DescribeAvailabilityZonesResultTypeDef = dict
    DescribeInstanceTypeOfferingsResultTypeDef = dict
    DescribeInstanceTypeOfferingsRequestTypeDef = dict
    DescribeInstanceTypesRequestPaginateTypeDef = dict
    DescribeInstanceTypesRequestTypeDef = dict
    DescribeSpotPriceHistoryRequestPaginateTypeDef = dict
    InstanceTypeOfferingTypeDef = dict
    InstanceTypeInfoTypeDef = dict
    PaginatorConfigTypeDef = dict

get_ec2_client = AWSService.EC2.get_client
get_ec2_resource = AWSService.EC2.get_resource


logger = logging.getLogger(__name__)

N = TypeVar("N", int, float)
RawRange = Union[None, N, Tuple[Optional[N], Optional[N]]]


def normalize_range(
    raw_range: Union[None, N, Tuple[Optional[N], Optional[N]]],
    min_limit: Optional[N] = None,
    max_limit: Optional[N] = None,
    raise_on_invalid: bool = True,
    treat_single_value_as_max: bool = False,
    num_type: Type[N] = int,
) -> Tuple[Optional[N], Optional[N]]:
    """Normalize a range of numbers to a tuple of (lower_limit, upper_limit)

    Args:
        raw_range (N | [N | None, N | None] | None): The raw range to normalize.
            This can be a single number, a tuple of numbers, or None.
        max_limit (Optional[N]): Restricts max end of range, if specified. Defaults to None.
        min_limit (Optional[N]): Restricts min end of range, if specified. Defaults to None.
        raise_on_invalid (bool): Raise an error if the range is invalid.
            If not, range is modified to match the max requirements. Defaults to True.
        treat_single_value_as_max (bool): If True, a single number is treated
            as the max end of the range. Defaults to False.
        num_type (Type[N], optional): the data type to use. Defaults to int.

    Returns:
        Tuple[Optional[N], Optional[N]]: The normalized range
    """
    if raw_range is None:
        return (None, None)
    elif isinstance(raw_range, num_type):
        if treat_single_value_as_max:
            return (None, raw_range)
        else:
            return (raw_range, None)
    elif isinstance(raw_range, (tuple, list)):
        if len(raw_range) != 2:
            raise ValueError(f"Limit tuple must have length 2, got {len(raw_range)}")
        if raw_range[0] is not None and raw_range[1] is not None and raw_range[0] > raw_range[1]:
            raw_range = (raw_range[1], raw_range[0])

        lower_limit = raw_range[0]
        upper_limit = raw_range[1]
        if lower_limit is not None and min_limit is not None and lower_limit < min_limit:
            if raise_on_invalid:
                raise ValueError(f"Lower limit {lower_limit} is less than minimum {min_limit}")

            logger.warning(
                f"Lower limit {lower_limit} is less than minimum {min_limit}, using minimum"
            )
            lower_limit = min_limit

        if upper_limit is not None and max_limit is not None and upper_limit > max_limit:
            if raise_on_invalid:
                raise ValueError(f"Upper limit {upper_limit} is greater than maximum {max_limit}")

            logger.warning(
                f"Upper limit {upper_limit} is greater than maximum {max_limit}, using maximum"
            )
            upper_limit = max_limit
        return (lower_limit, upper_limit)
    else:
        raise TypeError(f"Limit must be a number or a tuple of numbers, got {type(raw_range)}")


def instance_type_sort_key(instance_type: str) -> Tuple[str, int, int]:
    """Converts Instance Type into sort key (family, size rank, factor)

    Size Rank:
        1. nano
        2. micro
        3. small
        4. medium
        5. large
        6. metal


    Examples:
        - c5.2xlarge -> ('c5', 4, 2)
        - m7i-flex.metal -> ('m7i-flex', 5, 0)

    Args:
        instance_type (str): The instance type to split

    Returns:
        Tuple[str, int, int]: The instance type components (family, size rank, factor)
    """
    # Split instance type into prefix and size
    pattern = re.compile(r"([\w-]+)\.((\d*)x)?(nano|micro|small|medium|large|metal)")
    match = pattern.match(instance_type)

    if match is None:
        raise ValueError(f"Invalid instance type: {instance_type}. Cannot match regex {pattern}")

    family, factorstr, factornum, size = match.groups()

    # Define a dictionary to map sizes to numbers for sorting
    size_dict = {"nano": 0, "micro": 1, "small": 2, "medium": 3, "large": 4, "metal": 5}
    # If size is a number followed by 'xlarge', extract the number
    size_rank = size_dict[size]
    factor = int(factornum) if factornum else (1 if factorstr and "x" in factorstr else 0)
    return (family, size_rank, factor)


def network_performance_sort_key(network_performance: str) -> float:
    """Converts network performance description into a numerical sort key

    Args:
        network_performance (str): The network performance description
            e.g. "Low", "Moderate", "High", "Up to 10 Gigabit", "25 Gigabit", etc.

    Returns:
        float: The upper limit network performance value in Gbps
    """
    # If it matches a pattern like "10 Gigabit", "25 Gigabit", etc.
    pattern = re.compile(r"(\d+(?:.\d*)?)\s*Gigabit")
    # These are approximate values for the network performance
    conversion_dict = {
        "Low": 0.05,
        "Moderate": 0.3,
        "High": 1.0,
    }
    if network_performance in conversion_dict:
        return conversion_dict[network_performance]
    elif match := pattern.search(network_performance):
        return float(match.group(1))
    else:
        raise ValueError(f"Invalid network performance: {network_performance}")


def describe_regions():
    """Describe regions

    Returns:
        List of region info
    """
    ec2 = get_ec2_client()
    return ec2.describe_regions(AllRegions=False)["Regions"]


def get_regions() -> List[str]:
    """Gets all available regions

    Returns:
        List[str]: List of regions
    """
    return [
        region_info["RegionName"]
        for region_info in describe_regions()
        if "RegionName" in region_info
    ]


def describe_availability_zones(
    regions: Optional[List[str]] = None, all_regions: bool = False
) -> List[AvailabilityZoneTypeDef]:
    """Describe availability zones

    Args:
        regions (Optional[List[str]], optional): Return AZs for regions if specified.
            Defaults to None.
        all_regions (bool, optional): Whether to return for all regions (if no regions specified).
            Defaults to False.

    Returns:
        List of availability zone info
    """
    normalized_regions: list[str] | list[None] = [None]
    if regions is None and all_regions:
        regions = get_regions()

    if regions:
        normalized_regions = regions

    az_infos = []

    for region in normalized_regions:
        try:
            az_infos.extend(
                get_ec2_client(region=region).describe_availability_zones()["AvailabilityZones"]
            )
        except AWSError as e:
            # Fails for regions that don't support EC2.
            # If region is None, then we're using the default region and that should always work.
            if region is None:
                raise e
    return az_infos


def get_availability_zones(
    regions: Optional[List[str]] = None, all_regions: bool = False
) -> List[str]:
    """Gets a list of availability zones

    Args:
        regions (Optional[List[str]], optional): Return AZs for regions if specified.
            Defaults to None.
        all_regions (bool, optional): Whether to return for all regions (if no regions specified).
            Defaults to False.

    Returns:
        List[str]: List of availability zones
    """
    return [
        zone_info["ZoneName"]
        for zone_info in describe_availability_zones(regions=regions, all_regions=all_regions)
        if "ZoneName" in zone_info and zone_info.get("State") == "available"
    ]


def describe_instance_type_offerings(
    instance_types: Optional[List[str]] = None,
    regions: Optional[List[str]] = None,
    availability_zones: Optional[List[str]] = None,
) -> List[InstanceTypeOfferingTypeDef]:
    """Describe instance type offerings

    Args:
        instance_types (Optional[List[str]], optional): deprecated. Is not supported.
            Defaults to None.
        regions (Optional[List[str]], optional): Optional subset of regions. Defaults to None.
        availability_zones (Optional[List[str]], optional): Optional subset of availability zones.
            Defaults to None.

    Returns:
        _type_: List of instance type offerings
    """
    kwargs: DescribeInstanceTypeOfferingsRequestPaginateTypeDef = {}
    if instance_types is not None:
        logger.warning("instance_types filter is not supported. Ignoring.")
    if regions is not None:
        kwargs["LocationType"] = "region"
        kwargs["Filters"] = [dict(Name="location", Values=regions)]
    elif availability_zones is not None:
        regions = list(set([az[:-1] for az in availability_zones]))
        kwargs["LocationType"] = "availability-zone"
        kwargs["Filters"] = [dict(Name="location", Values=availability_zones)]

    normalized_regions: list[str] | list[None] = [None]
    if regions:
        normalized_regions = regions

    return [
        _
        for region in normalized_regions
        for response in get_ec2_client(region=region)
        .get_paginator("describe_instance_type_offerings")
        .paginate(**kwargs)
        for _ in response["InstanceTypeOfferings"]
    ]


def describe_instance_types(
    instance_types: Optional[List[InstanceTypeType]] = None,
    filters: Optional[Union[Dict[str, List[str]], List[Tuple[str, List[str]]]]] = None,
) -> List[InstanceTypeInfoTypeDef]:
    """Describe instance types

    Args:
        instance_types (Optional[List[str]], optional): subset of instance types.
            Defaults to None.
        filters (Optional[Dict[str, List[str]]], optional): Filters to apply. Defaults to None.

    Returns:
        List[InstanceTypeInfoTypeDef]: List of instance type details
    """
    ec2 = get_ec2_client()

    kwargs: DescribeInstanceTypesRequestPaginateTypeDef = {}
    if instance_types is not None:
        kwargs["InstanceTypes"] = instance_types
    if filters is not None:
        kwargs["Filters"] = [
            dict(Name=k, Values=v)
            for k, v in (filters.items() if isinstance(filters, dict) else filters)
        ]

    return [
        _
        for response in ec2.get_paginator("describe_instance_types").paginate(**kwargs)
        for _ in response["InstanceTypes"]
    ]


def describe_instance_types_by_props(
    architectures: Optional[List[Literal["arm64", "i386", "x86_64"]]] = None,
    vcpu_limits: Optional[RawRange] = None,
    memory_limits: Optional[RawRange] = None,
    gpu_limits: Optional[RawRange] = None,
    on_demand_support: Optional[bool] = None,
    spot_support: Optional[bool] = None,
    regions: Optional[List[str]] = None,
    availability_zones: Optional[List[str]] = None,
) -> List[InstanceTypeInfoTypeDef]:
    """Describe instance types by properties of those instance types

    Args:
        architectures (Optional[List["arm64", "i386", "x86_64"]]], optional):
            Filter by architecture. Defaults to None.
        vcpu_limits (Optional[RawRange], optional): vCPU range constraints.
            Can be a single number, a tuple of numbers, or None. Defaults to None.
        memory_limits (Optional[RawRange], optional): memory range constraints.
            Can be a single number, a tuple of numbers, or None. Defaults to None.
        gpu_limits (Optional[RawRange], optional): GPU range constraints.
            Can be a single number, a tuple of numbers, or None. Defaults to None.
        on_demand_support (Optional[bool], optional): filter based on on-demand support.
        spot_support (Optional[bool], optional): Filter based on spot support. Defaults to None.
        regions (Optional[List[str]], optional): Filter based on availability in regions.
            Defaults to None.
        availability_zones (Optional[List[str]], optional): Filter based on
            availability in availability zones. Defaults to None.

    Returns:
        List[InstanceTypeInfoTypeDef]: List of instance type details matching
            the specified filters
    """

    filters: List[Tuple[str, List[str]]] = []

    if architectures is not None:
        filters.append(("processor-info.supported-architecture", architectures))  # type: ignore[arg-type]
    if on_demand_support is not None and on_demand_support:
        filters.append(("supported-usage-class", ["on-demand"]))
    if spot_support is not None and spot_support:
        filters.append(("supported-usage-class", ["spot"]))

    instance_type_details = describe_instance_types(filters=filters)

    filtered_instance_type_details = []
    vcpu_range = normalize_range(vcpu_limits, min_limit=1, treat_single_value_as_max=True)
    memory_range = normalize_range(memory_limits, min_limit=1, treat_single_value_as_max=True)
    gpu_range = normalize_range(gpu_limits, min_limit=0, treat_single_value_as_max=True)

    for instance in instance_type_details:
        vcpus = instance.get("VCpuInfo", {}).get("DefaultVCpus", None)
        memory = instance.get("MemoryInfo", {}).get("SizeInMiB", None)
        gpus = instance.get("GpuInfo", {}).get("Gpus", [{}])[0].get("Count", 0)  # type: ignore[call-overload]
        if vcpus is None or memory is None:
            continue
        if vcpu_range[0] is not None and vcpus < vcpu_range[0]:
            continue
        if vcpu_range[1] is not None and vcpus > vcpu_range[1]:
            continue
        if memory_range[0] is not None and memory < memory_range[0]:
            continue
        if memory_range[1] is not None and memory > memory_range[1]:
            continue
        if gpu_range[0] is not None and (gpus is not None and gpus < gpu_range[0]):
            continue
        if gpu_range[1] is not None and (gpus is not None and gpus > gpu_range[1]):
            continue

        filtered_instance_type_details.append(instance)

    if regions or availability_zones:
        supported_instance_types = set(
            get_common_instance_types(regions=regions, availability_zones=availability_zones)
        )
        filtered_instance_type_details = [
            _
            for _ in filtered_instance_type_details
            if _["InstanceType"] in supported_instance_types
        ]

    return filtered_instance_type_details


def get_instance_types_by_az(
    regions: Optional[List[str]] = None,
    availability_zones: Optional[List[str]] = None,
) -> Dict[str, List[str]]:
    """Get the instance types available in each availability zone

    Results are combined regions and availability zones.
    If neither is specified, the current region's AZs are used.

    Args:
        regions (Optional[List[str]], optional): Specify zones by region.
            If no region or az specified, current region is used.
        availability_zones (Optional[List[str]], optional): Specify by az.
            If not specified, all availability zones in the specified regions are used.

    Returns:
        Dict[str, List[str]]: Mapping of availability zone to list of available instance types
    """
    if regions:
        availability_zones = availability_zones or []
        availability_zones.extend(get_availability_zones(regions=regions))
    elif availability_zones is None:
        availability_zones = get_availability_zones()

    offerings = describe_instance_type_offerings(availability_zones=availability_zones)

    az_instance_types: Dict[str, Set[str]] = defaultdict(set)

    for offering in offerings:
        az_instance_types[offering["Location"]].add(offering["InstanceType"])

    return {az: sorted(instance_types) for az, instance_types in az_instance_types.items()}


def get_common_instance_types(
    regions: Optional[List[str]] = None, availability_zones: Optional[List[str]] = None
) -> List[str]:
    """Get the common instance types across a list of regions or availability zones

    Args:
        regions (Optional[List[str]], optional): Optionally filter based on regions.
            Defaults to None. (uses current region if azs not specified)
        availability_zones (Optional[List[str]], optional): Filter based on availability zone.
            Defaults to None. If not specified, all availability zones in the
                specified regions are used.

    Returns:
        List[str]: List of instance types common across the specified
            regions or availability zones
    """

    instance_types_by_az = get_instance_types_by_az(
        regions=regions, availability_zones=availability_zones
    )

    if not instance_types_by_az:
        return []
    common_instance_types = reduce(
        lambda x, y: x & y, map(lambda _: set(_), instance_types_by_az.values())
    )
    return sorted(common_instance_types)


def get_instance_types_spot_price(
    region: str,
    instance_types: Optional[List[InstanceTypeType]] = None,
    product_descriptions: Optional[Sequence[str]] = ("Linux/UNIX",),
) -> Dict[InstanceTypeType, float]:
    """Get the current spot price for a list of instance types

    Args:
        region (str): The region to get spot price for
        instance_types (Optional[List[str]], optional): optional list of instance types.
            If not specified, all instance types in that region with spot prices are returned.
        product_descriptions (Optional[Sequence[str]], optional): Defaults to ("Linux/UNIX",).

    Returns:
        Dict[str, float]: Mapping of instance type to spot price in USD
    """
    ec2 = get_ec2_client(region=region)

    kwargs: DescribeSpotPriceHistoryRequestPaginateTypeDef = {}
    if instance_types is not None:
        kwargs["InstanceTypes"] = instance_types
    if product_descriptions is not None:
        kwargs["ProductDescriptions"] = list(product_descriptions)

    spot_history = [
        _
        for response in ec2.get_paginator("describe_spot_price_history").paginate(**kwargs)
        for _ in response["SpotPriceHistory"]
    ]

    spot_history_by_instance_type: Dict[InstanceTypeType, SpotPriceTypeDef] = {}
    for spot_price in spot_history:
        if (
            "InstanceType" not in spot_price
            or "SpotPrice" not in spot_price
            or "Timestamp" not in spot_price
        ):
            continue
        it = spot_price["InstanceType"]
        if (
            it not in spot_history_by_instance_type
            or spot_price["Timestamp"] > spot_history_by_instance_type[it]["Timestamp"]
        ):
            spot_history_by_instance_type[it] = spot_price

    return {
        it: float(spot_price["SpotPrice"])
        for it, spot_price in spot_history_by_instance_type.items()
    }


def get_instance_type_spot_price(
    region: str,
    instance_type: InstanceTypeType,
    product_description: str = "Linux/UNIX",
) -> float:
    """Get the latest spot price for an instance type

    Args:
        region (str): The region to get the price for
        instance_type (str): The instance type to get the price for
        product_description (str, optional): Defaults to "Linux/UNIX".

    Returns:
        float: price in USD per hour for Linux spot instance
    """
    return get_instance_types_spot_price(
        region, instance_types=[instance_type], product_descriptions=[product_description]
    )[instance_type]


def get_instance_type_on_demand_price(
    region: str, instance_type: str
) -> float:  # pragma: no cover
    """Get the on demand Linux price for an instance type in a region

    Args:
        region (str): The region to get the price for
        instance_type (str): The instance type to get the price for

    Returns:
        float: price in USD per hour for Linux on demand instance
    """

    # Pricing API endpoint is currently available only in 'us-east-1'
    pricing = get_client("pricing", region="us-east-1")

    response = pricing.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "operation", "Value": "RunInstances"},
            {"Type": "TERM_MATCH", "Field": "termType", "Value": "OnDemand"},
        ],
        MaxResults=100,
    )

    for product in response["PriceList"]:
        product_obj = json.loads(product)
        product_terms = list(product_obj["terms"]["OnDemand"].values())[0]
        price_dimensions = list(product_terms["priceDimensions"].values())[0]
        if "per On Demand" in price_dimensions["description"]:
            return float(price_dimensions["pricePerUnit"]["USD"])
    else:
        raise ValueError(f"No on demand pricing found for {instance_type} in {region}")
