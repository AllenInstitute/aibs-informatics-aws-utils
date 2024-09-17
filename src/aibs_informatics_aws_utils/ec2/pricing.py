import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, cast

import boto3
import requests
from aibs_informatics_core.utils.decorators import cache

from aibs_informatics_aws_utils.core import get_region
from aibs_informatics_aws_utils.ec2 import (
    describe_instance_types_by_props,
    get_instance_type_on_demand_price,
    get_instance_type_spot_price,
)
from aibs_informatics_aws_utils.ec2.functions import RawRange

try:
    import pandas as pd

    pd.set_option("display.max_rows", 100)
    pd.set_option("display.max_columns", 50)

    @cache
    def get_instance_type_spot_interruptions(
        os: Literal["Linux", "Windows"] = "Linux", region: Optional[str] = None
    ) -> Dict[str, Tuple[float, float]]:
        """Gets the spot interruption rate for a list of instance types

        https://stackoverflow.com/a/61526188/4544508

        Returns:
            Dict[str, Tuple[float, float]]: A dictionary of instance types and their corresponding
                spot interruption rates. The interruption rate is a tuple of (lower, upper) bounds
        """
        url_interruptions = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"
        response = requests.get(url=url_interruptions)
        spot_advisor = json.loads(response.text)["spot_advisor"]

        region = region or get_region()

        interruption_rate_by_instance_type: Dict[str, Tuple[float, float]] = {}
        for it in spot_advisor[region][os]:
            try:
                rate = spot_advisor[region][os][it]["r"]
                if rate == 0:
                    interruption_rate_by_instance_type[it] = (0.0, 0.05)
                elif rate == 1:
                    interruption_rate_by_instance_type[it] = (0.05, 0.10)
                elif rate == 2:
                    interruption_rate_by_instance_type[it] = (0.10, 0.15)
                elif rate == 3:
                    interruption_rate_by_instance_type[it] = (0.15, 0.20)
                else:
                    # NOTE: Upper limit is not specified by data, rather just a high number to indicate
                    #       that the instance type is not recommended for spot
                    interruption_rate_by_instance_type[it] = (0.2, 0.65)
            except KeyError:
                print(f"warning: {it} not found in spot advisor data")

        return interruption_rate_by_instance_type

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
            raise ValueError(
                f"Invalid instance type: {instance_type}. Cannot match regex {pattern}"
            )

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

    @dataclass
    class InstanceTypeFilters:
        architectures: Optional[List[Literal["arm64", "i386", "x86_64"]]] = None
        vcpu_limits: Optional[RawRange] = None
        memory_limits: Optional[RawRange] = None
        gpu_limits: Optional[RawRange] = None
        on_demand_support: Optional[bool] = None
        spot_support: Optional[bool] = None
        regions: Optional[List[str]] = None
        availability_zones: Optional[List[str]] = None

    @dataclass
    class InstanceTypePricing:
        region: str = field(default_factory=get_region)
        instance_type_filters: InstanceTypeFilters = InstanceTypeFilters()

        @property
        def instance_type_spot_interruptions(self) -> Dict[str, Tuple[float, float]]:
            try:
                return self._instance_type_spot_interruptions
            except AttributeError:
                self._instance_type_spot_interruptions = get_instance_type_spot_interruptions(
                    os="Linux", region=self.region
                )
            return self.instance_type_spot_interruptions

        @property
        def instance_type_info_df(self) -> pd.DataFrame:
            """Returns the instance type information as a DataFrame"""
            try:
                return self._it_info_df
            except AttributeError:
                self.instance_type_info_df = self.build_instance_type_info_dataframe()
            return self.instance_type_info_df

        @instance_type_info_df.setter
        def instance_type_info_df(self, it_info_list: pd.DataFrame):
            self._it_info_df = it_info_list

        def build_instance_type_info_list(
            self, instance_type_filters: Optional[InstanceTypeFilters] = None
        ) -> List[Dict[str, Any]]:
            instance_type_filters = instance_type_filters or self.instance_type_filters
            it_info_list = describe_instance_types_by_props(
                architectures=instance_type_filters.architectures,
                vcpu_limits=instance_type_filters.vcpu_limits,
                memory_limits=instance_type_filters.memory_limits,
                gpu_limits=instance_type_filters.gpu_limits,
                on_demand_support=instance_type_filters.on_demand_support,
                spot_support=instance_type_filters.spot_support,
                regions=instance_type_filters.regions,
                availability_zones=instance_type_filters.availability_zones,
            )
            for it_info in it_info_list:
                if "spot" not in it_info.get(
                    "SupportedUsageClasses", []
                ) or "on-demand" not in it_info.get("SupportedUsageClasses", []):
                    continue
                it_name = it_info["InstanceType"]  # type: ignore[attr-defined] ## typing states key as not required but it is
                it_info["Pricing"] = {
                    "OnDemand": get_instance_type_on_demand_price("us-west-2", it_name),
                    "Spot": get_instance_type_spot_price("us-west-2", it_name),
                }
                it_info["SpotInterruptionRate"] = self.instance_type_spot_interruptions.get(
                    it_name, (0.0, 0.0)
                )
            return cast(List[Dict[str, Any]], it_info_list)

    def build_instance_type_info_dataframe(
        self, instance_type_filters: Optional[InstanceTypeFilters] = None
    ) -> Tuple[pd.DataFrame, List[str]]:
        it_info_list = self.build_instance_info_list(instance_type_filters)
        it_info_df = pd.DataFrame(it_info_list, index=[it["InstanceType"] for it in it_info_list])

        # Flatten the dictionary columns
        for column in ["VCpuInfo", "Pricing", "MemoryInfo", "NetworkInfo"]:
            # Check if the column contains a dictionary
            if isinstance(it_info_df[column].iloc[0], dict):
                # Flatten the dictionary and create new columns
                df_out = pd.json_normalize(it_info_df[column])
                df_out.columns = [f"{column}.{col}" for col in df_out.columns]
                df_out.index = it_info_df.index
                it_info_df = it_info_df.drop(column, axis=1).join(df_out)

        # Split the instance type into family and size: e.g. c5.2xlarge -> c5, 2xlarge
        it_info_df["InstanceFamily"] = it_info_df["InstanceType"].apply(lambda x: x.split(".")[0])
        it_info_df["InstanceSize"] = it_info_df["InstanceType"].apply(lambda x: x.split(".")[1])

        # Create MemoryInfo.SizeInGiB and NetworkInfo.NetworkPerformanceGbps columns
        it_info_df["MemoryInfo.SizeInGiB"] = it_info_df["MemoryInfo.SizeInMiB"] / 1024
        it_info_df["NetworkInfo.NetworkPerformanceGbps"] = it_info_df[
            "NetworkInfo.NetworkPerformance"
        ].apply(network_performance_sort_key)

        # Create Pricing ratios and per unit pricing columns
        it_info_df["PricingSpotOnDemandRatio"] = (
            it_info_df["Pricing.Spot"] / it_info_df["Pricing.OnDemand"]
        )
        it_info_df["PricingOnDemandPerVcpu"] = (
            it_info_df["Pricing.OnDemand"] / it_info_df["VCpuInfo.DefaultVCpus"]
        )
        it_info_df["PricingOnDemandPerMemory"] = (
            it_info_df["Pricing.OnDemand"] / it_info_df["MemoryInfo.SizeInGiB"]
        )

        # Split the SpotInterruptionRate into lower and upper bounds
        it_info_df["SpotInterruptionRateLower"] = it_info_df["SpotInterruptionRate"].apply(
            lambda x: x[0]
        )
        it_info_df["SpotInterruptionRateUpper"] = it_info_df["SpotInterruptionRate"].apply(
            lambda x: x[1]
        )
        # These coefficients are just rough ways of making apples to apples comparisons
        # Basically, we want to make sure that the ratio of vcpus to memory is roughly the same
        # We do this by multiplying the vcpu price by 16 and the memory price by 64
        # making this the effective price per a 16 core / 64 GB machine
        COMPUTE_COEFF = 16.0
        MEM_COEFF = 64.0
        it_info_df["PricingOnDemandPerCompute"] = (
            it_info_df["PricingOnDemandPerVcpu"] * COMPUTE_COEFF
            + it_info_df["PricingOnDemandPerMemory"] * MEM_COEFF
        ) / 2
        it_info_df["PricingSpotPerCompute"] = (
            it_info_df["PricingOnDemandPerCompute"] * it_info_df["PricingSpotOnDemandRatio"]
        )

        priority_columns = [
            "InstanceType",
            "InstanceFamily",
            "InstanceSize",
            "VCpuInfo.DefaultVCpus",
            "MemoryInfo.SizeInGiB",
            "NetworkInfo.NetworkPerformanceGbps",
            "PricingOnDemandPerCompute",
            "PricingSpotPerCompute",
            "PricingOnDemandPerVcpu",
            "PricingOnDemandPerMemory",
            "PricingSpotOnDemandRatio",
            "SpotInterruptionRateLower",
            "SpotInterruptionRateUpper",
            "Pricing.OnDemand",
            "Pricing.Spot",
        ]
        return it_info_df, priority_columns

except ImportError:
    pass
