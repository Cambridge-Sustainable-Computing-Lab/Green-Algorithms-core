# ------------------------------------------------------------------
# Contains the data models used to represent cluster configurations passed to ga_core. 
# It also includes data validation methods and helper methods for instantiation.
# 
# Any cluster info config must be structured around these models. 
# NOTE: If the structure of the cluster info config file changes, this file must be updated.
# ------------------------------------------------------------------

from dataclasses import dataclass, field
from typing import Optional, Dict

from ga_core.utils import cluster_info_utils

"""
Useful definitions:

TDP (Thermal Design Power) [watts]
    (Manufacturer-specified) Maximum thermal power dissipation of a processor (CPU or GPU) under normal operating workloads

PUE (Power Usage Effectiveness)
    A measure of data center energy efficiency defined as PUE = Total facility energy / IT equipment energy

CI (Carbon Intensity) [gCO₂e/kWh]
The amount of greenhouse gas emissions associated with electricity consumption (i.e. from generation and distribution).

Energy Cost [<currency> per kWh]
    Financial cost of electricity per unit of energy

Memory Request Granularity [GigaBytes]
    It represents the smallest memory unit users can reserve
"""

@dataclass
class HardwareProfile:
    """
    Data model to represent hardware profile information and perform basic validations
    """
    name: str
    type: str
    model: str
    TDP: float

    # GPU specific
    model_CPU: Optional[str] = None
    TDP_CPU: Optional[float] = None

    # Validation
    def __post_init__(self):
        if self.TDP <= 0:
            raise ValueError(f"[cluster_info] TDP must be a positive value for hardware profile {self.name}")
        if self.type not in ["CPU", "GPU"]:
            raise ValueError(f"[cluster_info] Type must be either 'CPU' or 'GPU' for hardware profile {self.name}")
        if self.type == "GPU":
            if self.model_CPU is None:
                raise ValueError(f"[cluster_info] GPU hardware profile {self.name} requires model_CPU to be specified.")
            if self.TDP_CPU is None or self.TDP_CPU <= 0:
                raise ValueError(f"[cluster_info] GPU hardware profile {self.name} requires TDP_CPU to be a positive value.")

    @classmethod
    def from_dict(cls, data: Dict, name: Optional[str] = None) -> 'HardwareProfile':
        """
        Creates a HardwareProfile instance from a dictionary.
        """
        data = dict(data)
        if name is not None:
            data.setdefault("name", name)
        return cls(**data)

@dataclass
class NodeRange:
    """
    Data model to represent a range of nodes and the name of their associated hardware profile.
    """
    prefix: str
    range: tuple[int, int]
    hardware_profile: str

    def contains(self, node_name: str) -> bool:
        """
        Checks whether a node name (e.g. 'cpu-160') falls inside this range.
        """
        parsed = cluster_info_utils.split_trailing_number(node_name)
        if parsed is None:
            return False
        node_prefix, index_str = parsed
        if node_prefix != self.prefix:
            return False
        lower, upper = self.range
        return lower <= int(index_str) <= upper
    
    @classmethod
    def from_dict(cls, data: Dict, hardware_profiles: Dict[str, HardwareProfile]) -> 'NodeRange':
        """
        Creates a NodeRange instance from a dictionary and validates that the referenced hardware profile exists.
        """
        raw_range = data["range"]
        prefix, node_range = cluster_info_utils.parse_node_range_string(raw_range)
        hardware_profile = cluster_info_utils.check_hardware_profile_ref(data["hardware_profile"], hardware_profiles, context=f"Node range '{raw_range}'")
        return cls(
            prefix=prefix,
            range=node_range,
            hardware_profile=hardware_profile,
        )

@dataclass
class PartitionInfo:
    """
    Data model to represent partition information and perform basic validations
    """
    name: str
    homogenous: bool

    # if homogenous, must provide a hardware profile name or node list with a single entry
    hardware_profile: Optional[str] = None

    # required if not homogenous: each entry describes its own slice of hardware
    node_list: Optional[list[NodeRange]] = None

    def __post_init__(self):
        if self.homogenous:
            if not self.hardware_profile and not self.node_list:
                raise ValueError(
                    f"Partition '{self.name}' is marked homogenous but no hardware profile or node list was provided"
                )
            elif self.node_list and len(self.node_list) > 1:
                raise ValueError(
                    f"Partition '{self.name}' is marked homogenous but multiple node ranges were provided"
                )

        else:
            if not self.node_list:
                raise ValueError(
                    f"Partition '{self.name}' is marked heterogenous but no node_list was provided"
                )
            missing = [nr.range for nr in self.node_list if nr.hardware_profile is None]
            if missing:
                raise ValueError(
                    f"Partition '{self.name}' is heterogenous but these node ranges are missing a hardware_profile: {missing}"
                )

    @classmethod
    def from_dict(cls, name: str, data: Dict, hardware_profiles: Dict[str, HardwareProfile]) -> 'PartitionInfo':
        """
        Creates a PartitionInfo instance from a dictionary.
        """
        data = dict(data)

        hardware_profile_ref = data.pop("hardware_profile", None)
        hardware_profile = (
            cluster_info_utils.check_hardware_profile_ref(hardware_profile_ref, hardware_profiles, context=f"Partition '{name}'") 
            if hardware_profile_ref is not None
            else None
        )

        node_list_data = data.pop("node_list", None)
        node_list = (
            [NodeRange.from_dict(nr, hardware_profiles) for nr in node_list_data]
            if node_list_data is not None
            else None
        )

        return cls(
            name=name,
            hardware_profile=hardware_profile,
            node_list=node_list,
            **data,  # remaining fields
        )

            
@dataclass
class EnergyCost:
    """
    Data model to represent energy cost information and perform basic validations
    """
    cost: float
    currency: str

    # Validation
    def __post_init__(self):
        if self.cost <= 0:
            raise ValueError("[cluster_info] Energy cost must be positive")

        if not self.currency:
            raise ValueError("[cluster_info] Currency cannot be empty")

@dataclass
class ClusterInfo:
    """
    Data model to represent cluster information and perform basic validations
    """
    institution: str
    cluster_name: str
    granularity_memory_request: int
    partitions: Dict[str, PartitionInfo]
    hardware_profiles: Dict[str, HardwareProfile]
    PUE: float
    CI: float
    energy_cost: EnergyCost
    postcode: Optional[str] = None
    workload_manager: str = "slurm" # Defaulting to SLURM

    # Optional parameters if the html output is used.
    texts_intro: Dict[str, str] = field(default_factory=dict)
    default_unit_RSS: str = "K"

    # Validation
    def __post_init__(self):
        if not self.institution:
            raise ValueError("[cluster_info] Institution cannot be empty")
        if not self.cluster_name:
            raise ValueError("[cluster_info] Cluster name cannot be empty")
        if self.granularity_memory_request <= 0:
            raise ValueError("[cluster_info] Granularity of memory request must be a positive integer")
        if self.PUE < 1:
            raise ValueError("[cluster_info] PUE must be greater than or equal to 1")
        if self.CI < 0:
            raise ValueError("[cluster_info] Carbon Intensity (CI) cannot be negative")
        if not self.partitions:
            raise ValueError("[cluster_info] At least one partition must be defined")
        if not self.hardware_profiles:
            raise ValueError("[cluster_info] At least one hardware profile must be defined")
        
    @classmethod
    def from_dict(cls, data: Dict) -> 'ClusterInfo':
        """
        Creates a ClusterInfo instance from a dictionary. Also handles the creation of nested PartitionInfo and EnergyCost instances.
        """
        hardware_profiles = {
            name: HardwareProfile.from_dict(info, name=name)
            for name, info in data.get("hardware_profiles", {}).items()
        }

        partitions = {
            name: PartitionInfo.from_dict(name, info, hardware_profiles)
            for name, info in data.get("partitions", {}).items()
        }
        energy_cost = EnergyCost(**data["energy_cost"])
        
        return cls(
            institution=data["institution"],
            cluster_name=data["cluster_name"],
            workload_manager=data.get("workload_manager", "slurm").lower(),
            granularity_memory_request=data["granularity_memory_request"],
            partitions=partitions,
            hardware_profiles=hardware_profiles,
            PUE=data["PUE"],
            CI=data["CI"],
            energy_cost=energy_cost,

            # Optional parameters
            postcode=data.get("postcode"),
            texts_intro=data.get("texts_intro", {}),
            default_unit_RSS=data.get("default_unit_RSS", "K")
        )