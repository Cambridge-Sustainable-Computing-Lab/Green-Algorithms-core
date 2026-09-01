import re
from typing import Optional

# Patterns for parsing node names and ranges
# TODO: Check if these patterns apply to other workload managers (other than SLURM)
_TRAILING_NUMBER_PATTERN = re.compile(r'^(.*?)(\d+)$')
_NODE_RANGE_PATTERN = re.compile(r'^(.*?)\[\s*(\d+)\s*[,-]\s*(\d+)\s*\]$')

def check_hardware_profile_ref(ref: str, hardware_profiles, context: str) -> str:
    """
    Validates that a hardware_profile reference actually exists.

    'context' is used only to make the error message point at where the
    bad reference came from (e.g. a partition or node range).
    """
    if ref not in hardware_profiles:
        raise ValueError(
            f"[cluster_info] {context} references unknown hardware_profile '{ref}'. "
            f"Known hardware profiles: {sorted(hardware_profiles.keys())}"
        )
    return ref

def split_trailing_number(s: str) -> Optional[tuple[str, str]]:
    """
    Splits a string into its non-numeric prefix and trailing digit run.
        'cpu-p-160' -> ('cpu-p-', '160')
    Returns None if `s` doesn't end in digits.
    """
    match = _TRAILING_NUMBER_PATTERN.match(s)
    return match.groups() if match else None

def parse_node_range_string(raw: str) -> tuple[str, tuple[int, int]]:
    """
    Parses a config-supplied range string into its hostname prefix and
    numeric bounds. Accepts both a comma and a hyphen as the separator
    between bounds:
        'cpu-p-[100, 200]' -> ('cpu-p-', (100, 200))
        'cpu-p-[100-200]'  -> ('cpu-p-', (100, 200))
    """
    match = _NODE_RANGE_PATTERN.match(raw)
    if not match:
        raise ValueError(
            f"[cluster_info] Could not parse node range '{raw}'. "
            f"Expected a format like 'cpu-p-[100-200]' or 'cpu-p-[100, 200]'."
        )
    prefix, lower_str, upper_str = match.groups()
    lower, upper = int(lower_str), int(upper_str)
    if lower > upper:
        raise ValueError(
            f"[cluster_info] Node range '{raw}' is invalid: "
            f"lower bound {lower} is greater than upper bound {upper}"
        )
    return prefix, (lower, upper)