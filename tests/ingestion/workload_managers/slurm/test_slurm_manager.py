# tests/ingestion/workload_managers/slurm/test_slurm_manager.py

# ------------------------------------------------------------------
# This file contains pytest unit tests for slurm/manager.py.
# NOTE: Tests pertaining to similar functionality in SlurmUtils and SlurmManager should be grouped together in the same class
# ------------------------------------------------------------------

import pytest
import pandas as pd

from ga_core.data_models.cluster_info_model import ClusterInfo
from ga_core.ingestion.workload_managers.slurm.manager import SlurmUtils

class TestMemory:
    @pytest.fixture(autouse=True)
    def setup(self, cluster_info_dict):
        """
        Runs automatically before every test in this class.
        Creates a SlurmUtils instance using cluster_info_dict to be used in the tests.
        """
        cluster_info = ClusterInfo.from_dict(cluster_info_dict)
        self.slurm_utils = SlurmUtils(cluster_info)

    # @pytest.mark.parametrize lets you run the same test function multiple times with different inputs
    @pytest.mark.parametrize("memory,unit,expected", [
        (2, 'T', 2000.0),
        (1000, 'M', 1.0),
        (5, 'G', 5.0),
        (1_000_000, 'K', 1.0),
    ])
    def test_convert_to_gb(self, memory, unit, expected):
        """
        Scenario: Test the convert_to_GB function with various memory values and units against expected outputs.
        """
        assert self.slurm_utils.convert_to_GB(memory, unit) == expected

    def test_invalid_unit_raises(self):
        """
        Scenario: Test that an invalid unit raises an AssertionError.
        """
        with pytest.raises(AssertionError):
            self.slurm_utils.convert_to_GB(1, 'X')

    @pytest.mark.parametrize("mem_raw,n_nodes,n_cores,expected", [
        ("2Tn", 1, 8, 2000.0), #2 TB per node, 1 node
        ("2Tn", 4, 8, 8000.0), #2 TB per node, 4 nodes
        ("500Gc", 1, 16, 8000.0), #500 GB per core, 16 cores
        ("1T", 1, 1, 1000.0), #flat 1 TB, no per-node/core suffix
    ])
    def test_tb_pb_variants(self, mem_raw, n_nodes, n_cores, expected):
        """
        Scenario: Test the calc_ReqMem function with various memory request formats and expected outputs.
        """
        row = pd.Series({'ReqMem': mem_raw, 'NNodes': n_nodes, 'NCPUS': n_cores})
        assert self.slurm_utils.calc_ReqMem(row) == expected

    def test_unparseable_unit_raises(self):
        """
        Scenario: Test that an unparseable memory unit raises a ValueError.
        """
        row = pd.Series({'ReqMem': '5Xn', 'NNodes': 1, 'NCPUS': 1})
        with pytest.raises(ValueError):
            self.slurm_utils.calc_ReqMem(row)

    def test_clean_rss_terabytes(self):
        """
        Scenario: Test the clean_RSS function with a terabyte value.
        """
        row = pd.Series({'MaxRSS': '3T'})
        assert self.slurm_utils.clean_RSS(row) == 3000.0