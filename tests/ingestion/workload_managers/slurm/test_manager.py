# tests/ingestion/workload_managers/slurm/test_slurm_manager.py

# ------------------------------------------------------------------
# This file contains pytest unit tests for slurm/manager.py.
# NOTE: Tests pertaining to similar functionality in SlurmUtils and SlurmManager should be grouped together in the same class
# ------------------------------------------------------------------

import os

import pytest
import pandas as pd

from ga_core.data_models.cluster_info_model import ClusterInfo
from ga_core.ingestion.workload_managers.slurm.manager import SlurmManager
from ga_core.ingestion.workload_managers.slurm.sacct_client import SacctClient
from ga_core.ingestion.workload_managers.slurm.utils import SlurmUtils, NodeListUtil

class TestSlurmManager:
    """
    testing SlurmManager
    """
    @pytest.fixture(autouse=True)
    def setup(self, config_data, cluster_info_dict):
        """
        Runs automatically before every test in this class.
        """
        
        self.test_cluster_info = {
            **cluster_info_dict,
            "postcode": None, # Postcode is set to none to force static CI from cluster info.
        }
        self.test_config = config_data

    def make_manager(self, config_data):
        """
        Builds a SlurmManager instance.
        """
        wm = object.__new__(SlurmManager)
        wm.config_data = config_data
        return wm

    def test_mixed_states_sub_jobs(self):
        """
        Scenario: A job has sub-jobs with mixed states, i.e. some of them are completed and some still running. 
        In this case, the job must not be processed.
        """
        test_config = {**self.test_config, "useCustomLogs": f'tests/testdata/slurm/raw_logs/single_job_mixed_states.txt'}
        
        with open(os.path.join(test_config['useCustomLogs']), 'rb') as f:
            logs_raw = f.read() # Read custom logs

        wm = SlurmManager(self.test_config, self.test_cluster_info, logs_raw)

        # Raises ValueError since no finished jobs found
        with pytest.raises(ValueError):
            wm.clean_logs()


    # Tests for SlurmManager.pull_logs(): responsible for pulling raw logs via `sacct`
    def test_pull_logs_calls_sacct_with_expected_args(self, monkeypatch, config_data):
        """
        Scenario: pull_logs() should call SacctClient.pull_logs_by_time with the
        startDay and endDay values from config_data, and store
        the result on the instance.
        """
        calls = {}
        fake_raw_logs = b"JobID|User|State\n123|puppy|COMPLETED\n"

        def mock_pull_logs_by_time(start_day, end_day, all_users_access):
            calls["args"] = (start_day, end_day, all_users_access)
            return fake_raw_logs

        monkeypatch.setattr(
            SacctClient, "pull_logs_by_time", staticmethod(mock_pull_logs_by_time)
        )

        slurm_manager = self.make_manager(config_data)
        slurm_manager.pull_logs()

        assert calls["args"] == ("2026-01-01", "2026-04-01", True) # checks if arguments match ones defined in config in conftest.py
        assert slurm_manager.logs_raw == fake_raw_logs


    def test_pull_logs_wraps_exceptions_in_runtime_error(self, monkeypatch, config_data, caplog):
        """
        Scenario: If SacctClient.pull_logs_by_time raises an error, pull_logs() should re-raise as a RuntimeError
        """
        def failing_pull_logs_by_time(*args, **kwargs):
            raise ConnectionError("sacct not reachable")

        monkeypatch.setattr(
            SacctClient, "pull_logs_by_time", staticmethod(failing_pull_logs_by_time)
        )

        slurm_manager = self.make_manager(config_data)

        with pytest.raises(RuntimeError) as exc_info:
            slurm_manager.pull_logs()

        assert "Failed to pull logs" in str(exc_info.value)
        assert "sacct not reachable" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, ConnectionError) # original exception should be chained

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
        (1_000_000, 'K', 1.0)
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
        ("0n", 1, 1, 0.0),
    ])
    def test_reqmem_variants(self, mem_raw, n_nodes, n_cores, expected):
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

class TestNodeListUtil:

    @pytest.fixture
    def util(self):
        return NodeListUtil()

    def test_parse_list_simple_and_bracketed(self, util):
        assert util.parse_list("cpu-p-160,gpu-p-5") == ["cpu-p-160", "gpu-p-5"]
        assert util.parse_list("cpu-p-[160-165]") == [
            "cpu-p-160", "cpu-p-161", "cpu-p-162",
            "cpu-p-163", "cpu-p-164", "cpu-p-165",
        ]

    def test_parse_list_disjoint_and_zero_padded(self, util):
        assert util.parse_list("cpu-p-[100,150,199]") == [
            "cpu-p-100", "cpu-p-150", "cpu-p-199",
        ]
        assert util.parse_list("cg[001-002]") == ["cg001", "cg002"]

    def test_parse_list_invalid_range_raises(self, util):
        with pytest.raises(AssertionError, match="Invalid range"):
            util.parse_list("cg[5-2]")