# tests/test_pipeline_end_to_end.py

import pytest
import pandas as pd
from ga_core.hpc_data_pipeline import HPCDataProcessor
from tests.helpers import load_expected_csv

class TestHPCDataPipeline:
    """
    End-to-end (block-box) tests for the HPC data pipeline (extract + enrich) using various test cases.

    Each test feeds a small SLURM log file to the pipeline through the HPCDataProcessor class and 
    checks the output against manually calculated golden output. 
    For the sake of consistency, a static CI value is used for all tests which is set in the cluster_info_dict fixture. 
    """
    @pytest.fixture(autouse=True)
    def setup(self, config_data, cluster_info_dict, fixed_params):
        """
        Runs automatically before every test in this class.
 
        Derives the workload manager name (e.g. "slurm") from cluster_info_dict to
        build per-scenario file paths, and overrides postcode to 'None' so every test
        in this class uses the static CI fallback rather than call the API.
        """
        self.wm = cluster_info_dict["workload_manager"].lower()
        self.test_cluster_info = {
            **cluster_info_dict,
            "postcode": None, # Postcode is set to none to force static CI from cluster info.
        }
        self.test_config = config_data
        self.fixed_params = fixed_params
        
    def test_single_job_pipeline_completed(self):
        """
        Scenario: one CPU job in a normal COMPLETED state, on a CPU partition

        End-to-end test for a completed job log as input.
        """
        test_config = {**self.test_config, "useCustomLogs": f'tests/testdata/{self.wm}/raw_logs/single_job_completed.txt'}
        processor = HPCDataProcessor(
            config_data=test_config,
            cluster_info=self.test_cluster_info,
            fixed_params=self.fixed_params,
            all_users_access=True,
                )

        extracted_df = processor.extract_data()
        result = processor.enrich_data(extracted_df)
        expected = load_expected_csv(f"tests/testdata/{self.wm}/expected/single_job_completed_expected.csv")
        pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected[result.columns].reset_index(drop=True),
            check_exact=False,
            rtol=1e-4,
                )
    
    def test_single_job_pipeline_running(self):
        """
        Scenario: a job still in a RUNNING (unfinished) state, with no End timestamp.

        End-to-end test to filter unfinished jobs. 
        This expects a RuntimeError to be raised since no jobs are completed and the cleaning step cannot be performed.
        """
        test_config = {**self.test_config, "useCustomLogs": f'tests/testdata/{self.wm}/raw_logs/single_job_running.txt'}
        processor = HPCDataProcessor(
            config_data=test_config,
            cluster_info=self.test_cluster_info,
            fixed_params=self.fixed_params,
            all_users_access=True,
                )
        
        with pytest.raises(RuntimeError):
            processor.extract_data()
    
    def test_single_job_pipeline_failed(self):
        """
        Scenario: one CPU job that ran to completion but FAILED, on a CPU partition

        End-to-end test for a failed job log as input. 
        Expected to produce non-zero values in the *_failedJobs columns (energy_failedJobs, carbonFootprint_failedJobs)
        """
        test_config = {**self.test_config, "useCustomLogs": f'tests/testdata/{self.wm}/raw_logs/single_job_failed.txt'}
        processor = HPCDataProcessor(
            config_data=test_config,
            cluster_info=self.test_cluster_info,
            fixed_params=self.fixed_params,
            all_users_access=True,
                )
        
        extracted_df = processor.extract_data()
        result = processor.enrich_data(extracted_df)
        expected = load_expected_csv(f"tests/testdata/{self.wm}/expected/single_job_failed_expected.csv")
        pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected[result.columns].reset_index(drop=True),
            check_exact=False,
            rtol=1e-4,
                )

