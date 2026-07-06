# tests/test_pipeline_end_to_end.py

import pytest
import pandas as pd
from ga_core.hpc_data_pipeline import HPCDataProcessor
from tests.helpers import load_expected_csv

class TestHPCDataPipeline:
    """
    End-to-end tests for the HPC data pipeline (extract + enrich) using various test cases.
    """
    @pytest.fixture(autouse=True)
    def setup(self, config_data, cluster_info_dict, fixed_params):
        self.wm = cluster_info_dict["workload_manager"].lower()
        self.test_cluster_info = {
            **cluster_info_dict,
            "postcode": None, # Postcode is set to none to force static CI from cluster info.
        }
        self.test_config = config_data
        self.fixed_params = fixed_params
        
    def test_single_job_pipeline_completed(self):
        """
            End-to-end test for a single completed job log as input.
        """
        test_config = {**self.test_config, "useCustomLogs": f'tests/testdata/{self.wm}/raw_logs/single_job_completed.csv'}
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
            End-to-end test for a running job log as input.
        """
        test_config = {**self.test_config, "useCustomLogs": f'tests/testdata/{self.wm}/raw_logs/single_job_running.csv'}
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
            End-to-end test for a failed job log as input.
        """
        test_config = {**self.test_config, "useCustomLogs": f'tests/testdata/{self.wm}/raw_logs/single_job_failed.csv'}
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

