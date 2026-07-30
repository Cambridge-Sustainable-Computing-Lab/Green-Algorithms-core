import os
import pytest
import pandas as pd

from ga_core.ingestion.workload_managers.slurm.manager import SlurmManager

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
        