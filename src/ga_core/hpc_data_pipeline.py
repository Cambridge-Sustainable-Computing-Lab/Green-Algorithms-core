# ------------------------------------------------------------------
# Main entry point for the Green_Algorithms_core package.
# This module defines the HPCDataProcessor class, which orchestrates the data processing pipeline:
# 1. Extracts raw logs from the workload manager (e.g. SLURM)
# 2. Enriches the data by calculating energy usage, carbon footprint, and contextual metrics.
# 3. Stores the enriched data using a CIStorageBackend implementation (Optional)
# ------------------------------------------------------------------

import pandas as pd
import logging
from ga_core.computation.carbon import CarbonCalculator
from ga_core.computation.carbon_intensity.ci_store import CIStorageBackend
from ga_core.computation.context_metrics import ContextMetricsCalculator
from ga_core.computation.energy import EnergyCalculator
from ga_core.data_models.cluster_info_model import ClusterInfo
from ga_core.ingestion.workload_managers import BaseWorkloadManager
from ga_core.computation.carbon_intensity.carbon_intensity import CarbonIntensityService
from ga_core.utils import utils

logger = logging.getLogger(__name__)

class HPCDataProcessor:
    """
    Data processor class to load settings, extract, process, and store logs.
    """
    def __init__(self, config_data: dict, cluster_info: dict, fixed_params: dict, all_users_access: bool = True):
        """
        Loads cluster information, fixed parameters file, database settings, and users information using config data.
        Initialses Green Algorithms Tools (GA_tools) object - used for processing logs.

        :param config_data: dict containing configurations from config file
        :param cluster_info: dict containing cluster information
        :param fixed_params: dict containing fixed parameters
        :param users_df: pd.DataFrame containing user information
        :param all_users_access: bool indicating if slurm admin rights are available
        """

        self.cluster_info = ClusterInfo.from_dict(cluster_info)
        self.fixed_params = fixed_params
        self.config_data = config_data
        self.config_data['all_users_access'] = all_users_access

    # Ingestion
    def extract_data(self, logs_raw: bytes = None) -> pd.DataFrame:
        """
        Uses the registered workload manager classes in BaseWorkloadManager to create a required object.
        Uses this object to extract logs and clean them.

        :param logs_raw: [bytes] contains raw logs that need to be processed (if this is None, logs are fetched directly from workload manager)
        :return: [pd.DataFrame] cleaned logs where each row represents one job
        """
        try:
            logger.info("Starting data extraction pipeline.")
            if 'use_mock_agg_data' in self.config_data.keys(): # DEBUGONLY Create/use some mock jobs with different users
                return utils.get_mock_agg_data()
            
            ### Pull usage statistics from the workload manager
            WM = BaseWorkloadManager.create(manager_type=self.cluster_info.workload_manager, 
                                            config_data=self.config_data, 
                                            cluster_info=self.cluster_info,
                                            logs_raw=logs_raw)
            
            df_agg = WM.extract_logs()  # Pull and clean logs
            logger.info(f"Extracted {len(df_agg)} jobs from workload manager.")

            # Check if there are any jobs during the period from this directory and with these jobIDs
            utils.check_empty_results(df_agg, self.config_data)

            # Check that there is only one user's data if no admin right
            if not self.config_data['all_users_access']:
                if len(set(df_agg.UserX)) > 1:
                    raise ValueError(f"'all_users_access' is False yet more than one user's logs was included")
                
            logger.info("Data extraction completed successfully.")
            return df_agg
        
        except Exception as e: # TODO: More robust exception handling
            logger.exception(f"Failed to extract data from workload manager: {e}")
            raise RuntimeError(f"extract_data(): failed to extract data from workload manager: {e}") from e
    
    def enrich_data(self, df: pd.DataFrame, ci_store: CIStorageBackend = None) -> pd.DataFrame:
        """
        Adds data about the carbon footprint, etc.
        :param df: [pd.DataFrame] The existing data we've extracted.
        :param fixed_params: [dict] The fixed parameters used.
        :param GA [GA_tools] A GA_tools object. 
        :return: [pd.DataFrame] The enriched data.
        """
        logger.info(f"Starting data enrichment pipeline for {len(df)} jobs...")
        try: 
            ### Fetching Carbon Intensity
            postcode = self.cluster_info.postcode
            ci_avg_data = {}
            if postcode:
                postcode = postcode[:3] # Taking only the first three letters from the postcode
                ci_service = CarbonIntensityService(postcode, ci_store)
                ci_avg_data = ci_service.calc_day_average_CI(df.StartDatetimeX.min(), df.EndDatetimeX.max())
                logger.info("Carbon intensity data fetched successfully.")
            else:
                logger.info("No postcode provided in cluster info; skipping carbon intensity lookup.")
            
            ## Energy
            df = EnergyCalculator(self.cluster_info, self.fixed_params).run(df)

            ### carbon footprint
            df = CarbonCalculator(self.cluster_info, ci_avg_data).run(df)

            ## Context metrics
            df = ContextMetricsCalculator(self.cluster_info, self.fixed_params).run(df)

            logger.info("Data enrichment completed successfully.")
            return df
        
        except Exception as e: # TODO: More robust exception handling
            logger.exception(f"enrich_data(): failed to enrich data from workload manager: {e}")
            raise RuntimeError(f"enrich_data(): failed to enrich data from workload manager: {e}") from e
