# ------------------------------------------------------------------
# This file implements the BaseWorkloadManager abstract class for SLURM.
# It contains SLURM specifc functions to pull and clean logs.
# ------------------------------------------------------------------

import datetime
import logging
import pandas as pd

from ga_core.ingestion.workload_managers.base import BaseWorkloadManager
from ga_core.ingestion.workload_managers.slurm.utils import SlurmUtils
from ga_core.ingestion.workload_managers.slurm.sacct_client import SacctClient

logger = logging.getLogger(__name__)

class SlurmManager(SlurmUtils, BaseWorkloadManager, manager_type="slurm"):
    """
    This class implements the BaseWorkloadManager abstract class and inherits from SlurmUtils. 
    'manager_type="slurm"' is used to register this class with the register based factory defined in BaseWorkloadManager.
    """

    def __init__(self, config_data:dict, cluster_info, logs_raw):
        """
        Methods related to the Workload manager
        :param config_data: [dict] Configuration data
        :param cluster_info: [dict] information about this specific cluster.
        """
        SlurmUtils.__init__(self, cluster_info=cluster_info)
        BaseWorkloadManager.__init__(self, logs_raw=logs_raw)

        self.config_data = config_data
        self.logs_df = None
        self.df_agg_0 = None
        self.df_agg = None
        self.df_agg_X = None
    
    ### Implements abstract methods from BaseWorkloadManager
    def pull_logs(self):
        """
        Run the command line to pull usage from the workload manager.
        More: https://slurm.schedmd.com/sacct.html
        """
        try:
            logger.info(f"Pulling logs via sacct for the time period {self.config_data['startDay']} - {self.config_data['endDay']}")
            self.logs_raw = SacctClient.pull_logs_by_time(
                self.config_data['startDay'],
                self.config_data['endDay'],
                self.config_data['all_users_access']
            )
            logger.info(f"Successfully pulled raw logs.")
        except Exception as e:
            logger.exception(f"Failed to pull logs using config {self.config_data}: {e}")
            raise RuntimeError(f"Failed to pull logs using config {self.config_data}: {e}") from e     
    
    def clean_logs(self):
        """
        Clean the different fields of the usage logs.
        NB: the name of the columns ending with X need to be conserved, as they are used by the main script.
        """
        self.raw_logs_to_df()
        logger.info(f"Loaded {len(self.logs_df)} raw log rows into dataframe")

        self.logs_df = self.filter_finished_jobs() # Keep only those jobs that have finished - i.e. contains a valid End date/ finished state
        logger.info(f"{len(self.logs_df)} rows remain after filtering for finished jobs")

        if self.logs_df.empty:
            logger.error(f"No finished jobs found for period {self.config_data['startDay']} to {self.config_data['endDay']}")
            raise ValueError(f"No finished jobs found in the logs for the period {self.config_data['startDay']} to {self.config_data['endDay']}")     
        else:
            ### Calculate real memory usage
            self.logs_df['ReqMemX'] = self.logs_df.apply(self.calc_ReqMem, axis=1)

            ### Clean MaxRSS
            self.logs_df['UsedMem_'] = self.logs_df.apply(self.clean_RSS, axis=1)

            ### Parse wallclock time
            self.logs_df['WallclockTimeX'] = self.logs_df['Elapsed'].apply(self.parse_timedelta)

            ### Parse total CPU time
            # This is the total CPU used time, accross all cores.
            # But it is not reliably logged
            self.logs_df['TotalCPUtime_'] = self.logs_df['TotalCPU'].apply(self.parse_timedelta)

            ### Parse core-wallclock time
            # This is the maximum time cores could use, if used at 100% (Elapsed time * CPU count)
            self.logs_df['CPUwallclocktime_'] = self.logs_df['CPUTime'].apply(self.parse_timedelta)

            ### Number of GPUs
            # TODO double check that it includes multiple GPUs correctly
            if 'AllocTRES' in self.logs_df.columns:
                self.logs_df['NGPUS_'] = \
                    self.logs_df.AllocTRES.str.extract(r'((?<=gres\/gpu=)\d+)', expand=False).fillna(0).astype('int64')
            else:
                print('Using old logs, "AllocTRES" information not available.')  # TODO: remove this after a while
                self.logs_df['NGPUS_'] = 0

            ### Clean partition
            # Make sure it's either a partition name, or a comma-separated list of partitions
            self.logs_df['PartitionX'] = self.logs_df.apply(self.clean_partition, axis=1)
            self.logs_df['NodesList_'] = self.logs_df.apply(self.clean_nodes_list, axis=1)

            ### Parse datetimes - Submit, Start, End
            self.logs_df['SubmitDatetimeX'] = self.logs_df.Submit.apply(
                lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))
            
            self.logs_df['StartDatetimeX'] = self.logs_df.Start.apply(
                lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S") if pd.notnull(x) else pd.NaT)
            
            self.logs_df['EndDatetimeX'] = self.logs_df.End.apply(
                lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S") if pd.notnull(x) else pd.NaT)

            ### Number of CPUs
            # e.g. here there is no cleaning necessary, so I just standardise the column name
            self.logs_df['NCPUS_'] = self.logs_df.NCPUS

            ### Number of nodes
            self.logs_df['NNodes_'] = self.logs_df.NNodes

            ### Job name
            self.logs_df['JobName_'] = self.logs_df.JobName

            ### Working directory
            self.logs_df['WorkingDir_'] = self.logs_df.WorkDir

            ### Username and UID
            self.logs_df['UIDX'] = self.logs_df.UID
            self.logs_df['UserX'] = self.logs_df.User

            ### State
            customSuccessStates_list = self.config_data["customSuccessStates"].split(',') if 'customSuccessStates' in self.config_data.keys() else []
            self.logs_df['StateX'] = self.logs_df.State.apply(self.clean_State,
                                                            customSuccessStates_list=customSuccessStates_list)

            ### Pull jobID
            self.logs_df['single_jobID'] = self.logs_df.JobID.apply(lambda x: x.split('.')[0])

            ### Account
            if 'Account' in self.logs_df.columns:
                self.logs_df['Account_'] = self.logs_df.Account
            else:
                print('Using old logs, "Account" information not available.')  # TODO: remove this after a while
                self.logs_df['Account_'] = ''

            ### Aggregate per jobID
            self.df_agg = self.logs_df.groupby('single_jobID').agg({
                'TotalCPUtime_': 'max',
                'CPUwallclocktime_': 'max',
                'WallclockTimeX': 'max',
                'ReqMemX': 'max',
                'UsedMem_': 'max',
                'NCPUS_': 'max',
                'NGPUS_': 'max',
                'NNodes_': 'max',
                'PartitionX': lambda x: ''.join(x),
                'JobName_': 'first',
                'SubmitDatetimeX': 'min',
                'StartDatetimeX': 'min',
                'EndDatetimeX': 'min',
                'WorkingDir_': 'first',
                'StateX': 'min',
                'Account_': 'first',
                'UIDX': 'first',
                'UserX': 'first',
                "NodesList_": 'first',
            })

            self.df_agg.loc[self.df_agg.StateX == -1, 'StateX'] = 1 # Turn StateX==-1 into 1 (customSuccessStates are considered successful i.e. 1)

            ### Replace UsedMem_=-1 with memory requested (for when MaxRSS=NaN)
            self.df_agg['UsedMem2_'] = self.df_agg.apply(self.clean_UsedMem, axis=1)

            ### Hardware profile per job, based on the partition and the node list
            self.df_agg['HardwareProfileX'] = self.df_agg.apply(self.get_partition_hardware_profile, axis=1)

            ### Drop jobs where no hardware profile could be determined
            missing_hw_profile = self.df_agg.HardwareProfileX.isna() | (self.df_agg.HardwareProfileX == '')
            if missing_hw_profile.sum() > 0:
                logger.warning(f"Dropping {missing_hw_profile.sum()} jobs with no matching hardware profile")
            self.df_agg = self.df_agg.loc[~missing_hw_profile]

            ### Label as CPU or GPU partition
            self.df_agg['PartitionTypeX'] = self.df_agg.HardwareProfileX.apply(self.set_partitionType)

            # Just used to clean up with old logs:
            if 'AllocTRES' not in self.logs_df.columns:
                self.df_agg.loc[self.df_agg.PartitionTypeX == 'GPU', 'NGPUS_'] = 1  # TODO remove after a while

            # Sanity check (no GPU logged for CPU partitions and vice versa)
            cpu_hardware_prof_with_gpus = self.df_agg.loc[(self.df_agg.PartitionTypeX == 'CPU') & (self.df_agg.NGPUS_ != 0)]
            assert cpu_hardware_prof_with_gpus.empty, f"Found job(s) on a CPU partition with NGPUS_ != 0: {cpu_hardware_prof_with_gpus.single_jobID.tolist()}"

            # Cancelled GPU jobs won't have any GPUs allocated if they didn't start
            foo = self.df_agg.loc[(self.df_agg.PartitionTypeX == 'GPU') & (self.df_agg.NGPUS_ == 0)]
            failed_gpu_jobs = foo.loc[foo.WallclockTimeX.dt.total_seconds() != 0]
            assert failed_gpu_jobs.empty, (f"Found {len(failed_gpu_jobs)} GPU-partition job(s) with NGPUS_ == 0 but nonzero wallclock time: {failed_gpu_jobs.single_jobID.tolist()}")

            ## Check that there is no missing UID/User
            if self.df_agg.UIDX.isnull().sum() > 0:
                logger.warning(f"{self.df_agg.UIDX.isnull().sum()} jobs have missing UIDs")
            if self.df_agg.UserX.isnull().sum() > 0:
                logger.warning(f"{self.df_agg.UserX.isnull().sum()} jobs have missing Usernames")

            ### add the usage time to use for calculations
            self.df_agg['TotalCPUtime2useX'] = self.df_agg.apply(self.calc_CPUusage2use, axis=1)
            self.df_agg['TotalGPUtime2useX'] = self.df_agg.apply(self.calc_GPUusage2use, axis=1)

            ### Calculate core-hours charged
            self.df_agg[['CPUhoursChargedX', 'GPUhoursChargedX']] = self.df_agg.apply(self.calc_coreHoursCharged, axis=1, result_type='expand')

            ### Calculate real memory need
            self.df_agg['NeededMemX'] = self.df_agg.apply(
                self.calc_realMemNeeded,
                granularity_memory_request=self.cluster_info.granularity_memory_request,
                axis=1)

            ### Add memory waste information
            self.df_agg['memOverallocationFactorX'] = self.df_agg.apply(self.calc_memory_overallocation, axis=1)

            # foo = self.df_agg[['TotalCPUtime_', 'CPUwallclocktime_', 'WallclockTimeX', 'NCPUS_', 'CoreHoursChargedCPUX',
            #                    'CoreHoursChargedGPUX', 'TotalCPUtime2useX', 'TotalGPUtime2useX']] # DEBUGONLY

            ### Filter on working directory
            if 'filterWD' in self.config_data.keys():
                if self.config_data['filterWD'] is not None:
                    # FIXME: Doesn't work with symbolic links
                    self.df_agg = self.df_agg.loc[self.df_agg.WorkingDir_ == self.config_data['filterWD']]

            ### Filter on Job ID
            self.df_agg.reset_index(inplace=True)
            self.df_agg['parentJobID'] = self.df_agg.single_jobID.apply(self.get_parent_jobID)

            if 'filterJobIDs' in self.config_data.keys():
                if self.config_data['filterJobIDs'] != 'all':
                    list_jobs2keep = self.config_data['filterJobIDs'].split(',')
                    self.df_agg = self.df_agg.loc[self.df_agg.parentJobID.isin(list_jobs2keep)]

            ### Filter on Account
            if 'filterAccount' in self.config_data.keys():
                if self.config_data['filterAccount'] is not None:
                    self.df_agg = self.df_agg.loc[self.df_agg.Account_ == self.config_data['filterAccount']]

            self.df_agg_X = self.df_agg[[x for x in self.df_agg.columns if x[-1] == 'X']]
            
            logger.info(f"clean_logs produced {len(self.df_agg_X)} aggregated job rows")
            return self.df_agg_X

    def validate_raw_logs(self, logs_raw: bytes = None):
        if logs_raw:
            if isinstance(logs_raw, (bytes, bytearray)):
                try:
                    text = logs_raw.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ValueError(f"Couldn't decode logs_raw as utf-8: {exc}") from exc
            else:
                text = logs_raw

            lines = [ln for ln in text.splitlines() if ln.strip()]
            if not lines:
                raise ValueError("logs_raw contains no non-empty lines")
            header = lines[0].split('|')
            missing = [col for col in SacctClient.sacct_fields if col not in header]
            if missing:
                raise ValueError(f"logs_raw is missing required column(s): {missing}")

        