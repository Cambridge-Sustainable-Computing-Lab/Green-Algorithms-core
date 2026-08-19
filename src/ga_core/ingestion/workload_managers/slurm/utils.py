# ------------------------------------------------------------------
# Slurm specific utility functions to perform some pre-processing steps to SLURM logs.
# ------------------------------------------------------------------

import datetime
import pandas as pd
import numpy as np

from ga_core.utils import utils

class SlurmUtils:
    """
    A utility class for processing and analyzing SLURM data from HPC cluster job schedulers. 
    Handles data transformation, parsing, cleaning, and metric calculations to support 
    cluster resource management and performance analysis.
    """
    def __init__(self, cluster_info):
        self.cluster_info = cluster_info
        self.unfinished_states = ['PENDING','RUNNING','SUSPENDED','UNKNOWN','PREEMPTED'] # States from SLURM documentation: https://slurm.schedmd.com/job_state_codes.html (as of 10 Feb 2026)
        
    def convert_to_GB(self, memory, unit):
        """
        Converts data quantity into GB.
        :param memory: [float] quantity to convert
        :param unit: [str] unit of `memory`, has to be one of ['M', 'G', 'K', 'T']
        :return: [float] memory in GB.
        """
        assert unit in ['M', 'G', 'K', 'T'] 
        if unit == 'M':
            memory /= 1e3
        elif unit == 'K':
            memory /= 1e6
        elif unit == 'T':
            memory *= 1e3
        return memory

    def calc_ReqMem(self, x):
        """
        Calculates the total memory required when submitting the job.
        :param x: [pd.Series] one row of sacct output.
        :return: [float] total required memory, in GB.

        ReqMem Amount of memory requested; suffixed with 'c' if per CPU, 'n' if per node
        """
        mem_raw, n_nodes, n_cores = x['ReqMem'], x['NNodes'], x['NCPUS']
        valid_units = ['M', 'G', 'K', 'T']

        if pd.isnull(mem_raw) or mem_raw in ('0', '0n', '0c'):
            return self.convert_to_GB(0, 'G')

        elif mem_raw[-1] == 'n':
            unit = mem_raw[-2]
            memory = float(mem_raw[:-2]) * n_nodes
        elif mem_raw[-1] == 'c':
            unit = mem_raw[-2]
            memory = float(mem_raw[:-2]) * n_cores
        elif mem_raw[-1] in valid_units:
            unit = mem_raw[-1]
            memory = float(mem_raw[:-1])
        else:
            unit = None

        if unit not in valid_units:
            raise ValueError(f"Can't parse memory value: {mem_raw}. Please raise issue on GitHub.")

        return self.convert_to_GB(memory, unit)

    def clean_RSS(self, x):
        """
        Cleans the RSS value in sacct output.
        :param x: [NaN or str] the RSS value, either NaN or of the form '2745K'
        (optionally, just a number, we then use default_unit_RSS from cluster_info.yaml as unit).
        :return: [float] RSS value, in GB.
        """
        if pd.isnull(x.MaxRSS):
            # NB if no info on MaxRSS, we assume all memory was used
            memory = -1
        elif x.MaxRSS == '0':
            memory = 0
        else:
            assert isinstance(x.MaxRSS, str)
            # Special case for the situation where MaxRSS is of the form '154264' without a unit.
            if x.MaxRSS[-1].isalpha():
                memory = self.convert_to_GB(float(x.MaxRSS[:-1]), x.MaxRSS[-1])
            else:
                assert 'default_unit_RSS' in self.cluster_info, "Some values of MaxRSS don't have a unit. Please specify a default_unit_RSS in cluster_info.yaml"
                memory = self.convert_to_GB(float(x.MaxRSS), self.cluster_info.default_unit_RSS)

        return memory

    def clean_UsedMem(self, x):
        """
        Cleans the UsedMemory column
        :param x:
        :return: [float]
        """
        # NB when MaxRSS didn't store any values, we assume that "memory used = memory requested"
        return x.ReqMemX if x.UsedMem_ == -1 else x.UsedMem_

    def clean_partition(self, x):
        """
        Cleans the partition field, by replacing NaNs with empty string and selecting just one partition per job.
        :param x: data frame
        :return: [str] one partition or empty string

        x.Partition is [str] partition or comma-separated list of partitions
        """
        if pd.isnull(x.Partition):  # e.g. if it's NaN
            return ''

        L_partitions = x.Partition.split(',')
        if (x.WallclockTimeX.total_seconds() > 0) & (len(L_partitions) > 1):
            # Multiple partitions logged is only an issue for jobs that never started,
            # for the others, only the used partition is logged
            print(f"\n-!- WARNING: Multiple partitions logged on a job than ran: {x.JobID} - {x.Partition} (using the first one)\n")

        return L_partitions[0]

    def set_partitionType(self, x):
        assert x in self.cluster_info.hardware_profiles.keys(), f"\n-!- Unknown hardware profile: {x} -!-\n"
        return self.cluster_info.hardware_profiles[x].type

    def parse_timedelta(self, x):
        """
        Parse a string representing a duration into a `datetime.timedelta` object.
        :param x: [str] Duration, as '[DD-HH:MM:]SS[.MS]'
        :return: [datetime.timedelta] Timedelta object
        """
        # Parse number of days
        day_split = x.split('-')
        if len(day_split) == 2:
            n_days = int(day_split[0])
            HHMMSSms = day_split[1]
        else:
            n_days = 0
            HHMMSSms = x

        # Parse ms
        ms_split = HHMMSSms.split('.')
        if len(ms_split) == 2:
            n_ms = int(ms_split[1])
            HHMMSS = ms_split[0]
        else:
            n_ms = 0
            HHMMSS = HHMMSSms

        # Parse HH,MM,SS
        last_split = HHMMSS.split(':')
        if len(last_split) == 3:
            to_add = []
        elif len(last_split) == 2:
            to_add = ['00']
        elif len(last_split) == 1:
            to_add = ['00', '00']
        else:
            raise ValueError(f"Can't parse {x}")
        n_h, n_m, n_s = list(map(int, to_add + last_split))

        return datetime.timedelta(
            days=n_days, hours=n_h, minutes=n_m, seconds=n_s, milliseconds=n_ms
        )

    def calc_realMemNeeded(self, x, granularity_memory_request):
        """
        Calculate the minimum memory needed.
        This is calculated as the smallest multiple of `granularity_memory_request` that is greater than maxRSS.
        :param x: [pd.Series] one row of sacct output.
        :param  granularity_memory_request: [float or int] level of granularity available when requesting memory on this cluster
        :return: [float] minimum memory needed, in GB.
        """
        minimum_mem = (int(x.UsedMem2_ / granularity_memory_request) + 1) * granularity_memory_request
        return minimum_mem if x.ReqMemX < x.UsedMem2_ else min(x.ReqMemX, minimum_mem)

    def calc_memory_overallocation(self, x):
        """
        Calculate the overallocation factor, as the ratio between memory requested and memory needed.
        
        :param x: [pd.Series] one row of sacct output.
        :return: [float] overallocation factor, with 1 meaning no overallocation.
        """
        # This is in case ReqMem is wrong or too low
        if x.NeededMemX == 0 and x.ReqMemX == 0:
            return 1.0
        elif x.NeededMemX == 0: # Edge Case - needs revisiting
            return 10.0  # Arbitrary high value to reflect the fact that there was a lot of overallocation.
        return max(1.0, x.ReqMemX / x.NeededMemX)

    def calc_CPUusage2use(self, x):
        if x.TotalCPUtime_.total_seconds() == 0:
            # This is when the workload manager actually didn't store real usage
            # NB: when TotalCPU=0, we assume usage factor = 100% for all CPU cores
            return x.CPUwallclocktime_

        return x.TotalCPUtime_

    def calc_GPUusage2use(self, x):
        if x.PartitionTypeX != 'GPU':
            return datetime.timedelta(0)
        if x.WallclockTimeX.total_seconds() > 0:
            assert x.NGPUS_ != 0
        return x.WallclockTimeX * x.NGPUS_  # NB assuming usage factor of 100% for GPUs

    def calc_coreHoursCharged(self, x):
        '''
        Split CPU and GPU core hours charged, depending on the partition.
        :param x:
        :return: [(float, float)]
        '''
        if x.PartitionTypeX == 'CPU':
            return x.CPUwallclocktime_ / np.timedelta64(1, 'h'), 0.
        else:
            return 0., x.WallclockTimeX * x.NGPUS_ / np.timedelta64(1, 'h')

    def clean_State(self, x, customSuccessStates_list):
        """
        Standardise the job's state, coding with {-1,0,1}
        :param x: [str] "State" field from sacct output
        :return: [int] in [-1,0,1]
        """
        # Codes are found here: https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
        success_codes = ['CD', 'COMPLETED']
        if x in success_codes:
            codeState = 1
        elif x in customSuccessStates_list:
            # we allocate a lower value here so that when aggregating by jobID, the whole job keeps the flag
            # Otherwise a "cancelled" job could take over with StateX=0 for example
            codeState = -1
        else:
            codeState = 0

        return codeState

    def get_parent_jobID(self, x):
        """
        Get the parent job ID in case of array jobs
        :param x: [str] JobID of the form 123456789_0 (with or without '_0')
        :return: [str] Parent ID 123456789
        """
        job_id_parts = x.split('_')
        assert len(job_id_parts) <= 2, f"Can't parse the job ID: {x}"
        return job_id_parts[0]
    
        ### Other utility methods
    def raw_logs_to_df(self):
        """
        Convert raw logs output into a pandas dataframe - calling the static method convert2dataframe
        """
        self.logs_df = utils.convert2dataframe(self.logs_raw, types = {'NNodes': 'int64', 'NCPUS': 'int64'}, delimiter="|")


    def concat_logs_df(self, new_logs_df: pd.DataFrame):
        """
        Concatenate the existing logs dataframe with a new one, for example when we want to add finished jobs to previously-fetched logs.
        :param new_logs_df: [pd.DataFrame] new logs dataframe to concatenate with the existing one.
        """
        if self.logs_df is None:
            raise ValueError("logs_df is not initialised. Run pull_logs() and raw_logs_to_df() first.")
        
        self.logs_df = pd.concat([self.logs_df, new_logs_df], ignore_index=True)

    def filter_finished_jobs(self) -> pd.DataFrame:
        '''
        Filter finished jobs from the logs dataframe using the 'End' column if available, else the 'State' column.
        A job is considered finished only if ALL of its rows (parent + steps) are finished.
        '''
        if self.logs_df.empty:
            return self.logs_df.copy()
          
        single_jobID = self.logs_df['JobID'].str.split('.').str[0]
        
        if 'End' in self.logs_df:
            row_finished = self.logs_df['End'].notna() & (self.logs_df['End'] != "Unknown")
        elif 'State' in self.logs_df.columns:
            ## NOTE: This is a temporary workaround for retrocompatibility since in earlier versions 'End' field was not fetched. Must be removed eventually.
            row_finished = ~self.logs_df['State'].isin(self.unfinished_states) 
        else:
            raise KeyError(
                f"Cannot filter finished jobs: neither 'End' nor 'State' columns exist in logs_df. "
                f"Found columns: {list(self.logs_df.columns)}"
            )
        
        # Group row_finished by single_jobID (aligned by index); True only if all rows in the job finished    
        job_finished = row_finished.groupby(single_jobID).transform('all')
        return self.logs_df[job_finished].copy()

    def clean_nodes_list(self, x) -> list:
        """
        Clean the node list field, by replacing NaNs with empty string and parsing the node list into a list of nodes.
        :param x: data frame
        :return: [list] list of nodes or empty list
        """
        if 'NodeList' not in x.index or pd.isnull(x.NodeList) or x.NodeList == "" or x.NodeList == "None assigned": 
            return []
        
        nodeList = NodeListUtil().parse_list(x.NodeList)
        return nodeList

    def get_partition_hardware_profile(self, x):
        """
        Get the hardware profile associated with the Partition/NodeList of a job, based on the cluster_info configuration.
        """
        partition_name = x.PartitionX
        cluster_partitions = self.cluster_info.partitions

        if partition_name not in cluster_partitions:
            raise ValueError(f"Unknown partition '{partition_name}' for job {x.name}; Known partitions: {list(cluster_partitions.keys())}")
        if cluster_partitions[partition_name].hardware_profile is not None:
            return cluster_partitions[partition_name].hardware_profile
        else:
            # Using NodeList to find the hardware profile
            # x.NodesList_ is a list of nodes on which the job ran, e.g. ['cpu-1', 'cpu-2']
            if x.NodesList_ is None or len(x.NodesList_) == 0:
                if x.WallclockTimeX.total_seconds() > 0:
                    raise ValueError(f"Job {x.name} seems to have run but no nodes were assigned to it.")
                return None  # No nodes available to determine hardware profile
            for i in x.NodesList_:
                node_list = cluster_partitions[partition_name].node_list
                for node_range in node_list:
                    if node_range.contains(i): # checks if the node is in the range
                        return node_range.hardware_profile
                    
            raise ValueError(f"Could not find hardware profile in partition '{partition_name}' with NodeList {x.NodesList_} for job {x.name}. Please check cluster_info configuration.")
                        

class NodeListUtil:
    """
    A utility class for parsing and handling SLURM node lists.

    Picked from 'magic slurm node list parser' https://gist.github.com/ebirn/cf52876120648d7d85501fcbf185ff07
    """

    def parse_int(self, s):
        for i, c in enumerate(s):
            if c not in "0123456789":
                return s[:i], s[i:]
        return s, ""

    def parse_brackets(self, s):
        # parse a "bracket" expression (including closing ']')
        lst = []
        while len(s) > 0:
            if s[0] == ',':
                s = s[1:]
                continue
            if s[0] == ']':
                return lst, s[1:]
            a, s = self.parse_int(s)
            assert len(s) > 0, "Missing closing ']'"
            if s[0] in ',]':
                lst.append(a)
            elif s[0] == '-':
                b, s = self.parse_int(s[1:])
                assert int(a) <= int(b), 'Invalid range'
                # A leading zero on a lower boundary suggests that the
                # numerical part of the node name is padded with zeros,
                # e.g. nia0001.
                #
                # Just a single 0 on the lower boundary suggests a numerical
                # range without padding, e.g. nia[0-4].
                if a != '0' and a.startswith('0'):
                    assert len(a) == len(b), \
                        (
                            'Boundaries of a ranged string with padding '
                            'must have the same length.'
                        )
                    lst.extend(
                        [str(x).zfill(len(a)) for x in range(int(a), int(b) + 1)]
                    )
                elif a != '0' and b.startswith('0'):
                    raise ValueError('Could not determine the padding style.')
                # If no padding is detected, simply use the range.
                else:
                    lst.extend(
                        [str(x) for x in range(int(a), int(b) + 1)]
                    )
        assert len(s) > 0, "Missing closing ']'"

    def parse_node(self, s):
        # parse a "node" expression
        for i, c in enumerate(s):
            if c == ',':  # name,...
                return [s[:i]], s[i + 1:]
            if c == '[':  # name[v],...
                b, rest = self.parse_brackets(s[i + 1:])
                if len(rest) > 0:
                    assert rest[0] == ',', \
                        f"Expected comma after brackets in {s[i:]}"
                    rest = rest[1:]
                return [s[:i] + z for z in b], rest
    
        return [s], ""

    def parse_list(self, s) -> list:
        lst = []
        while len(s) > 0:
            v, s = self.parse_node(s)
            lst.extend(v)
        return lst
