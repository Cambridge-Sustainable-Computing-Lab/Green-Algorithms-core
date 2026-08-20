# ------------------------------------------------------------------
# Contains the client for interacting with SLURM using 'sacct' command.
# ------------------------------------------------------------------

import subprocess
import re

class SacctClient:
    """
    Client to interact with the SLURM workload manager using the 'sacct' command.
    Contains separate methods to pull logs in different contexts (by time, by JobID, etc.) and can be extended with more methods as needed.
    """
    sacct_fields = ["UID","User","JobID","JobName","Submit","Start",'End',"Elapsed","Partition","NNodes",
                "NCPUS","TotalCPU","CPUTime","ReqMem","MaxRSS","WorkDir","State","Account","AllocTRES"]
    bash_com = [
                "sacct",
                "--format",
                ",".join(sacct_fields),
                "-P",
                "-L"  # All clusters
            ]
    
    @classmethod
    def screen_sacct_rows(cls, data: bytes) -> tuple[bytes, list[str]]:
        """
        Preliminary schema check on raw sacct output.
        
        Checks done:
        1. Row must have the expected number of fields
        2. Row must not begin with a non-alphanumeric/special character
        3. Rows that fail either check are logged, not silently dropped/altered

        :param data: raw bytes 
        :return: (cleaned_bytes, quarantined_lines)
        """
        VALID_LEADING_CHAR = re.compile(r'^[A-Za-z0-9]')
        expected_field_count = len(cls.sacct_fields)
        expected_header = '|'.join(cls.sacct_fields)

        text = data.decode('utf-8', errors='replace')
        rows = text.splitlines()

        screened_rows = []
        malformed_rows = []
        header_seen = False

        for row in rows:
            if not row.strip():
                continue

            if not header_seen and row == expected_header:
                screened_rows.append(row)
                header_seen = True
                continue

            if not VALID_LEADING_CHAR.match(row):
                malformed_rows.append(f"[invalid_leading_char] {row}")
                continue

            row_field_count = len(row.split('|'))
            if row_field_count != expected_field_count:
                malformed_rows.append(
                    f"[field_count={row_field_count}, expected={expected_field_count}] {row}"
                )
                continue

            screened_rows.append(row)

        screened_text = '\n'.join(screened_rows) + '\n'
        return screened_text.encode('utf-8'), malformed_rows
    
    @classmethod
    def pull_logs_by_time(cls, startDay, endDay, all_users=False):
        """
        Run the command line to pull usage from the workload manager by time.
        All Jobs started between the given start date and end date are pulled.
        More: https://slurm.schedmd.com/sacct.html

            Parameters:
            startDay (str): The start date in the format "YYYY-MM-DD".
            endDay (str): The end date in the format "YYYY-MM-DD".
            all_users (bool): Whether to pull logs for all users i.e. in case of Admin access. Default is False (pull logs for the current user only).
        """
        try:
            bash_com_full = cls.bash_com + [
                "--starttime", startDay,
                "--endtime", endDay
            ]

            if all_users:
                bash_com_full.append("--allusers")

            logs = subprocess.run(bash_com_full, capture_output=True)
            return logs.stdout   
        except Exception as e:
            raise RuntimeError(f"(SacctClient.pull_logs_by_time) Error occurred while pulling logs by time using sacct: {e}") from e