from typing import ClassVar


class SacctSchema:
    required_fields: ClassVar[list[str]] = [
        "UID",
        "User",
        "JobID",
        "JobName",
        "Submit",
        "Start",
        'End',
        "Elapsed",
        "Partition",
        "NNodes",
        "NCPUS",
        "TotalCPU",
        "CPUTime",
        "ReqMem",
        "MaxRSS",
        "WorkDir",
        "State",
        "Account",
        "AllocTRES",
        "NodeList"
    ]

    optional_fields: ClassVar[list[str]] = [
        "CPUTimeRAW",
        "ElapsedRaw",
        "ConsumedEnergyRaw",
        "ConsumedEnergy",
    ]

    @classmethod
    def get_required_fields(cls) -> list[str]:
        return cls.required_fields

    @classmethod
    def get_all_fields(cls) -> list[str]:
        return cls.required_fields + cls.optional_fields

    @classmethod
    def get_bash_command(cls, all = True) -> list[str]:
        """
        Returns the bash command to pull logs from SLURM using sacct command.
        If all is True, returns the command with all fields (required + optional).
        If all is False, returns the command with only required fields.
        """
        if all:
            sacct_fields = SacctSchema.get_all_fields()
        else:
            sacct_fields = SacctSchema.get_required_fields()

        bash_com = [
                    "sacct",
                    "--format",
                    ",".join(sacct_fields),
                    "-P", # Parseable output (pipe-separated)
                    "-L"  # All clusters
                ]
        return bash_com