from datetime import timedelta

import pandas as pd

from ga_core.computation.carbon import CarbonCalculator


class TestCarbon:
    def _row(self, start, end):
        return pd.Series(
            {
                "StartDatetimeX": start,
                "EndDatetimeX": end,
                "WallclockTimeX": timedelta(hours=1),
                "energy": 1.0,
            }
        )

    def test_missing_job_timestamps_do_not_raise_or_emit_carbon(self):
        """
        Scenario: A job with missing/NaT start timestamp should return 0.0 carbon emissions and not raise an error - considering the job as incomplete.
        """
        calculator = CarbonCalculator(cluster_info=None)

        result = calculator._calc_carbonFootprint_by_row(
            self._row(pd.NaT, pd.Timestamp("2026-01-01 01:00:00")), #no start timestamp
            "",
            {"01-01-2026": 100.0}, #Carbon intensity
        )

        assert result == 0.0
