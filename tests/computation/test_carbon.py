# tests/compuation/test_carbon.py

# ------------------------------------------------------------------
# This file contains pytest unit tests for computation/carbon.py.
# ------------------------------------------------------------------

from datetime import timedelta

import pandas as pd
import pytest

from ga_core.computation.carbon import CarbonCalculator


class TestCarbon:
    def _row(self, start, end, wallclock=timedelta(hours=1), energy=1.0):
        return pd.Series(
            {
                "StartDatetimeX": start,
                "EndDatetimeX": end,
                "WallclockTimeX": wallclock,
                "energy": energy,
            }
        )

    def test_missing_job_start_timestamp(self):
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

    def test_single_day_job_carbon_emission(self):
        """
        Scenario: A two-hour job runs within one day with a known daily carbon intensity.
        """
        calculator = CarbonCalculator(cluster_info=None)

        result = calculator._calc_carbonFootprint_by_row(
            self._row(
                pd.Timestamp("2026-01-01 10:00:00"),
                pd.Timestamp("2026-01-01 12:00:00"),
                wallclock=timedelta(hours=2),
                energy=10.0,
            ),
            "",
            {"01-01-2026": 100.0},
        )

        assert result == 1000.0

    def test_multi_day_job_carbon_emission(self):
        """
        Scenario: An eight-hour job crosses midnight and uses each day's carbon intensity for the hours worked on that day.
        """
        calculator = CarbonCalculator(cluster_info=None)

        result = calculator._calc_carbonFootprint_by_row(
            self._row(
                pd.Timestamp("2026-01-01 20:00:00"),
                pd.Timestamp("2026-01-02 04:00:00"),
                wallclock=timedelta(hours=8),
                energy=8.0,
            ),
            "",
            {
                "01-01-2026": 100.0,
                "02-01-2026": 200.0,
            },
        )

        assert result == pytest.approx(1200.0)

    def test_missing_end_timestamp(self):
        """
        Scenario: A job with a valid start timestamp but missing/NaT end timestamp is treated as incomplete.
        """
        calculator = CarbonCalculator(cluster_info=None)

        result = calculator._calc_carbonFootprint_by_row(
            self._row(pd.Timestamp("2026-01-01 01:00:00"), pd.NaT),
            "",
            {"01-01-2026": 100.0},
        )

        assert result == 0.0
