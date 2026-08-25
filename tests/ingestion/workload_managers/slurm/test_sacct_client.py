# tests/ingestion/workload_managers/slurm/test_sacct_client.py

# ------------------------------------------------------------------
# This file contains pytest unit tests for slurm/sacct_client.py.
# ------------------------------------------------------------------

import pytest
from ga_core.ingestion.workload_managers.slurm.sacct_client import SacctClient

class TestScreenSacctRows:
    """
    Testing SacctClient.screen_sacct_rows focusing on schema validation.
    """

    @pytest.fixture(autouse=True)
    def mock_sacct_schema(self, monkeypatch):
        """
        Mocking the SacctClient object with mock class variables
        """
        monkeypatch.setattr(
            SacctClient, "sacct_fields", ["UID", "User", "JobID", "State"]
        )

    @pytest.fixture
    def valid_header(self):
        return "|".join(SacctClient.sacct_fields)

    def test_valid_data_passes_through(self, valid_header):
        """
        Scenario: Process a valid row. It must pass the test cleanly
        """
        valid_row = "1001|alice|55500|COMPLETED"
        raw_input = f"{valid_header}\n{valid_row}\n".encode("utf-8")

        cleaned_bytes, malformed = SacctClient.screen_sacct_rows(raw_input)

        assert cleaned_bytes == raw_input
        assert malformed == []

    @pytest.mark.parametrize("invalid_char", ["*", "#", "-", "@", "_", " "])
    def test_quarantines_invalid_leading_character(self, valid_header, invalid_char):
        """
        Scenario: Process rows starting with an invalid character. These must be caught as malformed
        """
        invalid_row = f"{invalid_char}1001|alice|55500|COMPLETED"
        raw_input = f"{valid_header}\n{invalid_row}".encode("utf-8")

        cleaned_bytes, malformed = SacctClient.screen_sacct_rows(raw_input)

        assert cleaned_bytes == f"{valid_header}\n".encode("utf-8")
        assert len(malformed) == 1
        assert malformed[0].startswith("[invalid_leading_char]")

    @pytest.mark.parametrize(
        "bad_row, actual_count",
        [
            ("1001|alice|55500", 3),                
            ("1001|alice|55500|COMPLETED|EXTRA", 5),  
        ],
    )
    def test_quarantines_field_count_mismatch(self, valid_header, bad_row, actual_count):
        """
        Scenario: Process rows with mismatching column lengths from what is expected. These must be caught as malformed.
        """
        raw_input = f"{valid_header}\n{bad_row}".encode("utf-8")

        cleaned_bytes, malformed = SacctClient.screen_sacct_rows(raw_input)

        assert cleaned_bytes == f"{valid_header}\n".encode("utf-8")
        assert len(malformed) == 1
        assert f"[field_count={actual_count}, expected=4]" in malformed[0]

    def test_handles_mixed_batch(self, valid_header):
        """
        Scenario: Processes a mix of valid, blank, and malformed rows correctly in a single stream.
        """
        lines = [
            valid_header,
            "1001|alice|55500|COMPLETED", # valid
            "*1001|alice|55500|COMPLETED", # invalid leading char
            "1002|bob|55501", # Too few fields
            "1003|charlie|55502|FAILED", # valid
        ]
        raw_input = "\n".join(lines).encode("utf-8")

        cleaned_bytes, malformed = SacctClient.screen_sacct_rows(raw_input)

        expected_cleaned = (
            f"{valid_header}\n"
            "1001|alice|55500|COMPLETED\n"
            "1003|charlie|55502|FAILED\n"
        ).encode("utf-8")

        assert cleaned_bytes == expected_cleaned
        assert len(malformed) == 2
        assert "[invalid_leading_char]" in malformed[0]
        assert "[field_count=3, expected=4]" in malformed[1]