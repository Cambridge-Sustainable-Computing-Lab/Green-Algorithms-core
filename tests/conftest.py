# ------------------------------------------------------------------
# This file contains pytest fixtures (test configurations). It consists of fixtures that are to be used across multiple test files.
# These fixtures are automatically discovered by pytest and can be used in any test file without explicit import.
# 
# A fixture is the ready-made setup (or known-correct example) a test uses, so it need not be built fresh every time.
# ------------------------------------------------------------------

import pytest

@pytest.fixture
def config_data():
    return {
        "startDay": "2026-01-01",
        "endDay": "2026-04-01",
        "useCustomLogs": "tests/testdata/slurm_logs_many_cases.csv",
        "skip_db_overwrite": False,
        "db_name": "ga_dev",
        "db_host": "localhost",
        "db_port": 5432,
    }

@pytest.fixture
def cluster_info_dict():
    return {
        "institution": "University",
        "cluster_name": "OurCluster",
        "granularity_memory_request": 6.0,
        "workload_manager": "SLURM",
        "partitions": {
            "yew-himem": {
                "type": "CPU", 
                "model": "Xeon Gold 6142", 
                "TDP": 9.4
                },
            "oak": {
                "type": "GPU",
                "model": "NVIDIA A100-SXM-80GB GPUs",
                "TDP": 300,
                "model_CPU": "AMD EPYC 7763",
                "TDP_CPU": 4.4,
            },
        },
        "PUE": 1.15,
        "CI": 231.12,
        "energy_cost": {
            "cost": 0.34, 
            "currency": "£"
            },
        "postcode": "CB1",
        "default_unit_RSS": "K",
    }

@pytest.fixture
def fixed_params():
    return {
        "power_memory_perGB": 0.3725,
        "tree_month": 917,
        "passengerCar_EU_perkm": 175,
        "passengerCar_US_perkm": 251,
        "flight_NY_SF": 570000,
        "flight_PAR_LON": 50000,
        "flight_NYC_MEL": 2310000,
        "electricity_cost": 0.34,
    }