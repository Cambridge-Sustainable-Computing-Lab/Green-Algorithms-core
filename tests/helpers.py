import pandas as pd
from ga_core.data_models.normalised_job_record import NORMALISED_SCHEMA

def load_expected_csv(path: str) -> pd.DataFrame:
    """
    Load expected CSV file and convert columns to the correct dtypes based on NORMALISED_SCHEMA data model.
    :param path: Path to the expected CSV file.
    :return: DataFrame with columns converted to the correct dtypes.
    """
    df = pd.read_csv(path, dtype=str)

    for col in df.columns:
        if col in NORMALISED_SCHEMA:
            expected_dtype = NORMALISED_SCHEMA[col]
            if expected_dtype.startswith("timedelta"):
                df[col] = pd.to_timedelta(df[col])
            elif expected_dtype.startswith("datetime"):
                df[col] = pd.to_datetime(df[col])
            elif expected_dtype == "object":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(expected_dtype)
        else:
            # Enrichment-stage columns (energy, carbon, etc.) aren't covered
            # by NORMALISED_SCHEMA yet - they're all numeric in this pipeline.
            df[col] = df[col].astype("float64")

    return df