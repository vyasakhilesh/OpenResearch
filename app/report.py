import pandas as pd
import numpy as np
from typing import Optional

def data_quality_report(df: pd.DataFrame, sample_values: int = 5) -> pd.DataFrame:
    """
    Compute data quality metrics for each column in df.
    Returns a DataFrame with one row per column.
    """
    rows = []
    n = len(df)
    for col in df.columns:
        series = df[col]
        non_null = series.notna().sum()
        missing = n - non_null
        missing_pct = (missing / n) * 100 if n > 0 else np.nan

        # Unique values
        try:
            unique_count = series.nunique(dropna=True)
        except Exception:
            unique_count = np.nan
        unique_pct = (unique_count / n) * 100 if n > 0 else np.nan

        # Top value and frequency
        top_value = None
        top_freq = 0
        if non_null > 0:
            vc = series.value_counts(dropna=True)
            if not vc.empty:
                top_value = vc.index[0]
                top_freq = int(vc.iloc[0])

        # Sample distinct values
        try:
            samples = series.dropna().unique()[:sample_values].tolist()
        except Exception:
            samples = []

        # dtype and numeric/datetime stats
        dtype = str(series.dtype)
        min_val = max_val = mean = std = np.nan
        if pd.api.types.is_numeric_dtype(series):
            min_val = series.min(skipna=True)
            max_val = series.max(skipna=True)
            mean = series.mean(skipna=True)
            std = series.std(skipna=True)
        elif pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_datetime64_ns_dtype(series):
            min_val = series.min(skipna=True)
            max_val = series.max(skipna=True)

        rows.append({
            "column": col,
            "dtype": dtype,
            "non_null_count": int(non_null),
            "missing_count": int(missing),
            "missing_pct": round(missing_pct, 2),
            "unique_count": int(unique_count) if not pd.isna(unique_count) else np.nan,
            "unique_pct": round(unique_pct, 2) if not pd.isna(unique_pct) else np.nan,
            "top_value": top_value,
            "top_freq": int(top_freq),
            "sample_values": samples,
            "min": min_val,
            "max": max_val,
            "mean": mean,
            "std": std,
        })

    report = pd.DataFrame(rows).set_index("column")
    # Order columns for readability
    cols_order = [
        "dtype", "non_null_count", "missing_count", "missing_pct",
        "unique_count", "unique_pct", "top_value", "top_freq",
        "sample_values", "min", "max", "mean", "std"
    ]
    return report[cols_order]