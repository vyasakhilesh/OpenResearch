from time import time
from prefect import flow, get_run_logger
from tasks.core import create_icore_conference_details_data
import os
import pandas as pd

# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")

    
@flow(name="create-core-data", description="Create core data from core pages")
def create_core_data(year: str = "2026", source: str = "Source: ICORE2026", rank: str = "has CORE2026 Rank"
):
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    core_path = os.environ.get(f"CORE_{year}_PATH", f"CORE_{year}.csv")
    details_path = core_path.replace('.csv', '_details.csv')
    if not os.path.exists(details_path):
        df_core_details = create_icore_conference_details_data(core_path, source=source, rank=rank)
    return df_core_details
    
if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    import numpy as np
    
    years = ["2013", "2014", "2017", "2018", "2020", "2021", "2023", "2026"]
    
    for year in years:
        if year == "2026":
            source = "Source: ICORE2026"
        else:
            source = f"Source: CORE{year}"
        df_core_details = create_core_data(year=year, source=source, rank=f"has CORE{year} Rank")
        logger = get_run_logger()
        logger.setLevel(PREFECT_LOGGING_LEVEL)
        logger.info(f"Created core data for year {year} with {len(df_core_details)} unique entries.")
    