from prefect import flow, get_run_logger
from tasks.core import create_icore_conference_details_data
import os
import pandas as pd
import numpy as np

# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")

    
@flow(name="create-core-data", description="Create core data from core pages")
def create_core_data(years: list = ["2026"]):
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    for year in years:
        if year == "2026":
            source = "Source: ICORE2026"
        elif year == "2010":
            source = "Source: ERA2010"
        else:
            source = f"Source: CORE{year}"
            
        core_path = os.environ.get(f"CORE_{year}_PATH", f"CORE_{year}.csv")
        details_path = core_path.replace('.csv', '_details.csv')
        rank = f"has CORE{year} Rank"
    
        if not os.path.exists(details_path):
            df_core_details = create_icore_conference_details_data(core_path, source=source, rank=rank)
            logger.info(f"Created core data for year {year} with {len(df_core_details)} unique entries.")
            
    # merge all details dataframes into a single dataframe
    keys = ["Title", "Acronym"]

    def prep(year):
        df= pd.read_csv(f"./data/coreranking/CORE_{year}_details.csv")
        df.replace(np.nan, '', regex=True, inplace=True)  # Replace NaN with empty string
        # Treat empty strings as missing so they can be filled from lower-priority dfs
        out = df.copy()
        out = out.replace(r"^\s*$", np.nan, regex=True)
        # If key duplicates exist, keep first (or change logic if needed)
        out = out.drop_duplicates(subset=keys, keep="first")
        return out.set_index(keys)

    d2008 = prep(2008)
    d2010 = prep(2010)
    d2013 = prep(2013)
    d2014 = prep(2014)
    d2017 = prep(2017)
    d2018 = prep(2018)
    d2020 = prep(2020)
    d2021 = prep(2021)
    d2023 = prep(2023)
    d2026 = prep(2026)
        
    # Priority fill: 2026 -> 2023 -> 2021 -> 2020 -> 2018 -> 2017 -> 2014 -> 2013
    # merged = d2026.combine_first(d2014).combine_first(d2013).reset_index()
    merged = d2026.combine_first(d2023)\
                  .combine_first(d2021)\
                  .combine_first(d2020)\
                  .combine_first(d2018)\
                  .combine_first(d2017)\
                  .combine_first(d2014)\
                  .combine_first(d2013)\
                  .combine_first(d2010)\
                  .combine_first(d2008)\
                  .reset_index()
    # Optional: convert NaN back to empty strings
    merged = merged.fillna("")
    merged.to_csv("./data/coreranking/CORE_all_details.csv", index=False)
    logger.info(f"Merged core data saved to ./data/coreranking/CORE_all_details.csv with {len(merged)} unique entries.")
    return True
    
if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    import numpy as np
    
    years = ["2008", "2010", "2013", "2014", "2017", "2018", "2020", "2021", "2023", "2026"]
    create_core_data(years=years)