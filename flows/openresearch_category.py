from prefect import flow
import os
from tasks.category import preprocessing_openresearch_categories

#PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to ERROR if not specified

    
@flow(name="openresearch_category", description="OpenResearch category tasks for preprocessing, deduplication, and creation of category pages")
def openresearch_category(api_url: str,
                            username: str,
                            password: str,
                            core_all_details_path: str,
                            template_name: str = "category",
                            llm_api_key: str = None,
                            dry_run: bool = True):
    
    
    # preprocessing open research category pages using core data details
    preprocessing_openresearch_categories(api_url,
                                      username,
                                      password,
                                      core_all_details_path,
                                      template_name,
                                      llm_api_key=llm_api_key,
                                      dry_run=dry_run,
)
    
    return True

if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    import numpy as np
    core_all_details_path = os.environ.get("CORE_ALL_DETAILS_PATH", "CORE_all_details.csv")

    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    openresearch_category( API,
                        USER,
                        PASS,
                        core_all_details_path,
                        template_name="category",
                        llm_api_key='', #os.environ.get("OPENROUTER_API_KEY"),
                        dry_run=False,
                        )
    
    # create_openresearch_categories_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))