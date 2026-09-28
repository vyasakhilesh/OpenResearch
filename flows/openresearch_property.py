from prefect import flow
import os
from tasks.property import preprocessing_openresearch_properties

#PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to ERROR if not specified

    
@flow(name="openresearch_property", description="OpenResearch property tasks for preprocessing, deduplication, and creation of property pages")
def openresearch_property(api_url: str,
                            username: str,
                            password: str,
                            core_all_details_path: str,
                            template_name: str = "property",
                            llm_api_key: str = None,
                            dry_run: bool = True):
    
    
    # preprocessing open research property pages using core data details
    preprocessing_openresearch_properties(api_url,
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
    openresearch_property( API,
                        USER,
                        PASS,
                        core_all_details_path,
                        template_name="property",
                        llm_api_key='', #os.environ.get("OPENROUTER_API_KEY"),
                        dry_run=False,
                        )
    
    # create_openresearch_properties_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))