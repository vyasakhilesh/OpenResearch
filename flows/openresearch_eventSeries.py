from prefect import flow
from tasks.eventSeries import preprocessing_core_openresearch_eventSeries, create_core_openresearch_eventSeries, deduplicate_openresearch_eventSeries
import os

# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to ERROR if not specified

    
@flow(name="openresearch_eventSeries", description="OpenResearch event series tasks for preprocessing, deduplication, and creation of event series pages")
def openresearch_eventSeries(api_url: str,
                            username: str,
                            password: str,
                            core_all_details_path: str,
                            template_name: str = "Event series",
                            dry_run: bool = True):
    
    """
    # preprocessing open research event series pages using core data details
    preprocessing_core_openresearch_eventSeries(api_url, 
                                                username,
                                                password,
                                                core_all_details_path,
                                                template_name=template_name,
                                                dry_run=dry_run,
    )
    
    # Create open research event series pages using core data details if does not exist
    create_core_openresearch_eventSeries(api_url,
                                    username,
                                    password,
                                    core_all_details_path,
                                    template_name=template_name,
                                    dry_run=dry_run,
    )
    """
    
    # deduplication of open research event series pages
    deduplicate_openresearch_eventSeries(api_url,
                                        username,
                                        password,
                                        core_all_details_path,
                                        template_name=template_name,
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
    openresearch_eventSeries(API,
                             USER,
                             PASS,
                             core_all_details_path,
                             "Event series",
                             False,
                             )
    # create_openresearch_events_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))