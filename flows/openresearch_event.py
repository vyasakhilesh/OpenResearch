from prefect import flow
import os
from tasks.event import preprocessing_openresearch_events

#PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to ERROR if not specified

    
@flow(name="openresearch_event", description="OpenResearch event tasks for preprocessing, deduplication, and creation of event pages")
def openresearch_event(api_url: str,
                            username: str,
                            password: str,
                            core_all_details_path: str,
                            template_name: str = "Stand-alone event",
                            llm_api_key: str = None,
                            dry_run: bool = True):
    
    
    # preprocessing open research event pages using core data details
    preprocessing_openresearch_events(api_url,
                                      username,
                                      password,
                                      core_all_details_path,
                                      template_name,
                                      llm_api_key=llm_api_key,
                                      dry_run=dry_run,
)
    """
    # Create open research event pages using core data details if does not exist
    create_core_openresearch_event(api_url,
                                    username,
                                    password,
                                    core_all_details_path,
                                    template_name=template_name,
                                    dry_run=dry_run,
    )"""
    
    
    # deduplication of open research event pages
    """
    deduplicate_openresearch_event(api_url,
                                        username,
                                        password,
                                        core_all_details_path,
                                        template_name=template_name,
                                        dry_run=dry_run,
    )"""
                
    return True

if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    import numpy as np
    core_all_details_path = os.environ.get("CORE_ALL_DETAILS_PATH", "CORE_all_details.csv")

    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    openresearch_event( API,
                        USER,
                        PASS,
                        core_all_details_path,
                        template_name="Stand-alone event",
                        llm_api_key='', #os.environ.get("OPENROUTER_API_KEY"),
                        dry_run=False,
                        )
    
    # create_openresearch_events_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))