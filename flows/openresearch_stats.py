from prefect import flow
import os
from tasks.event import create_stats_openresearch_events
from tasks.eventSeries import create_stats_openresearch_eventSeries

#PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to ERROR if not specified

    
@flow(name="openresearch_stats", description="OpenResearch statistics tasks for event, event series etc.")
def openresearch_stats(api_url: str,
                            username: str,
                            password: str,
                            core_all_details_path: str,
                            template_name: str = "Stand-alone event",
                            llm_api_key: str = None,
                            dry_run: bool = True):
    
    
    # collecting statistics of open research event pages in a dataframe
    """
    create_stats_openresearch_events(api_url,
                                      username,
                                      password,
                                      core_all_details_path,
                                      template_name,
                                      llm_api_key=llm_api_key,
                                      dry_run=dry_run)
    """
    # collecting statistics of open research event series pages in a dataframe
    create_stats_openresearch_eventSeries(api_url,
                                          username,
                                          password,
                                          core_all_details_path,
                                          template_name="Event series",
                                          llm_api_key=llm_api_key,
                                          dry_run=dry_run)
    return True

if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    import numpy as np
    core_all_details_path = os.environ.get("CORE_ALL_DETAILS_PATH", "CORE_all_details.csv")

    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    openresearch_stats( API,
                        USER,
                        PASS,
                        core_all_details_path,
                        template_name="Stand-alone event",
                        llm_api_key='', #os.environ.get("OPENROUTER_API_KEY"),
                        dry_run=False,
                        )
    
    # create_openresearch_events_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))