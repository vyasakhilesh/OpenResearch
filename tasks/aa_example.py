# tasks/extract.py
from prefect.logging import get_run_logger
from prefect import task

@task
def extract_data():
    """
    Example pure function that can be executed locally or inside a Prefect task.
    Replace with real extraction logic (DB query, API call, file read).
    """
    logger = get_run_logger()
    logger.info("Extracting data...")

    sample = [{"id": i, "value": i * 2} for i in range(10)]
    return sample

if __name__ == "__main__":
    logger = get_run_logger()
    logger.info("Running extract_data() directly...")
    extract_data()