# flows/ex_flow.py
from prefect import flow
from tasks.aa_ex_task import extract_data
from prefect.logging import get_run_logger

@flow(name="example-pipeline")
def my_pipeline():
    data = extract_data()
    logger = get_run_logger()
    logger.info(f"Extracted {len(data)} rows")
    return len(data)

if __name__ == "__main__":
    my_pipeline.serve(name="example-pipeline-deployment")