# flows/ex_flow.py
from prefect import flow
from tasks.ex_task import extract_data

@flow(name="example-pipeline")
def my_pipeline():
    data = extract_data()
    print(f"Extracted {len(data)} rows")
    return len(data)

if __name__ == "__main__":
    my_pipeline()