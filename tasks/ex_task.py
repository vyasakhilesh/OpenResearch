# tasks/extract.py
def extract_data():
    """
    Example pure function that can be executed locally or inside a Prefect task.
    Replace with real extraction logic (DB query, API call, file read).
    """
    sample = [{"id": i, "value": i * 2} for i in range(10)]
    return sample

if __name__ == "__main__":
    print(extract_data())