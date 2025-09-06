import os

# Set environment variables for this run
os.environ['CONNECTOR_NAME'] = 'eurostat'
os.environ['RUN_ID'] = 'local-dev'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CACHE_REQUESTS'] = 'true'
os.environ['WRITE_SNAPSHOT'] = 'false'
os.environ['STORAGE_BACKEND'] = 'local'
os.environ['DATA_DIR'] = 'data'

from utils import validate_environment, upload_data
from assets.datasets.datasets import process_dataset

# Test with the specific dataset
dataset_code = 'ei_bpm6ca_q'
print(f"Testing with dataset: {dataset_code}")

validate_environment()

# Process the dataset
dataset_data = process_dataset(dataset_code)

# Show info about the data
print(f"\nDataset shape: {dataset_data.num_rows} rows x {dataset_data.num_columns} columns")
print(f"Columns: {dataset_data.column_names}")

# Show first few rows as pylist
print("\nFirst 5 rows:")
first_5 = dataset_data.slice(0, 5).to_pylist()
for row in first_5:
    print(row)

# Upload the data
upload_data(dataset_data, dataset_code)

print(f"\nSuccessfully processed and uploaded dataset {dataset_code}")