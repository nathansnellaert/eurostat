#!/usr/bin/env python3
"""
Process a single Eurostat dataset by code.
Designed to be called as a subprocess to avoid memory issues.
"""
import os
import sys
import argparse
from datetime import datetime
from utils import upload_data, save_state, validate_environment
from assets.datasets.datasets import process_dataset

def main():
    parser = argparse.ArgumentParser(description='Process a single Eurostat dataset')
    parser.add_argument('dataset_code', type=str, help='Eurostat dataset code (e.g., ei_bpm6ca_m)')
    
    args = parser.parse_args()
    
    # Set up environment
    os.environ['CONNECTOR_NAME'] = 'eurostat'
    if not os.environ.get('RUN_ID'):
        os.environ['RUN_ID'] = f'dataset-{args.dataset_code}-{datetime.now().strftime("%Y%m%d%H%M%S")}'
    
    validate_environment()
    
    # Process the dataset
    print(f"Processing {args.dataset_code}...")
    data = process_dataset(args.dataset_code)
    
    if data.num_rows == 0:
        print(f"No data found for {args.dataset_code}")
        sys.exit(0)
    
    # Upload the data
    print(f"Uploading {data.num_rows:,} rows...")
    upload_data(data, f"eurostat_{args.dataset_code}")
    
    # Save state
    save_state(args.dataset_code, {
        "last_updated": datetime.now().isoformat(),
        "row_count": data.num_rows
    })
    
    print(f"✓ Successfully processed {args.dataset_code}: {data.num_rows:,} rows")

if __name__ == "__main__":
    main()