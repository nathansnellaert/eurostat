import os
os.environ['CONNECTOR_NAME'] = 'eurostat'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

import subprocess
import sys
from pathlib import Path
from datetime import datetime
from utils import validate_environment, upload_data, load_state, save_state
from assets.catalogue.catalogue import process_catalogue

def process_dataset_subprocess(dataset_code: str) -> bool:
    """
    Process a dataset in a subprocess to avoid memory issues.
    
    Returns:
        True if successful, False otherwise
    """
    cmd = [
        sys.executable,
        "process_dataset.py",
        dataset_code
    ]
    
    try:
        # Run in subprocess with memory constraints
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=1200  # 10 minute timeout per dataset
        )
        
        # Print subprocess output
        if result.stdout:
            print(result.stdout.strip())
        if result.stderr and result.returncode != 0:
            print(f"Error: {result.stderr.strip()}", file=sys.stderr)
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout processing {dataset_code}")
        return False
    except Exception as e:
        print(f"✗ Error processing {dataset_code}: {e}")
        return False

def main():
    validate_environment()
    
    # Process catalogue to get list of all datasets
    print("Fetching Eurostat catalogue...")
    catalogue_data = process_catalogue()
    upload_data(catalogue_data, "eurostat_catalogue")
    save_state("catalogue", {
        "last_updated": datetime.now().isoformat(),
        "dataset_count": len(catalogue_data)
    })
    
    # Get all dataset codes
    dataset_codes = catalogue_data['code'].to_pylist()
    print(f"Found {len(dataset_codes)} datasets in catalogue")
    
    # Check state for each dataset
    datasets_to_process = []
    up_to_date_datasets = []
    
    for dataset_code in dataset_codes:
        state = load_state(dataset_code)
        if state and 'last_updated' in state:
            last_updated = datetime.fromisoformat(state['last_updated'])
            days_ago = (datetime.now() - last_updated).days
            if days_ago < 30:
                up_to_date_datasets.append((dataset_code, days_ago))
                continue
        
        datasets_to_process.append(dataset_code)
    
    # Print summary
    print(f"\n📊 Dataset Status Summary:")
    print(f"  ✓ Up-to-date (< 30 days): {len(up_to_date_datasets)}")
    print(f"  ⏳ To process: {len(datasets_to_process)}")
    
    if not datasets_to_process:
        print("\n✅ All datasets are up to date!")
        return
    
    print(f"\n🚀 Processing {len(datasets_to_process)} datasets...")
    
    # Process each dataset in a subprocess
    successful = []
    failed = []
    
    for i, dataset_code in enumerate(datasets_to_process, 1):
        print(f"\n[{i}/{len(datasets_to_process)}] Processing {dataset_code}...")
        
        success = process_dataset_subprocess(dataset_code)
        
        if success:
            successful.append(dataset_code)
        else:
            failed.append(dataset_code)
    
    # Print final summary
    print("\n" + "="*50)
    print("📊 Eurostat Connector Summary")
    print("="*50)
    
    if successful:
        print(f"\n✅ Successfully processed {len(successful)} datasets")
    
    if failed:
        print(f"\n❌ Failed to process {len(failed)} datasets:")
        for code in failed[:20]:
            print(f"  - {code}")
        if len(failed) > 20:
            print(f"  ... and {len(failed) - 20} more")
    
    if up_to_date_datasets:
        print(f"\n⏭️ Skipped {len(up_to_date_datasets)} up-to-date datasets")
    
    print("\n✨ Eurostat connector run complete!")

if __name__ == "__main__":
    main()