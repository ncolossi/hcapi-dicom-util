# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs a batch processing and validation pipeline for DICOM files.

This script orchestrates the following steps:
1. Reads configuration parameters from environment variables.
2. Invokes the process_dicom_batch function to unzip and import DICOM files.
3. Invokes the validate_dicom_batch function to compare the imported data
   against a provided CSV report.

Ensure the following environment variables are set:
- REPORT_CSV_GCS_URI: GCS URI of the CSV report for validation (format: gs://bucket/path/REPORT/YYYMMDD-report.csv).
- DICOM_STORE_PATH: Path to the Healthcare API DICOM store.
- BIGQUERY_TABLE_ID: BigQuery table ID in 'project.dataset.table' format.
- STORAGE_CLASS: (Optional) Storage class for imported DICOMs (defaults to ARCHIVE).

Example usage:
export REPORT_CSV_GCS_URI="gs://your-bucket/path/REPORT/20231220-report.csv"
export DICOM_STORE_PATH="projects/.../dicomStores/..."
export BIGQUERY_TABLE_ID="your-project.your_dataset.your_table"
export STORAGE_CLASS="ARCHIVE"  # Optional
python run_batch.py
"""

import os
import re
import time
from process_dicom_batch import process_dicom_batch
from validate_dicom_batch import validate_dicom_batch


def run_batch_pipeline():
    """Executes the DICOM batch processing and validation pipeline."""

    start_time = time.time()
    print("Starting DICOM batch processing pipeline...")

    try:
        # Read required parameters from environment variables
        report_csv_gcs_uri = os.environ["REPORT_CSV_GCS_URI"]
        dicom_store_path = os.environ["DICOM_STORE_PATH"]
        bigquery_table_id = os.environ["BIGQUERY_TABLE_ID"]

        # Read optional STORAGE_CLASS, defaulting to ARCHIVE
        storage_class = os.getenv("STORAGE_CLASS", "ARCHIVE")

        # Validate REPORT_CSV_GCS_URI format
        if not re.match(r"gs://([^/]+)/(.+)/REPORT/\d{8}-report\.csv$", report_csv_gcs_uri):
            raise ValueError(
                "Invalid REPORT_CSV_GCS_URI format. It should be like: gs://bucket/path/REPORT/YYYMMDD-report.csv"
            )

        # Extract bucket, path, and date from REPORT_CSV_GCS_URI
        match = re.match(r"gs://([^/]+)/(.*)/REPORT/(\d{8})-report\.csv$", report_csv_gcs_uri)
        bucket = match.group(1)
        path = match.group(2)
        date_str = match.group(3)

        # Construct GCS URI for DICOM files
        dicom_gcs_uri = f"gs://{bucket}/{path}/DICOM/{date_str}"

        # 1. Process DICOM batch
        print(f"Processing DICOM batch from: {dicom_gcs_uri}")
        if not process_dicom_batch(dicom_gcs_uri, dicom_store_path, storage_class):
            print("DICOM batch processing did not complete successfully. Proceeding to validation...")

        # 2. Validate DICOM batch
        print(f"Validating DICOM batch against: {report_csv_gcs_uri}")
        if not validate_dicom_batch(report_csv_gcs_uri, bigquery_table_id):
            raise ValueError("DICOM batch validation did not pass. Check logs for details.")

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        print(
            f"DICOM batch processing and validation completed in {elapsed_time} seconds."
        )

    except KeyError as e:
        print(f"Error: Missing required environment variable: {e}")
        return False
    except ValueError as e:
        print(f"Error: {e}")
        return False
    except Exception as e:
        print(f"An error occurred during batch processing: {e}")
        return False

    return True


if __name__ == "__main__":
    if not run_batch_pipeline():
        exit(1)  # Exit with an error code
