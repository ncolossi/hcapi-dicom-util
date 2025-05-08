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

"""Monitors a GCS directory for DICOM import reports and triggers processing.

This script monitors a specified GCS directory for CSV reports (YYYMMDD-report.csv)
and triggers the DICOM import and validation pipeline for each new report. It ensures
that each report is processed only once by checking for the presence of corresponding
processing and result files.

Required environment variables:
- REPORT_GCS_URI: GCS URI of the directory containing the reports (e.g., gs://your-bucket/path/REPORT).
- DICOM_STORE_PATH: Path to the Healthcare API DICOM store.
- BIGQUERY_TABLE_ID: BigQuery table ID in 'project.dataset.table' format.
- STORAGE_CLASS: (Optional) Storage class for imported DICOMs (defaults to ARCHIVE).
- SKIP_VALIDATION: (Optional) If set to 'true', skips the validation step (defaults to 'false').
"""

import os
import re
import sys
import time
from google.cloud import storage
from unzip_batch import unzip_and_upload
from import_dicom_batch import import_dicom
from validate_dicom_batch import validate_dicom_batch


def run_batch_for_report(report_csv_gcs_uri: str, dicom_store_path: str, bigquery_table_id: str, storage_class: str, skip_validation: bool = False) -> bool:
    """Executes the DICOM batch processing and validation pipeline for a single report.

    Args:
        report_csv_gcs_uri: GCS URI of the CSV report for validation.
        dicom_store_path: Path to the Healthcare API DICOM store.
        bigquery_table_id: BigQuery table ID in 'project.dataset.table' format.
        storage_class: Storage class for imported DICOMs.
        skip_validation: If True, skips the validation step.

    Returns:
        bool: True if the processing and validation were successful, False otherwise.
    """

    start_time = time.time()
    print(f"Starting DICOM batch processing for report: {report_csv_gcs_uri}")

    try:
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

        # 1. Unzip DICOM batch
        print(f"Unzipping DICOM batch from: {dicom_gcs_uri}")
        if not unzip_and_upload(bucket, f"{path}/DICOM/{date_str}", debug_logs=False):
            raise ValueError("DICOM batch unzipping did not complete successfully. Cannot continue to import.")

        # 2. Import DICOM batch
        print(f"Importing DICOM batch from: {dicom_gcs_uri}")
        if not import_dicom(dicom_gcs_uri, dicom_store_path, storage_class):
            print("DICOM batch importing did not complete successfully. Proceeding to validation...")

        # 3. Validate DICOM batch (with retry) if not skipped
        if skip_validation:
            print("Skipping validation as requested by SKIP_VALIDATION flag.")
        else:
            print(f"Waiting 30 seconds for metadata sync before validation...")
            time.sleep(30)  # Wait for 30 seconds

            validation_attempts = 0
            max_attempts = 5  # One initial attempt + four retries
            sleep_time = 0
            while validation_attempts < max_attempts:
                print(f"Validating DICOM batch against: {report_csv_gcs_uri} (Attempt {validation_attempts + 1})")
                if validate_dicom_batch(report_csv_gcs_uri, bigquery_table_id):
                    print("DICOM batch validation passed.")
                    break  # Validation successful, exit the loop
                else:
                    validation_attempts += 1
                    if validation_attempts < max_attempts:
                        sleep_time += 120  # Wait for 2 more minutes before retrying
                        print(f"DICOM batch validation failed. Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time) 
                    else:
                        raise ValueError("DICOM batch validation did not pass after retry. Check logs for details.")

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        print(f"DICOM batch processing and validation completed in {elapsed_time} seconds.")

        return True

    except ValueError as e:
        print(f"Error: {e}")
        return False
    except Exception as e:
        print(f"An error occurred during batch processing: {e}")
        return False


def monitor_report() -> bool:
    """Monitors the report directory and triggers processing for new reports.

    Returns:
        bool: True if the monitoring process completed without errors, False otherwise.
    """

    print("Starting DICOM report monitoring...")

    try:
        # Read required environment variables
        report_gcs_uri = os.environ["REPORT_GCS_URI"]
        dicom_store_path = os.environ["DICOM_STORE_PATH"]
        bigquery_table_id = os.environ["BIGQUERY_TABLE_ID"]

        # Read optional environment variables
        storage_class = os.getenv("STORAGE_CLASS", "ARCHIVE")
        skip_validation = os.getenv("SKIP_VALIDATION", "false").lower() == "true"

        # Remove trailing slash from REPORT_GCS_URI if present
        report_gcs_uri = report_gcs_uri.rstrip("/")

        # Create a Cloud Storage client
        storage_client = storage.Client()

        # Get the bucket and prefix from the GCS URI
        try:
            bucket_name = report_gcs_uri.split("/")[2]
            prefix = "/".join(report_gcs_uri.split("/")[3:])
        except IndexError:
            print(f"Error: Invalid REPORT_GCS_URI format: {report_gcs_uri}")
            return False

        # Create a dictionary to cache blob file names
        blob_cache = {}

        # List all files in the report directory and populate the cache
        print(f"Listing files in: {report_gcs_uri}")
        try:
            blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
            for blob in blobs:
                file_name = blob.name.split("/")[-1]
                date_str = file_name.split("-")[0]
                if date_str not in blob_cache:
                    blob_cache[date_str] = []
                blob_cache[date_str].append(file_name)
        except Exception as e:
            print(f"Error listing files in GCS: {e}")
            return False

        # Iterate through the cache to process reports
        for date_str, files in blob_cache.items():
            # Define file names
            report_name = f"{date_str}-report.csv"
            processing_name = f"{date_str}-report-processing.txt"
            result_name = f"{date_str}-report-hcapi.csv"

            if report_name in files and (
                processing_name not in files
                and result_name not in files
            ):
                print(f"Processing report: {report_name}")

                # Double-check on GCS if not found in the cache (concurrent processing safe)
                processing_file = f"{prefix}/{processing_name}"
                result_file = f"{prefix}/{result_name}"
                try:
                    if (storage_client.bucket(bucket_name).blob(processing_file).exists() or 
                        storage_client.bucket(bucket_name).blob(result_file).exists()
                    ):
                        print(f"Skipping report {report_name} (already processed or in progress)")
                        continue
                except Exception as e:
                    print(f"Error checking for existing files on GCS: {e}")
                    continue

                # Create a processing file to mark the report as in progress
                print(f"Creating processing file: {processing_name}")
                try:
                    storage_client.bucket(bucket_name).blob(processing_file).upload_from_string("")
                except Exception as e:
                    print(f"Error creating processing file: {e}")
                    continue  # Skip to the next report

                # Invoke run_batch_for_report with the necessary parameters
                report_gcs_uri = f"gs://{bucket_name}/{prefix}/{report_name}"
                if not run_batch_for_report(report_gcs_uri, dicom_store_path, bigquery_table_id, storage_class, skip_validation):
                    print(f"Error processing report: {report_name}")
                    continue

                print(f"Successfully processed report: {report_name}")

                # Delete the processing file
                print(f"Deleting processing file: {processing_name}")
                try:
                    storage_client.bucket(bucket_name).blob(processing_file).delete()
                except Exception as e:
                    print(f"Error deleting processing file: {e}")
                    # Continue to the next report even if deletion fails

        print("Finished DICOM report monitoring!")
        return True  # Monitoring completed without errors

    except KeyError as e:
        print(f"Error: Missing required environment variable: {e}")
        return False
    except Exception as e:
        print(f"An error occurred during batch monitoring: {e}")
        return False


if __name__ == "__main__":
    if not monitor_report():
        exit(1)  # Exit with an error code if monitoring failed
