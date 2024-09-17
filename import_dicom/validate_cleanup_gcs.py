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

"""Validates DICOM reports against a BigQuery summary table.

This script retrieves all '-report.csv' files from a GCS URI,
queries a BigQuery summary table for matching StudyDates and StudyInstanceUIDs,
and compares object counts to validate report consistency.

Usage: python validate_dicom_reports.py <REPORT_GCS_URI> <HCAPI_BIGQUERY_TABLE_ID> <LEGACY_BIGQUERY_TABLE_ID> [--cleanup]

Example:
python validate_dicom_reports.py gs://your-bucket/reports your_project.your_dataset.hcapi_table your_project.your_dataset.legacy_table --cleanup
"""

import argparse
import re
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd

def cleanup_gcs_objects(bucket_name, dicom_prefix):
    """Cleans up GCS objects related to a DICOM date.

    Args:
        bucket_name: The name of the GCS bucket.
        dicom_prefix: The prefix of the report in GCS (e.g., "path/to/DICOM/20231225").
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        print(f"  - Cleaning up GCS objects with prefix: {dicom_prefix}")
        blobs = bucket.list_blobs(prefix=dicom_prefix)
        for blob in blobs:
            # print(f"    - Deleting: {blob.name}")
            blob.delete()

    except Exception as e:
        print(f"  - Error cleaning up GCS objects: {e}")

def validate_dicom_reports(report_gcs_uri, hcapi_bigquery_table_id, legacy_bigquery_table_id, cleanup_gcs):
    """Validates DICOM reports against a BigQuery summary table.

    Args:
        report_gcs_uri: GCS URI of the directory containing the reports.
        hcapi_bigquery_table_id: BigQuery table ID for HCApI data.
        legacy_bigquery_table_id: BigQuery table ID for legacy data.
        cleanup_gcs: Boolean flag to enable or disable GCS cleanup.
    """

    try:
        # Create GCS and BigQuery clients
        storage_client = storage.Client()
        bq_client = bigquery.Client()

        # Extract bucket name and prefix from GCS URI
        match = re.match(r"gs://([^/]+)/(.*)", report_gcs_uri)
        if not match:
            raise ValueError(f"Invalid REPORT_GCS_URI format: {report_gcs_uri}")
        bucket_name = match.group(1)
        prefix = match.group(2)

        # Get all report file names from GCS
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        report_files = [blob.name for blob in blobs if blob.name.endswith('-report.csv')]

        # Query BigQuery for StudyDates and object counts (grouped by StudyDate)
        query = f"""
        SELECT
            FORMAT_DATE('%Y%m%d', legacy.StudyDate) AS StudyDate,
            SUM(legacy.ObjectCount) AS LegacyObjectCount,
            SUM(hcapi.ObjectCount) AS HcapiObjectCount
        FROM
            (SELECT StudyDate, StudyInstanceUID, SUM(ObjectCount) AS ObjectCount 
             FROM `{legacy_bigquery_table_id}` 
             GROUP BY StudyDate, StudyInstanceUID) AS legacy
            JOIN (SELECT StudyDate, StudyInstanceUID, SUM(ObjectCount) AS ObjectCount 
                  FROM `{hcapi_bigquery_table_id}` 
                  GROUP BY StudyDate, StudyInstanceUID) AS hcapi
        ON
        legacy.StudyDate = hcapi.StudyDate
        AND legacy.StudyInstanceUID = hcapi.StudyInstanceUID
        GROUP BY
            1
        """
        query_job = bq_client.query(query)
        bq_results = query_job.result()

        # Create a dictionary to store BigQuery results for faster lookup
        bq_data = {row.StudyDate: (row.LegacyObjectCount, row.HcapiObjectCount) 
                   for row in bq_results}

        # Iterate through report files and validate
        for report_file in report_files:
            report_date = re.search(r"(\d{8})-report\.csv$", report_file)
            if report_date:
                report_date = report_date.group(1)
                print(f"Validating report: {report_file} (StudyDate: {report_date})")

                # Read object count from the report CSV
                try:
                    report_blob = bucket.blob(report_file)
                    report_df = pd.read_csv(report_blob.open("r"))
                    report_object_count = report_df['objectcount'].sum()
                except KeyError as e:
                    print(f"  - Error reading report: {report_file}. Missing column: {e}")
                    continue  # Skip to the next report
                except Exception as e:
                    print(f"  - Error reading report: {report_file}. Error: {e}")
                    continue  # Skip to the next report

                # Check if report date exists in BigQuery data
                if report_date in bq_data:
                    bq_legacy_count, bq_hcapi_count = bq_data[report_date]
                    if report_object_count == bq_legacy_count and report_object_count == bq_hcapi_count:
                        print(f"  - Report validated OK. Object count matches: {report_object_count}")

                        # Remove processing.txt file if it exists
                        processing_file = report_file.replace('-report.csv', '-processing.txt')
                        try:
                            bucket.delete_blob(processing_file)
                            print(f"    - Removed: {processing_file}")
                        except Exception as e:
                            pass

                        # Perform GCS cleanup only if cleanup_gcs is True
                        if cleanup_gcs:
                            dicom_prefix = f"{prefix}/{report_date}".replace("REPORT", "DICOM", 1)
                            cleanup_gcs_objects(bucket_name, dicom_prefix)
                    else:
                        print(f"  - Report validation INCONSISTENT! Report images: {report_object_count}, HCAPI images: {bq_hcapi_count}, Legacy images: {bq_legacy_count}")
                else:
                    print(f"  - Report date {report_date} not found in BigQuery. Removing associated files for reprocessing...")
                    # Remove -report-hcapi.csv and -processing.txt files if they exist
                    hcapi_file = report_file.replace('-report.csv', '-report-hcapi.csv')
                    try:
                        bucket.delete_blob(hcapi_file)
                        print(f"    - Removed: {hcapi_file}")
                    except Exception as e:
                        pass
                    processing_file = report_file.replace('-report.csv', '-processing.txt')
                    try:
                        bucket.delete_blob(processing_file)
                        print(f"    - Removed: {processing_file}")
                    except Exception as e:
                        pass
            else:
                print(f"  - Skipping invalid report file name: {report_file}")

        return True

    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate DICOM reports against a BigQuery summary table, and optionally cleanup GCS DICOM objects."
    )
    parser.add_argument("report_gcs_uri", help="GCS URI of the directory containing the reports.")
    parser.add_argument("hcapi_bigquery_table_id", help="BigQuery table ID for HCAPI data.")
    parser.add_argument("legacy_bigquery_table_id", help="BigQuery table ID for legacy data.")
    parser.add_argument("--cleanup", action="store_true", help="Enable GCS object cleanup (use with caution!)")

    args = parser.parse_args()

    validate_dicom_reports(args.report_gcs_uri, args.hcapi_bigquery_table_id, args.legacy_bigquery_table_id, args.cleanup)
