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

"""Validates a batch of DICOMs against a provided CSV report.

This script compares a CSV report of expected DICOM metadata with the actual
metadata stored in a BigQuery table associated with a Healthcare API DICOM store.
It generates a new CSV report from BigQuery and compares it with the provided
report, highlighting any differences.

Usage: python validate_dicom_batch.py <GCS_URI_CSV> <BIGQUERY_TABLE_ID>

Example:
python validate_dicom_batch.py gs://your-bucket/path/20231220-report.csv your_project.your_dataset.your_table
"""

import argparse
from datetime import datetime
from io import StringIO

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound


def export_dicom_metadata_to_csv(
    date_str: str, bigquery_table_id: str, gcs_uri_csv: str
) -> bool:
    """
    Exports DICOM metadata to a CSV string, and uploads the CSV to GCS.

    Args:
        date_str: Date to filter StudyDate (format: YYYY-MM-DD).
        bigquery_table_id: BigQuery table ID in the format 'project.dataset.table'.
        gcs_uri_csv: CSV file name in bucket to upload (URI).

    Returns:
        bool: True if the export and upload were successful, False otherwise.
    """

    print(f"Generating report from BigQuery for StudyDate: {date_str}...")
    try:
        # Construct BigQuery client and query
        bq_client = bigquery.Client()
        query = f"""
            SELECT
                StudyDate,
                StudyInstanceUID,
                AccessionNumber,
                PatientID,
                COUNT(*) AS ObjectCount
            FROM
                `{bigquery_table_id}`
            WHERE
                StudyDate = '{date_str}'
            GROUP BY
                StudyDate,
                StudyInstanceUID,
                AccessionNumber,
                PatientID
            ORDER BY
                StudyDate,
                StudyInstanceUID
        """

        # Execute the query
        query_job = bq_client.query(query)
        results = query_job.result()

        # Write results to a CSV string
        csv_data = "studyinstanceuid,accessionnumber,patientid,objectcount,stddate\n"
        for row in results:
            csv_data += f"{row.StudyInstanceUID},{row.AccessionNumber if row.AccessionNumber else ''},{row.PatientID if row.PatientID else ''},{row.ObjectCount},{row.StudyDate}\n"

        # Determine bucket_name and blob_name
        parts = gcs_uri_csv.replace("gs://", "").split("/", 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""

        # Upload CSV data to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(csv_data)

        print(f"CSV file uploaded to gs://{bucket_name}/{blob_name}")
        return True

    except Exception as e:
        print(f"Error exporting DICOM metadata to CSV: {e}")
        return False


def download_csv_from_bucket(gcs_uri_csv: str) -> str:
    """
    Downloads a CSV file from a bucket, and returns the CSV as a string.

    Args:
        gcs_uri_csv: CSV file name in bucket to download (URI).

    Returns:
        str: The CSV content as a string, or an empty string if an error occurs.
    """
    print(f"Downloading CSV file from: {gcs_uri_csv}")
    try:
        # Determine bucket_name and blob_name
        parts = gcs_uri_csv.replace("gs://", "").split("/", 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""

        # Download CSV
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check if the blob exists
        if not blob.exists():
            raise NotFound(f"CSV file not found: {gcs_uri_csv}")

        csv_str = blob.download_as_text()

        return csv_str

    except NotFound as e:
        print(f"Error: {e}")
        return ""
    except Exception as e:
        print(f"Error downloading CSV from bucket: {e}")
        return ""


def compare_csv_reports(gcs_uri_csv1: str, gcs_uri_csv2: str) -> bool:
    """
    Compares two CSV files from a bucket and prints the differences,
    sorted by StudyInstanceUID and Source.

    Args:
        gcs_uri_csv1: First CSV file.
        gcs_uri_csv2: Second CSV file.

    Returns:
        bool: True if the reports are identical, False otherwise.
    """

    print(f"Comparing CSV reports...")
    try:
        # Download CSVs from GCS
        csv1_str = download_csv_from_bucket(gcs_uri_csv1)
        csv2_str = download_csv_from_bucket(gcs_uri_csv2)

        # If either download failed, return False
        if not csv1_str or not csv2_str:
            return False

        # Read CSVs into DataFrames
        df1 = pd.read_csv(StringIO(csv1_str))
        df2 = pd.read_csv(StringIO(csv2_str))

        # Add a 'Source' column to identify the origin of each row
        df1["Source"] = gcs_uri_csv1
        df2["Source"] = gcs_uri_csv2

        # Find differences based on all columns except 'Source'
        print(f"Finding differences in reports...")
        diff = pd.concat([df1, df2]).drop_duplicates(
            subset=df1.columns.difference(["Source"]), keep=False
        )

        if diff.empty:
            print("The CSV reports are identical.")
            return True
        else:
            print("Differences found in the CSV reports:")
            # Sort the differences by StudyInstanceUID and Source
            print(diff.sort_values(by=['studyinstanceuid', 'Source']).to_string())
            return False

    except Exception as e:
        print(f"Error comparing CSV reports: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate and compare DICOM metadata reports."
    )
    parser.add_argument(
        "gcs_uri_csv",
        help="GCS URI of the CSV file to compare (e.g., gs://your-bucket/path/20231220-report.csv)",
    )
    parser.add_argument(
        "bigquery_table_id",
        help="BigQuery table ID in the format 'project.dataset.table'",
    )

    args = parser.parse_args()

    # Check file extension
    if not args.gcs_uri_csv.endswith(".csv"):
        print("Error: GCS URI should end with .csv")
        exit(1)

    # Extract date from GCS URI
    try:
        date_str = args.gcs_uri_csv.split("/")[-1].split("-")[0]
        date_str = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        print("Incorrect GCS URI format. It should be like: gs://your-bucket/path/YYYYMMDD-report.csv")
        exit(1)

    # Generate report from BigQuery
    gcs_uri_csv2 = args.gcs_uri_csv.replace(".csv", "-hcapi.csv")
    if not export_dicom_metadata_to_csv(
        date_str, args.bigquery_table_id, gcs_uri_csv2
    ):
        print("Error generating report from BigQuery. Exiting.")
        exit(1)

    # Compare reports
    if not compare_csv_reports(args.gcs_uri_csv, gcs_uri_csv2):
        print("CSV reports have differences. Exiting.")
        exit(1)

    print("Validation completed.")
