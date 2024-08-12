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

"""Exports DICOM metadata to BigQuery and configures streaming.

This script performs the following steps:
1. Validates the provided Healthcare API DICOM store path.
2. Validates the existence of the BigQuery dataset.
3. Exports DICOM metadata to a BigQuery table with a temporary suffix.
4. Copies the temporary table to a new table with partitioning.
5. Configures streaming from the DICOM store to the partitioned table.

Usage: python export_dicom_metadata_to_bq.py <dicom_store_path> <dataset_id>

Example:
python export_dicom_metadata_to_bq.py \
"projects/your-project-id/locations/your-location/datasets/your-dataset-id/dicomStores/your-dicom-store-id" \
"your_dataset_id"
"""

import argparse
import time

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from googleapiclient import discovery

def export_dicom_metadata_to_bq(dicom_store_path: str, dataset_id: str) -> None:
    """Exports DICOM metadata to BigQuery and configures streaming.

    Args:
        dicom_store_path: Path to the DICOM store.
        dataset_id: ID of the BigQuery dataset.
    """

    # Create API clients
    dicom_client = discovery.build('healthcare', 'v1')
    bq_client = bigquery.Client()

    try:
        # 1. Validate DICOM store path
        print(f"Validating DICOM store: {dicom_store_path}")
        dicom_store = dicom_client.projects().locations().datasets().dicomStores().get(name=dicom_store_path).execute()
        print(f"DICOM store validated: {dicom_store['name']}")

        # 2. Validate BigQuery dataset
        print(f"Validating BigQuery dataset: {dataset_id}")
        bq_client.get_dataset(dataset_id)  # Raises NotFound if dataset doesn't exist
        print(f"BigQuery dataset validated: {dataset_id}")

        # 3. Export metadata to BigQuery temporary table
        dicom_store_id = dicom_store_path.split("/")[-1]
        table_id_temp = f"{dicom_store_id}_temp".replace(".", "_")
        table_ref_temp = bq_client.dataset(dataset_id).table(table_id_temp)
        print(f"Exporting metadata to temporary table: {table_ref_temp}")

        response = dicom_client.projects().locations().datasets().dicomStores().export(
            name=dicom_store_path,
            body={
                "bigqueryDestination": {
                    "tableUri": f"bq://{table_ref_temp}",
                    "writeDisposition": "WRITE_TRUNCATE"
                }
            }
        ).execute()

        # Wait for the export operation to complete
        while True:
            operation = dicom_client.projects().locations().datasets().operations().get(
                name=response['name']
            ).execute()
            if 'done' in operation and operation['done']:
                if 'error' in operation:
                    raise Exception(f"Export operation failed: {operation['error']}")
                break
            time.sleep(5)

        print(f"Metadata exported to temporary table: {table_ref_temp}")


        # 4. Copy to partitioned table
        table_id = dicom_store_id.replace(".", "_")
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        print(f"Creating partitioned table: {table_ref}")

        schema = bq_client.get_table(table_ref_temp).schema
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="StudyDate"
        )
        bq_client.create_table(table, True)

        # Copy data from temporary table to partitioned table
        job_config = bigquery.QueryJobConfig(
            destination=table_ref, write_disposition="WRITE_TRUNCATE"
        )
        query = f"SELECT * FROM `{table_ref_temp}`"
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()  # Wait for the query to complete

        print(f"Data copied to partitioned table: {table_ref}")

        # Drop the temporary table
        bq_client.delete_table(table_ref_temp)  
        print(f"Temporary table {table_ref_temp} deleted.")

        # 5. Configure streaming
        print(f"Configuring streaming to: {table_ref}")

        dicom_store['streamConfigs'] = [{
            'bigqueryDestination': {
                'tableUri': f"bq://{table_ref}"
            }
        }]
        dicom_client.projects().locations().datasets().dicomStores().patch(
            name=dicom_store_path,
            updateMask='streamConfigs',
            body=dicom_store
        ).execute()

        print(f"Streaming configured successfully.")

    except NotFound as e:
        print(f"Error: Resource not found - {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export DICOM metadata to BigQuery and configure streaming."
    )
    parser.add_argument("dicom_store_path", help="Path to the DICOM store.")
    parser.add_argument("dataset_id", help="ID of the BigQuery dataset.")

    args = parser.parse_args()

    export_dicom_metadata_to_bq(args.dicom_store_path, args.dataset_id)
