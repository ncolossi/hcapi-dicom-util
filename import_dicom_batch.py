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

"""Import a GCS folder with DICOM into Healhcare API DICOM store.

It expects DICOM files to have a .dcm extension.

Usage: python import_dicom.py <GCS_FOLDER> [<STORAGE_CLASS>]
STORAGE_CLASS options: STANDARD, NEARLINE, COLDLINE, and ARCHIVE
"""

import argparse
from googleapiclient import discovery
from googleapiclient.errors import HttpError
import time

def import_dicom(gcs_folder, dicom_store_path, storage_class="ARCHIVE"):
    """Imports DICOM files from a GCS folder to a DICOM store.

    Args:
        gcs_folder: The GCS folder containing the DICOM files.
        dicom_store_path: Path to the DICOM store (e.g., projects/your-project-id/locations/your-location/datasets/your-dataset-id/dicomStores/your-dicom-store-id)
        storage_class: The storage class to use for the imported files.
    """

    # Check if GCS_FOLDER starts with gs://
    if not gcs_folder.startswith("gs://"):
        raise ValueError("GCS_FOLDER should start with gs://")
    if gcs_folder.endswith("/"):
        gcs_folder = gcs_folder[:-1]

    # Build the request body
    request_body = {
        "gcsSource": {
            "uri": f"{gcs_folder}/**.dcm"
        },
        "blobStorageSettings": {
            "blobStorageClass": storage_class
        }
    }

    # Create a Healthcare API client
    healthcare = discovery.build('healthcare', 'v1beta1')

    try:
        start_time = time.time()  # Start the timer
        # Execute the import request
        request = healthcare.projects().locations().datasets().dicomStores().import_(
            name=dicom_store_path, body=request_body
        )
        response = request.execute()

        print(f"Import request submitted successfully: {response['name']}")
        print(f"Waiting for completion...")

        # Poll for operation completion
        operation_name = response['name']
        while True:
            time.sleep(5)  # Wait for 5 seconds
            operation = healthcare.projects().locations().datasets().operations().get(
                name=operation_name
            ).execute()

            if 'done' in operation and operation['done']:
                if 'error' in operation:
                    print(f"Import operation failed with error: {operation['error']}")
                    return False
                else:
                    print("Import operation completed successfully.")
                break
        # Report elapsed time
        end_time = time.time()  # Stop the timer
        elapsed_time = round(end_time - start_time, 2)  # Calculate elapsed time in seconds
        print(f"Imported DICOM files: {gcs_folder} in {elapsed_time} seconds")


    except HttpError as err:
        print(f"Error importing DICOM instances: {err}")
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import DICOM files from GCS to a DICOM store.")
    parser.add_argument("gcs_folder", help="GCS folder containing DICOM files (e.g., gs://your-bucket/dicom-files/)")
    parser.add_argument("dicom_store_path", help="Path to the DICOM store (e.g., projects/your-project-id/locations/your-location/datasets/your-dataset-id/dicomStores/your-dicom-store-id)")
    parser.add_argument("storage_class", nargs='?', default="ARCHIVE", 
                        help="Storage class (STANDARD, NEARLINE, COLDLINE, ARCHIVE), defaults to ARCHIVE")

    args = parser.parse_args()

    import_dicom(args.gcs_folder, args.dicom_store_path, args.storage_class)
