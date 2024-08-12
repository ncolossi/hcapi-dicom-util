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

"""Processes a GCS URI containing zip files of DICOMs.

This script unzips files in the given GCS URI and then imports them
into a Healthcare API DICOM store.

Usage: python process_dicom_batch.py <GCS_URI> <DICOM_STORE_PATH> [<STORAGE_CLASS>]
STORAGE_CLASS options: STANDARD, NEARLINE, COLDLINE, and ARCHIVE

Example:
python process_dicom_batch.py gs://your-bucket/dicom-zips \
projects/your-project-id/locations/your-location/datasets/your-dataset-id/dicomStores/your-dicom-store-id \
ARCHIVE
"""

import argparse
from unzip_batch import unzip_and_upload
from import_dicom_batch import import_dicom

def process_dicom_batch(gcs_uri, dicom_store_path, storage_class="ARCHIVE"):
    """Unzips files in the GCS URI and imports them to a DICOM store.

    Args:
        gcs_uri: The GCS URI containing the zip files.
        dicom_store_path: Path to the DICOM store.
        storage_class: The storage class to use for imported DICOMs.
    """

    try:
        # Parse the GCS URI
        if not gcs_uri.startswith("gs://"):
            raise ValueError("GCS URI should start with gs://")
        parts = gcs_uri.replace("gs://", "").split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        # Unzip the files
        print(f"Unzipping files in {gcs_uri}...")
        if not unzip_and_upload(bucket_name, prefix):
            raise Exception("Error occurred during unzipping.")
        print(f"Unzipping completed successfully.")

        # Import the DICOM files
        print(f"Importing DICOM files from path {gcs_uri}...")
        if not import_dicom(gcs_uri, dicom_store_path, storage_class):
            raise Exception("Error occurred during DICOM import.")
        print(f"DICOM import completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process DICOM zip files in GCS.")
    parser.add_argument("gcs_uri", help="GCS URI containing zip files (e.g., gs://your-bucket/dicom-zips)")
    parser.add_argument("dicom_store_path", help="Path to the DICOM store (e.g., projects/your-project-id/locations/your-location/datasets/your-dataset-id/dicomStores/your-dicom-store-id)")
    parser.add_argument("storage_class", nargs='?', default="ARCHIVE", 
                        help="Storage class (STANDARD, NEARLINE, COLDLINE, ARCHIVE), defaults to ARCHIVE")

    args = parser.parse_args()

    process_dicom_batch(args.gcs_uri, args.dicom_store_path, args.storage_class)
