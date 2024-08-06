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

from google.cloud import storage
import zipfile
from io import BytesIO
import os.path

INCLUDE_ZIP_NAME = True

def unzip_and_upload(bucket_name, prefix):
    """Unzips zip files in a GCS bucket, uploads contents, and deletes originals.

    Args:
        bucket_name (str): The name of the GCS bucket.
        prefix (str): The prefix to filter zip files (e.g., "path/to/zip/files/").
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    for blob in blobs:
        if blob.name.endswith(".zip"):
            try:
                print(f"Unzipping: {blob.name}")
                file_name = blob.name
                zip_bytes = blob.download_as_bytes()

                # Extract the zip file in memory
                with zipfile.ZipFile(BytesIO(zip_bytes)) as zip_file:
                    for file_info in zip_file.infolist():
                        if not file_info.is_dir():
                            # Get the file name and contents
                            if INCLUDE_ZIP_NAME:
                                # Use file_name (without .zip) as path
                                extracted_file_name = f"{file_name[:-4]}/{file_info.filename}"
                            else:
                                # Use only dirname from file_name as path
                                extracted_file_name = f"{os.path.dirname(file_name)}/{file_info.filename}"
                            extracted_file_content = zip_file.read(file_info)

                            # Upload the extracted file back to GCS (same bucket, different name)
                            extracted_blob = bucket.blob(extracted_file_name)
                            extracted_blob.upload_from_string(extracted_file_content)

                # Delete the original .zip file
                blob.delete()
                print(f"Unzipped and uploaded: {blob.name}")

            except zipfile.BadZipFile:
                print(f"Error: {blob.name} is not a valid zip file.")

            except Exception as e:  # Catch any other unexpected errors
                print(f"Error processing {blob.name}: {e}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unzip and upload files in GCS.")
    parser.add_argument("gcs_uri", help="GCS URI to the folder containing zip files (e.g., gs://your-bucket/path/to/zip/files/)")

    args = parser.parse_args()

    # Parse the GCS URI to extract bucket name and prefix
    parts = args.gcs_uri.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    unzip_and_upload(bucket_name, prefix)
