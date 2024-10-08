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

"""Provides functionality to unzip batches of files in Google Cloud Storage (GCS).

This script defines functions to unzip zip files located in a GCS bucket. 
It supports parallel unzipping for faster processing of large batches.
"""

from google.cloud import storage
import zipfile
from io import BytesIO
import os.path
from concurrent.futures import ThreadPoolExecutor
import time
import random

INCLUDE_ZIP_NAME = True  # Add the file name (without .zip) as a folder to the unzipped files
NUM_THREADS = 10         # Define the number of threads

def unzip_and_upload_single(blob, bucket, debug_logs=False, retries=3, backoff_factor=2):
    """Unzips a single zip file, uploads contents, and deletes the original.

    Args:
        blob: The GCS blob object representing the zip file.
        bucket: The GCS bucket object.
        debug_logs (bool): Whether to print debug logs.
        retries (int): The maximum number of retries.
        backoff_factor (int): The factor to increase the backoff time between retries.
    """

    attempts = 0
    while attempts <= retries:
        try:
            start_time = time.time()  # Start the timer
            file_name_uri = f"gs://{bucket.name}/{blob.name}"
            if debug_logs:
                print(f"Unzipping: {file_name_uri}")
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

                        # Upload the extracted file back to GCS
                        extracted_blob = bucket.blob(extracted_file_name)
                        extracted_blob.upload_from_string(extracted_file_content)

            # Delete the original .zip file
            blob.delete()
            if debug_logs:
                end_time = time.time()  # Stop the timer
                elapsed_time = round(end_time - start_time, 2)  # Calculate elapsed time in seconds
                print(f"Unzipped and uploaded: {file_name_uri} in {elapsed_time} seconds")

            return True  # Success

        except zipfile.BadZipFile:
            print(f"Error: {file_name_uri} is not a valid zip file.")
            return False  # Not a retryable error

        except Exception as e:
            attempts += 1
            if attempts <= retries:
                wait_time = backoff_factor ** (attempts - 1) + random.uniform(0, 1)  # Exponential backoff with jitter
                print(f"Error processing {file_name_uri}: {e}. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Error processing {file_name_uri}: {e}. Max retries exceeded.")
                return False  # Max retries exceeded

    return False  # Should not reach here, but added for clarity


def unzip_and_upload(bucket_name, prefix, debug_logs=False):
    """Unzips zip files in a GCS bucket in parallel.

    Args:
        bucket_name (str): The name of the GCS bucket.
        prefix (str): The prefix to filter zip files.
    """
    try:
        start_time = time.time()  # Start the timer
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        if debug_logs:
            print(f"Seeking zip files...")
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
        zip_blobs = [blob for blob in blobs if blob.name.endswith(".zip")]
        print(f"Found {len(zip_blobs)} zip files.")

        if not zip_blobs:
            return True  # No zip files found, but not an error

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            # Submit each zip file to the thread pool
            results = list(executor.map(unzip_and_upload_single, zip_blobs, [bucket]*len(zip_blobs), [debug_logs]*len(zip_blobs)))

        # Report elapsed time
        end_time = time.time()  # Stop the timer
        elapsed_time = round(end_time - start_time, 2)  # Calculate elapsed time in seconds
        print(f"Processed {results.count(True)} zip files in {elapsed_time} seconds")

        # Count the number of False results (errors)
        error_count = results.count(False)
        if error_count > 0:
            print(f"Encountered {error_count} errors during unzip and upload process.")
            return False

        return True

    except Exception as e:
        print(f"An error occurred in unzip_and_upload: {e}")
        return False

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unzip and upload files in GCS.")
    parser.add_argument("gcs_uri", help="GCS URI to the folder containing zip files (e.g., gs://your-bucket/path/to/zip/files/)")

    args = parser.parse_args()

    # Parse the GCS URI
    parts = args.gcs_uri.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    unzip_and_upload(bucket_name, prefix, debug_logs=True)
