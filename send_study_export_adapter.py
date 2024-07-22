# Sends DICOM studies to Export DICOM Adapter
#
# This script queries a BigQuery table for all instances associated with a given StudyInstanceUID.
# For each instance, it constructs a DICOMweb instance path and publishes it to a Pub/Sub topic for 
# the Export DICOM Adapter.
#
# Usage: python send_study_export_adapter.py <StudyInstanceUID>
#
# Example: python send_study_export_adapter.py 1.2.3.4.5.6.7.8.9.0.1
# 

import os
import sys
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import pubsub_v1

# Load environment variables from .env file
if os.path.isfile("vars-local.env"):
    load_dotenv("vars-local.env")
else:
    load_dotenv("vars.env")
# Get variables from Environment
project_id = os.getenv("PROJECT_ID")
pubsub_topic_id = os.getenv("PUBSUB_TOPIC_ID")
bigquery_dataset = os.getenv("BIGQUERY_DATASET")
bigquery_table = os.getenv("BIGQUERY_TABLE")


# Publishes all instances from a given StudyInstanceUID
def publish_study_pubsub(study_instance_uid):

    # Clients
    client = bigquery.Client(project=project_id)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, pubsub_topic_id)

    # Query to Fetch Instance Paths
    query = f"""
    SELECT CONCAT('/studies/', StudyInstanceUID, '/series/', SeriesInstanceUID, '/instances/', SOPInstanceUID) instance
    FROM `{project_id}.{bigquery_dataset}.{bigquery_table}`
    WHERE StudyInstanceUID = '{study_instance_uid}'
    """

    # Run BigQuery Query
    query_job = client.query(query)

    # Process Results and Publish to Pub/Sub 
    for row in query_job.result():
        instance_path = row["instance"]
        # Publish the message (ensuring it's encoded)
        future = publisher.publish(topic_path, instance_path.encode("utf-8"))
        print(f"Published instance path: {instance_path} (message ID: {future.result()})")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Missing StudyInstanceUID argument.")
        sys.exit(1)  # Exit with an error code
    else:
        publish_study_pubsub(sys.argv[1]) 
