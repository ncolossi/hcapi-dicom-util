# DICOM Report Monitor Setup Guide

This document provides instructions on setting up and running the `monitor_report.py` script on Google Cloud Batch on an hourly schedule. This script monitors a GCS directory for DICOM import reports and triggers the DICOM import and validation pipeline for each new report.

## Overview

The purpose of this setup is to automate the processing of DICOM import reports on an hourly schedule. The `monitor_report.py` script, running within a Cloud Batch job, continuously monitors a specified GCS directory for new reports and triggers the import and validation process for each new report found. This ensures that DICOM data is imported and validated in a timely and efficient manner.

## Setup Instructions

### 1. Enable APIs
Enable the following Google Cloud APIs:
- Google Cloud Batch API
- Google Workflows API
- Cloud Scheduler API
You can enable these APIs through the Google Cloud Console or using the `gcloud` command-line tool:
```bash
gcloud services enable batch.googleapis.com workflows.googleapis.com cloudscheduler.googleapis.com
```

### 2. Create a Service Account
Create a service account that will be used to run the monitor_report.py script. You can create a service account through the Google Cloud Console or using the gcloud command-line tool:
```bash
gcloud iam service-accounts create monitor-report-sa --display-name="DICOM Report Monitor Service Account"
```

### 3. Set up IAM Permissions for service account
Grant the following IAM roles to the service account:
* **Workflows Invoker:** Allows the service account to invoke Google Cloud Workflows.
* **Batch Job Editor:** Allows the service account to create and manage Cloud Batch jobs.
* **Service Account User:** Allows the service account to act as the specified service account.
* **Batch Agent Reporter:** Allows the service account to run Cloud Batch jobs.
* **Log Writer:** Allows the service account to write logs to Cloud Logging.
* **Artifact Registry Reader:** Allows the service account to read from Artifact Registry.
* **Storage Object User:** Allows the service account to access and manage Cloud Storage objects.
* **BigQuery Data Editor:** Allows the service account to create, update, and delete BigQuery data.
* **BigQuery Job User:** Allows the service account to create and manage BigQuery jobs.
* **Healthcare DICOM Editor:** Allows the service account to import, export, and manage DICOM instances in Healthcare API.
```bash
export PROJECT_ID="your-project-id"
export SERVICE_ACCOUNT_EMAIL="monitor-report-sa@$PROJECT_ID.iam.gserviceaccount.com"
for role in roles/workflows.invoker roles/batch.jobsEditor roles/iam.serviceAccountUser \
            roles/batch.agentReporter roles/logging.logWriter roles/artifactregistry.reader \
            roles/storage.objectUser roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/healthcare.dicomEditor 
do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="$role" > /dev/null
done
```

### 4. Setup IAM Permissions for the user

In order to create Workflows jobs and Scheduler jobs, you need the following permission roles assigned to your user:

* **Artifact Registry Administrator:** Grants access to manage Artifact Registry resources, including repositories, packages, and settings.
* **Artifact Registry Repository Administrator:** Grants access to manage a specific Artifact Registry repository, including packages and settings within that repository.
* **Workflows Admin:** Grants access to create, manage, and execute Google Cloud Workflows workflows.
* **Cloud Scheduler Admin:** Grants access to create, manage, and execute Cloud Scheduler jobs.

```bash
export PROJECT_ID="your-project-id"
export USER_EMAIL="your-user@your-company.com"
for role in roles/artifactregistry.admin roles/artifactregistry.repoAdmin \
            roles/workflows.admin roles/cloudscheduler.admin 
do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="user:$USER_EMAIL" \
        --role="$role" > /dev/null
done
```

## Setup Container monitor-report

### 1. Run setup_monitor_report.sh
Navigate to the `import_dicom` directory and run the `setup_monitor_report.sh` script to build and push the Docker image for the `monitor_report.py` script to Artifact Registry:
```bash
export PROJECT_ID="your-project-id"
export REGION="your-region"
bash setup_monitor_report.sh
```
Make sure to set the `PROJECT_ID` and `REGION` environment variables before running the script.

## Configure Workflows YAML

### 1. Change Variables in `workflows_monitor_report.yaml`
Open the `workflows_monitor_report.yaml` file and replace the placeholders within the init task, listed between quotes, with your actual values:

* `"your-project"`: Your Google Cloud project ID.
* `"your-region"`: The region where you want to run the Cloud Batch job (e.g., "us-central1").
* `"gs://your-bucket/path/REPORT"`: The GCS URI of the directory containing the DICOM import reports.
* `"your-dicom-store-path"`: The full path to your Healthcare API DICOM store (e.g., "projects/your-project/locations/your-region/datasets/your-dataset/dicomStores/your-dicom-store").
* `"your-bigquery-table-id"`: The BigQuery table ID in the format "project.dataset.table".
* `"ARCHIVE"`: Your desired storage class for imported DICOMs (or leave as "ARCHIVE" for the default).
* `"your-vpc"`: The name of your VPC network.
* `"your-subnet-name"`: The name of the subnet within your VPC.
* `"your-service-account-email"`: The email address of the service account you created for running the workflow.

## Create Workflows Based on YAML

### 1. Create Workflows from gcloud
Use the following gcloud command to create a Google Cloud Workflows workflow based on the configured YAML file:
```bash
export REGION="your-region"
gcloud workflows deploy monitor-report-workflow --source=workflows_monitor_report.yaml --location=$REGION
```
Replace `your-region` with the desired region for your workflow.

### 2. Set up Schedule Trigger
Use the following gcloud command to create a Cloud Scheduler job that triggers the workflow every hour:
```bash
export PROJECT_ID="your-project-id"
export REGION="your-region"
export SERVICE_ACCOUNT_EMAIL="monitor-report-sa@$PROJECT_ID.iam.gserviceaccount.com"
gcloud scheduler jobs create http monitor-report-job \
  --schedule="0 * * * *" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/monitor-report-workflow/executions" \
  --http-method=POST \
  --oauth-service-account-email="$SERVICE_ACCOUNT_EMAIL" \
  --location=$REGION 
```
Replace `your-project`, `your-region`, and the `service account email` with your actual values.

This setup ensures that your DICOM import reports are processed automatically every hour using Google Cloud Batch and Workflows.