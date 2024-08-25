# DICOM Import and Validation Scripts

This directory contains a set of Python scripts designed to automate the process of importing DICOM files into a Healthcare API DICOM store and validating the imported data against a provided CSV report.

## Overall Objective

The primary goal of these scripts is to provide a robust and automated solution for:

1. **Unzipping and importing DICOM files from Cloud Storage into a Healthcare API DICOM store.**
2. **Validating the imported DICOM metadata against a reference CSV report.**

This ensures data integrity and consistency during the import process.

## Setup

### 1. Python Virtual Environment

It's recommended to use a Python virtual environment to manage dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Requirements

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Running the Scripts

### 1. Running DICOM Report Monitor on Schedule
It is possible to run the DICOM Report Monitor on Google Cloud Batch on an hourly schedule. For detailed instructions, refer to the [DICOM Report Monitor Setup Guide](monitor_report.md).

### 2. Running Individual Scripts

#### 2.1. `process_dicom_batch.py`
This script unzips DICOM files from Cloud Storage and imports them into a DICOM store.

Usage:
```bash
python process_dicom_batch.py <GCS_URI> <DICOM_STORE_PATH> <STORAGE_CLASS>
```

Example:
```bash
python process_dicom_batch.py gs://your-bucket/path/to/dicoms projects/.../dicomStores/... ARCHIVE
```

#### 2.2. `validate_dicom_batch.py`
This script compares a CSV report with data in a BigQuery table to validate imported DICOM metadata.

Usage:

```bash
python validate_dicom_batch.py <GCS_URI_CSV> <BIGQUERY_TABLE_ID>
```

Example:

```bash
python validate_dicom_batch.py gs://your-bucket/path/to/report.csv your-project.your_dataset.your_table
```

## Notes
* Refer to the individual script docstrings for more detailed information on their usage and parameters.