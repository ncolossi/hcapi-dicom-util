#!/bin/bash
# Import a GCS folder with DICOM into Healhcare API DICOM store
# It expects DICOM files have a .dcm extension
#
# import_dicom.sh <GCS_FOLDER> [<STORAGE_CLASS>]
# STORAGE_CLASS options STANDARD, NEARLINE, COLDLINE and ARCHIVE

# Load variables
if [ -e vars-local.env ] ; then
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"/vars-local.env
else
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"/vars.env
fi

# Check input
if [ $# -eq 0 ] ; then
    echo "Missing parameters. Syntax: import_dicom.sh <GCS_FOLDER> [<STORAGE_CLASS>]"
    exit 1
fi
GCS_FOLDER="$1"

# Check storage class
STORAGE_CLASS="$2"
if [ -z "$STORAGE_CLASS" ] ; then
    STORAGE_CLASS="STANDARD"
fi
if [ "$STORAGE_CLASS" != "STANDARD" -a "$STORAGE_CLASS" != "NEARLINE" -a \
     "$STORAGE_CLASS" != "COLDLINE" -a "$STORAGE_CLASS" != "ARCHIVE" ] ; then
    echo "Invalid storage class. Please use one of the following: STANDARD, NEARLINE, COLDLINE and ARCHIVE"
    exit 1
fi

# Check if GCS_FOLDER starts with gs://
if [ "${GCS_FOLDER:0:5}" != "gs://" ] ; then
    echo "GCS_FOLDER should start with gs://"
    exit 1
fi

# Import data
if [ "$STORAGE_CLASS" = "STANDARD" ] ; then
    gcloud healthcare dicom-stores import gcs $DICOM_STORE_ID \
    --dataset=$DATASET_ID \
    --location=$LOCATION \
    --gcs-uri=$GCS_FOLDER/**.dcm
else
    # Use REST call for non-standard storage classes
    REQUEST_FILE=".import_dicom_request.json"
    cat << EOF > $REQUEST_FILE 
{
  "gcsSource": {
    "uri": "$GCS_FOLDER/**.dcm"
  },
  "blob_storage_settings": {
    "blob_storage_class": "$STORAGE_CLASS"
  }
}
EOF
    
    curl -X POST \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     -H "Content-Type: application/json" \
     -d @$REQUEST_FILE \
     "https://healthcare.googleapis.com/v1beta1/projects/$PROJECT_ID/locations/$LOCATION/datasets/$DATASET_ID/dicomStores/$DICOM_STORE_ID:import"
fi
