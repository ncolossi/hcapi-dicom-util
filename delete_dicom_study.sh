#!/bin/bash
# Delete a DICOM study in Healhcare API DICOM store
#
# delete_dicom_study.sh <StudyInstanceUID>

# Load variables
if [ -e vars-local.env ] ; then
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"/vars-local.env
else
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"/vars.env
fi

# Check input
if [ $# -eq 0 ] ; then
    echo "Missing parameters. Syntax: delete_dicom_study.sh <StudyInstanceUID>"
    exit 1
fi
STUDY_INSTANCE_UID="$1"

curl -X DELETE \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "Content-Type: application/dicom+json; charset=utf-8" \
    "https://healthcare.googleapis.com/v1beta1/projects/$PROJECT_ID/locations/$LOCATION/datasets/$DATASET_ID/dicomStores/$DICOM_STORE_ID/dicomWeb/studies/$STUDY_INSTANCE_UID"