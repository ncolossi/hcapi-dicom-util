#/bin/bash
# Creates a GKE cluster for testing purposes (DICOM Adapter)

# Load variables
if [ -e "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"/../vars-local.env ] ; then
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"/../vars-local.env
else
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"/../vars.env
fi

# Create cluster
gcloud container clusters create dicom-adapter \
  --zone=$ZONE \
  --scopes=https://www.googleapis.com/auth/cloud-healthcare,https://www.googleapis.com/auth/pubsub \
  --num-nodes=$NUM_NODES \
  --machine-type=$MACHINE_TYPE \
  --network=$VPC \
  --subnetwork=$SUBNET \
  --service-account=$SERVICE_ACCOUNT