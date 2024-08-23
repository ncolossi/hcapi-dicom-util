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

#/bin/bash

# Set variables
if [ -z "$REGION" ] ; then
    # REGION="us-central1"
    REGION="southamerica-east1"
fi
if [ -z "$PROJECT_ID" ] ; then
    PROJECT_ID=$(gcloud config get-value project)
fi
IMAGE_NAME="run-batch"
IMAGE_PATH=${REGION}-docker.pkg.dev/$PROJECT_ID/imaging/$IMAGE_NAME

# setup Artifact Registry
gcloud auth configure-docker ${REGION}-docker.pkg.dev
gcloud artifacts repositories create imaging --repository-format=docker --location=${REGION} --description="Imaging Repository"

# Build and push image
docker build . -t $IMAGE_NAME
docker tag $IMAGE_NAME $IMAGE_PATH
docker push $IMAGE_PATH
