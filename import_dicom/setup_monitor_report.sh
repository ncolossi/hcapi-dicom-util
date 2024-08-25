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

# This script builds and pushes the Docker image for the DICOM report monitor to Artifact Registry.

# It performs the following steps:
# 1. Sets required variables (PROJECT_ID, REGION, IMAGE_NAME, IMAGE_PATH).
# 2. Configures Docker for authentication with Artifact Registry.
# 3. Creates the "imaging" repository in Artifact Registry if it doesn't exist.
# 4. Builds the Docker image using the Dockerfile in the current directory.
# 5. Tags the image with the Artifact Registry path.
# 6. Pushes the tagged image to Artifact Registry.

set -e # Exit immediately if any command fails

# Set variables
echo "Setting up variables..."
if [ -z "$PROJECT_ID" ] ; then
    PROJECT_ID=$(gcloud config get-value project)
fi
if [ -z "$PROJECT_ID" ] ; then
    echo "Error: PROJECT_ID environment variable is not set." >&2
    exit 1
fi
if [ -z "$REGION" ] ; then
    echo "Error: REGION environment variable is not set." >&2
    exit 1
fi
IMAGE_NAME="monitor-report"
IMAGE_PATH=${REGION}-docker.pkg.dev/$PROJECT_ID/imaging/$IMAGE_NAME

# Setup Artifact Registry
echo "Configuring Docker for Artifact Registry..."
gcloud auth configure-docker ${REGION}-docker.pkg.dev || { echo "Error configuring Docker for Artifact Registry." >&2; exit 1; }

# Check if repository exists
echo "Checking if Artifact Registry repository exists..."
if ! gcloud artifacts repositories describe imaging --location=$REGION > /dev/null 2>&1; then
  echo "Creating Artifact Registry repository..."
  gcloud artifacts repositories create imaging --repository-format=docker --location=${REGION} --description="Imaging Repository" || { echo "Error creating Artifact Registry repository." >&2; exit 1; }
fi

# Build and push image
echo "Building Docker image..."
docker build . -t $IMAGE_NAME || { echo "Error building Docker image." >&2; exit 1; }
echo "Tagging Docker image..."
docker tag $IMAGE_NAME $IMAGE_PATH || { echo "Error tagging Docker image." >&2; exit 1; }
echo "Pushing Docker image to Artifact Registry..."
docker push $IMAGE_PATH || { echo "Error pushing Docker image to Artifact Registry." >&2; exit 1; }
echo "Docker image successfully pushed to Artifact Registry."
