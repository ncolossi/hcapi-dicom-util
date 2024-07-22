# hcapi-dicom-util

This repo concentrates several utilities used with Healthcare API and DICOM Adapter.

### Setup
```shell
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

### Configure
Make sure to fill in the vars.env


### Login to Google Cloud
```shell
gcloud auth login
gcloud auth application-default login
```


### DICOM Adapter
Based on the [healthcare-dicom-dicomweb-adapter](https://github.com/GoogleCloudPlatform/healthcare-dicom-dicomweb-adapter) repo, this repo helps with commands to create cluster and deploy adapters.

Please make sure to edit the yaml files to fill in parameters according to your environment.