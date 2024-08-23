# Use a suitable Python base image from Google Cloud
FROM google/cloud-sdk:slim

# Set the working directory within the container
WORKDIR /app

# Install OS dependencies
RUN apt-get update && apt-get install -y python3-venv

# Copy all Python scripts and requirements.txt to the container
COPY *.py requirements.txt /app/

# Install Python dependencies
RUN python3 -m venv .venv
RUN . .venv/bin/activate && pip3 install --no-cache-dir -r requirements.txt

# Set the entrypoint to run run_batch.py by default
ENTRYPOINT ["/bin/sh", "-c", ". .venv/bin/activate && python3 run_batch.py"]
