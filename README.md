# Load streaming data from Google Cloud PubSub to Bigquery table with transformations by Apache Beam.

## Pre-requisites:

## Create Pubsub topic and subscription
```bash
gcloud pubsub topics create your-topic-name

gcloud pubsub subscriptions create your-subscription-name --topic=projects/your-gcp-project-id/topics/your-topic-name
```

## Create Bigquery table
Note: Copy only the contents of the fields array into the file.
```bash
bq mk --location=EU --schema purchase_details.json --table your-project-id:your-dataset.purchase_details
```

## Install the dependencies
```bash
pip install -r requirements.txt
```

## Configure environment with Google Cloud defaults - Varies depends on the environment

## Windows:

```bash
gcloud auth application-default login

$env:GOOGLE_APPLICATION_CREDENTIALS="C:\<path>\gcloud\application_default_credentials.json"
$env:GOOGLE_CLOUD_PROJECT="your-gcp-project-id"

gcloud config set project "your-gcp-project-id"
```

## Linux:

```bash
gcloud auth application-default login

export GOOGLE_APPLICATION_CREDENTIALS="$HOME\.config\gcloud\application_default_credentials.json"
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"

gcloud config set project "your-gcp-project-id"
```

## Run the program on Cloud Shell
```bash
python main.py
```

## Publish messages to the topic
```bash
python publisher.py
```
