from google.cloud import bigquery
from google.cloud import storage

def process_data(event, context):
    # Extract bucket and file information from the event
    bucket_name = event["bucket"]
    file_name = event["name"]

    # Check if the uploaded file is 'output.json'
    if file_name == "output.json":
        # Initialize BigQuery client and destination table
        bq_client = bigquery.Client()
        dataset_id = "tiki"
        table_id = "new_product"
        bq_client.project = "project_id"

        # Define the fully-qualified table name
        table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
        
        # Read the JSON file data
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        json_data = json.loads(blob.download_as_text())

        # Load data from Cloud Storage into a BigQuery table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect = True,
            max_bad_records = 50  # Set maximum number of errors allowed

        )

        uri = f"gs://{bucket_name}/{file_name}"
        load_job = bq_client.load_table_from_json(json_data, table_ref, location = "US", job_config=job_config)
        load_job.result()  # Wait for the job to complete

        # Delete the file from Cloud Storage
        blob.delete()

        print(f"Write '{file_name}' to BigQuery table '{table_id}' in dataset '{dataset_id}'.")

    else:
        print(f"Skipping file '{file_name}' as it is not 'output.json'.")
