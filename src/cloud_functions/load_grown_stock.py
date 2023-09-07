from google.cloud import bigquery
from google.cloud import storage

def process_data(event, context):
    bucket_name = event["bucket"]
    file_name = event["name"]

    # Check if the uploaded file is 'product.json'
    if file_name.endswith(".csv"):
        bq_client = bigquery.Client()
        dataset_id = "vnstock"
        table_id = "grown_stock"
        bq_client.project = "project_id"

        table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

        # Load data from Cloud Storage into a BigQuery table
        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records = 100
        )

        uri = f"gs://{bucket_name}/{file_name}"
        load_job = bq_client.load_table_from_uri(uri, table_ref, location = "US", job_config=job_config)
        load_job.result()
        print(f"Successfully loaded {file_name} to stock_data table.")

    else:
        print(f"Skipping {file_name} as it is not a CSV file.")
