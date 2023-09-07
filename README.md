# Source

## Architecture
(images/vnstock_architecture)
## Setup
- Set up `Airflow` on Google Cloud VM
- Create `GCS buckets`
- Set up a [Google Cloud connection](src/connection_configurating/cloud_connection.py) for `Airflow`
- Configure `Airflow SMTP` to send alert emails when a task failed
- Create the `vnstock` topic on Google Pub/sub

### ETL FLOW
