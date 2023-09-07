# Source

## Architecture
![Alt text](images/vnstock_architecture.png)
## 0. Setup
- Set up `Airflow` on Google Cloud VM
- Create `GCS buckets`
- Set up a [Google Cloud connection](src/connection_configurating/cloud_connection.py) for `Airflow`
- Configure `Airflow SMTP` to send alert emails when a task failed
- Create the `vnstock` topic on Google Pub/sub
## 1. Airflow Dags
### a. [Daily_pipeline](src/dags/daily_dag.py)
- ETL flow: load stock data and store it in the GCS bucket and the Google BigQuery daily
- Run at 4 PM every weekday (Monday to Friday)
- Retry 3 times, each time 5 minutes apart
- Send an alert email when a task failed
### b. [Hourly_pipeline](src/dags/hourly_dag.py)
- ETL flow: load subscribed stock data and publish it to Google Pub/sub hourly. If any stock drops over 10% in price, send a warning message to Telegram
- Run hourly from 10 AM to 3 PM every weekday
- Retry 3 times, each time 5 minutes apart
- Send an alert email when a task failed
### 1. Load data
- Load stock data in the past 1 year and store it as a `CSV` file in a GCS bucket folder: [load_year_data.py](src/data_processing/load_year_data.py)
- Create an Airflow dag:
  - Load stock data daily and store each day as a `CSV` file in a GCS bucket folder.
  - Calculate and select 
