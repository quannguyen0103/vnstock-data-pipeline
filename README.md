# Source

## Architecture
![Alt text](images/vnstock_architecture.png)

## 0. Setup
- Set up `Airflow` on Google Cloud VM
- Create `GCS buckets`: `vnstock`, `growth_stock`
- Set up a [Google Cloud connection](src/connection_configurating/cloud_connection.py) for `Airflow`
- Configure `Airflow SMTP` to send alert emails when a task failed
- Create the `vnstock` topic on `Google Pub/Sub`

### 1. Load data to GCS buckets
- Load stock data in the past 1 year as a `CSV` file in the `vnstock` bucket: [load_year_data.py](src/data_processing/load_year_data.py)
- Create an Airflow dag: [daily_pipeline.py](src/dags/daily_dag.py)
  - Load stock data daily and store each day as individual `CSV` files in the `vnstock` bucket: [load_daily_data.py](src/data_processing/load_data.py)
  - Calculate and select stocks with the most stable growth in the last 3 months by submit a job to `Dataproc` (Spark): [load_growth_stock.py](src/data_processing/grown_stock.py)
  - Run at 4 PM every weekday (Monday to Friday)
  - Retry 3 times, each time 5 minutes apart
  - Send an alert email when a task failed
- Create another Airflow dag: [hourly_pipeline.py](src/dags/hourly_dag.py)
  - Load and publish subscribed stock data to the `Google Pub/Sub` `vnstock` topic hourly: [load_subscribe_data.py](src/data_processing/subsribed_stock.py)
  - If any subscribed stock drops over 10% in price, send a warning message to `Telegram` via the Telegram bot
  - Run hourly from 10 AM to 3 PM every weekday
  - Retry 3 times, each time 5 minutes apart
  - Send an alert email when a task failed
