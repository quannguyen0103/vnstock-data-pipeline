import datetime
from airflow import models
from airflow.operators import bash_operator

default_dag_args = {
	"start_date": datetime.datetime(2023, 8, 12)
	, "retries": 3
	, "retry_delay": datetime.timedelta(minutes=5)
	, "email": "user@gmail.com"
	, "email_on_failure": True
	, "email_on_retry": True
}

with models.DAG("process_subscribe_stock"
	, schedule_interval = "0 3-8 * * 1-5"
	, default_args = default_dag_args) as dag:
	for hour in range(10, 16):
		task_id = f"load_data_{hour}"
		bash_command = f"python3 /home/user/vnstock/src/data_processing/stock_subscribe.py --hour={hour}"

		load_data = bash_operator.BashOperator(
			task_id = task_id
			, bash_command = bash_command
			, dag=dag
			,
		)

load_data
