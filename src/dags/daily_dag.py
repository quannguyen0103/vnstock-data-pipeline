import datetime
from airflow import models
from airflow.operators import bash_operator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.contrib.operators import dataproc_operator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

current_date = datetime.datetime.now().date()
job_name = f"job_{current_date}"

default_dag_args = {
	"start_date": datetime.datetime(2023, 8, 12)
	, "retries": 3
	, "retry_delay": datetime.timedelta(minutes=5)
	, "email": "user@gmail.com"
	, "email_on_failure": True
	, "email_on_retry": True
}

with models.DAG("process_vnstock_data"
	, schedule_interval = "0 9 * * 1-5"
	, default_args = default_dag_args) as dag:

	load_data = bash_operator.BashOperator(
		task_id = "load_data"
		, bash_command = "python3 /home/user/vnstock/src/data_processing/load_data.py"
		, dag=dag
		,
)
	migrate_data = bash_operator.BashOperator(
		task_id = "migrate_data"
		, bash_command = "/home/user/vnstock/src/data_processing/migrate_data.sh "
		, dag=dag
		,
)
	create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
                task_id = "create_dataproc_cluster"
		, gcp_conn_id = "gcp_connection"
                , project_id = "project_id"
                , cluster_name = "my-cluster"
                , region = "us-central1"
		, num_workers = 2
		, master_machine_type = "n1-standard-2"
		, worker_machine_type="n1-standard-2"
		, master_disk_size=50
		, worker_disk_size=50
                , dag=dag
                ,
)
	submit_job = DataProcPySparkOperator(
		task_id = "select_stock"
		, gcp_conn_id = "gcp_connection"
    		, region = "us-central1"
		, cluster_name = "my-cluster"
		, main= "gs://growth_stock/py_script/growth_stock.py"
		, job_name = job_name
    		, dag=dag
		,
)
	delete_cluster = DataprocDeleteClusterOperator(
		task_id = "delete_cluster"
		, gcp_conn_id = "gcp_connection"
		, project_id = "project_id"
		, cluster_name = "my-cluster"
		, region = "us-central1"
		, dag=dag
		,
)

load_data >> migrate_data >> create_dataproc_cluster >> submit_job >> delete_cluster
