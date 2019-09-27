import airflow
import datetime
from airflow import DAG
from airflow import models
from airflow.operators import BashOperator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.utils import trigger_rule

default_dag_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    # Email whenever an Operator in the DAG fails.
    # 'start_date': airflow.utils.dates.days_ago(1)
    'email': models.Variable.get('email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}



output_file ='gs://prp-target/address.csv' 

with airflow.DAG(
        dag_id='demo_bq_dag_1',        
        schedule_interval = datetime.timedelta(days=1),
        default_args = default_dag_args) as dag:      
        
            
        export_commits_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_airflow_commits_to_gcs',
        source_project_dataset_table = 'able-hull-251304.my_dataset.Address',
        destination_cloud_storage_uris = [output_file],
        export_format = 'CSV'
        )
        
export_commits_to_gcs
