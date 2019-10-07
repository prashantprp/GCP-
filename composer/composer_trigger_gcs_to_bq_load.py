#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START composer_trigger_gcs_to_bq_dag]
# [Code developed by Prashant Patiyan Rajendran -- Copy File from Cloud Storage to CSV]

"""DAG running in response to a Cloud Storage bucket change."""

import datetime

import airflow
from airflow.operators import bash_operator

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2017, 1, 1),
    }

with airflow.DAG('composer_trigger_gcs_to_bq_dag',
                 default_args=default_args, schedule_interval=None) as \
    dag:  # Not scheduled, trigger only

    # Print the dag_run's configuration, which includes information about the
    # Cloud Storage object change.

    print_gcs_info = bash_operator.BashOperator(task_id='print_gcs_info'
            , bash_command='echo {{ dag_run.conf }}')

    # [Create dataset in BQ]

    create_test_dataset = \
        bash_operator.BashOperator(task_id='create_test_dataset',
                                   bash_command='bq mk airflow_test1')

    # [Upload Csv from GCS to BQ using Load ]

    Upload_csv = bash_operator.BashOperator(task_id='Upload_csv',
            bash_command='bq load --autodetect --source_format=CSV airflow_test1.simple gs://prp-source/simple.csv'
            )

      # Template_fields= ['bucket', 'source_objects', 'schema_object', 'destination_project_dataset_table']........

print_gcs_info >> create_test_dataset >> Upload_csv

# [END composer_trigger_gcs_to_bq_dag]
