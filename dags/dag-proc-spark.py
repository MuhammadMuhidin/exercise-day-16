import datetime
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule


default_dag_args = {
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('project_id')
}

with models.DAG(
        'composer_spark',
        start_date=datetime.datetime(2023, 9, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_dag_args) as dag:

    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='temp-spark-{{ ds_nodash }}',
        num_workers=2,
        region='us-east1',
        zone=models.Variable.get('zone'),
        image_version='2.0',
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2')

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        region='us-east1',
        cluster_name='temp-spark-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> delete_dataproc_cluster
