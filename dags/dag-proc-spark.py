import datetime
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule


default_dag_args = {
    'project_id': models.Variable.get('gcp_project'),
    'retry_delay': datetime.timedelta(minutes=5),
    'retries': 1
}

with models.DAG(
        'composer_spark',
        start_date=datetime.datetime(2023, 9, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_dag_args
) as dag:

    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='temp-spark-{{ ds_nodash }}',
        num_workers=2,
        region=models.Variable.get('gce_region'),
        zone=models.Variable.get('gce_zone'),
        image_version='2.0',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2',
        master_disk_size=50,
        worker_disk_size=50)

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        region=models.Variable.get('gce_region'),
        cluster_name='temp-spark-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> delete_dataproc_cluster
