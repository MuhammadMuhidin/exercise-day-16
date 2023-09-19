import datetime
import os
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule


MAIN_CLASS= 'org.apache.spark.examples.JavaWordCount'
FILE_JAR=[models.Variable.get('gcp_bucket') + '/spark-examples_2.12-3.4.1.jar']
ARGUMENTS=[models.Variable.get('gcp_bucket') + '/input.txt']

default_dag_args = {
    'project_id': models.Variable.get('gcp_project'),
    'retry_delay': datetime.timedelta(minutes=5),
    'retries': 1
}

with models.DAG(
        'dag_proc_spark',
        start_date=datetime.datetime(2023, 9, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_dag_args
) as dag:

    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='temp-spark-cluster-{{ ds_nodash }}',
        num_workers=2,
        region=models.Variable.get('gce_region'),
        zone=models.Variable.get('gce_zone'),
        image_version='2.0',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2',
        master_disk_size=50,
        worker_disk_size=50)

    run_dataproc_spark = dataproc_operator.DataProcSparkOperator(
        task_id='run_dataproc_spark',
        region=models.Variable.get('gce_region'),
        dataproc_jars=FILE_JAR,
        main_class=MAIN_CLASS,
        cluster_name='temp-spark-cluster-{{ ds_nodash }}',
        arguments=ARGUMENTS)

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        region=models.Variable.get('gce_region'),
        cluster_name='temp-spark-cluster-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster