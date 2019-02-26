from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
import airflow
from airflow import DAG

from tempfile import NamedTemporaryFile

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow_training.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


class HttpToGcsOperator(BaseOperator):
    """


    """

    template_fields = ('url', 'file', 'bucket')
    template_ext = ()

    ui_color = "#f4a460"

    @apply_defaults
    def __init__(self, url, bucket, file, *args, **kwargs):
        self.url = url
        self.bucket = bucket
        self.file = file
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        named_file = NamedTemporaryFile()
        http = HttpHook(method="GET")
        res = http.run(self.url)
        print(self.url)
        print(res.text)
        named_file.write(res.text)
        named_file.flush()

        gcs = GoogleCloudStorageHook()

        gcs.upload(self.bucket, self.file, named_file.name)


dag = DAG(
    dag_id='real_estate',
    default_args={
        'owner': 'daniel',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

http_ops = []

for currency in ["EUR", "USD"]:
    http_ops.append(HttpToGcsOperator(
        url='convert-currency?date={{ ds }}&from=GBP&to=EUR',
        bucket="airflow-daniel",
        file="currency/{{ ds }}/" + currency + ".json",
        task_id="conversion_rate_" + currency,
        dag=dag))

pgsql_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_job",
    bucket="airflow-daniel",
    sql="select * from land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    filename="land_registry_price_paid_uk/{{ ds }}/land_registry_price.json",
    dag=dag
)

load_into_bigquery = DataFlowPythonOperator(
    task_id="bqjob",
    dataflow_default_options={
        'region': 'europe-west1',
        'input': 'gs://airflow-daniel/*/*.json',
        'temp_location': 'gs://airflow-daniel/tmp',
        'staging_location': 'gs://airflow-daniel/staging',
        'table': 'airflow',
        'dataset': 'airflow',
        'project': 'airflowbolcom-b01c3abbfb10e7ee',
        'bucket': 'europe-west1-training-airfl-bb0beabe-bucket',
        'job_name': '{{ task_instance_key_str }}'
    },
    py_file="gs://airflow-daniel/dataflow_job.py",
    dag=dag)

from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator, DataprocClusterDeleteOperator,
                                                         DataProcPySparkOperator, )

dataproc_create_cluster = DataprocClusterCreateOperator(task_id="create_dataproc",
                                                        cluster_name="analyse-pricing-{{ ds }}",
                                                        project_id="airflowbolcom-b01c3abbfb10e7ee",
                                                        num_workers=2, zone="europe-west4-a", dag=dag, )
compute_aggregates = DataProcPySparkOperator(task_id='compute_aggregates',
                                             main='gs://airflow-daniel/build_statistics.py',
                                             cluster_name='analyse-pricing-{{ ds }}', arguments=[
        "gs://airflow-daniel/land_registry_price_paid_uk/{{ ds }}/*.json",
        "gs://airflow-daniel/currency/{{ ds }}/*.json", "gs://airflow-training-data/average_prices/{{ ds }}/"],
                                             dag=dag)
from airflow.utils.trigger_rule import TriggerRule

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc", cluster_name="analyse-pricing-{{ ds }}", project_id="airflowbolcom-b01c3abbfb10e7ee",
    trigger_rule=TriggerRule.ALL_DONE, dag=dag)

write_to_bq = GoogleCloudStorageToBigQueryOperator(task_id="write_to_bq",
                                                   bucket="airflow-training-data",
                                                   source_objects=["average_prices/transfer_date={{ ds }}/*"],
                                                   destination_project_dataset_table="airflow.airflow{{ ds_nodash }}",
                                                   source_format="PARQUET", write_disposition="WRITE_TRUNCATE",
                                                   dag=dag, )

[pgsql_to_gcs, http_ops] >> load_into_bigquery

pgsql_to_gcs >> dataproc_create_cluster

http_ops >> dataproc_create_cluster >> compute_aggregates >> [dataproc_delete_cluster, write_to_bq]
