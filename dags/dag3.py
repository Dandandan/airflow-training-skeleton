from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
import airflow
from airflow import DAG

from tempfile import NamedTemporaryFile

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class HttpToGcsOperator(BaseOperator):
    """


    """

    template_fields = ('url', 'bucket')
    template_ext = ()

    ui_color = "#f4a460"

    @apply_defaults
    def __init__(self, url, bucket, *args, **kwargs):
        self.url = url
        self.bucket = bucket

        super(HttpToGcsOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        named_file = NamedTemporaryFile()
        http = HttpHook(method="GET")
        res = http.run(self.url)

        named_file.write(res.json())

        gcs = GoogleCloudStorageHook()

        gcs.upload(self.bucket, "abc.json", named_file.name)

        named_file.close()


dag = DAG(
    dag_id='real_estate',
    default_args={
        'owner': 'daniel',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

http_op = HttpToGcsOperator(
    url='convert-currency?date={{ ds }}&from=GBP&to=EUR',
    bucket="airflow-daniel",
    task_id="conversion_rate",
    dag=dag)

# pgsql_to_gcs = PostgresToGoogleCloudStorageOperator(
#    task_id="postgres_job",
#    bucket="airflow-daniel",
#    sql="select * from land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
#    filename="land_registry_price_paid_uk/{{ ds }}/land_registry_price.json",
#    dag=dag
# )
