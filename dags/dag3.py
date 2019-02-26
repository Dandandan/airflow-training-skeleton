from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
import airflow
from airflow import DAG

dag = DAG(
    dag_id='real_estate',
    default_args={
        'owner': 'daniel',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

pgsql_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_job",
    bucket="airflow-dheres",
    sql="select * from land_registry_price_paid_uk",
    filename="xxx.txt",
    dag=dag
)
