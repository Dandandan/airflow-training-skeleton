from airflow import DAG
from datetime import datetime
import airflow

from bigquery_get_data import BigQueryGetDataOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

dag = DAG(
    dag_id='godatafest',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql="""select author.name,
        count(*) as commits
    from `bigquery-public-data.github_repos.commits` 
    where "apache/airflow" in unnest(repo_name)
    and EXTRACT(DATE FROM committer.date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    group by author.name
    
    order by count(*) DESC
    LIMIT 10""",
    dag=dag
)

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def send_to_slack_func(**context):
    ti = context['ti']

    v1 = ti.xcom_pull(key=None, task_ids='bq_fetch_data')
    print(v1)
    op = SlackAPIPostOperator(
        task_id="slack_post",
        text=str(v1),
        token="xoxp-559854890739-559228586160-560368279685-30c1e30ee86fff97ccfcaee36719d845", dag=dag)
    op.execute()


send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack