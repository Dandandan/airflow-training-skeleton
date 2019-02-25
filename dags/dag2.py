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

import ast
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
    print(type(v1))

    res = []

    for x in v1:
        #print(x[0])
        res.append(x[0].decode('utf-8', errors="replace"))

    op = SlackAPIPostOperator(
        task_id="slack_post",
        text=str(", ".join(res) + " were really active last week!"),
        username="daniels_github_analyzer",
        token=Variable.get("token"), dag=dag)
    op.execute(context=context)


send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack
