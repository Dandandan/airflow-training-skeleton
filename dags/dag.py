import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

import random

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)

print_execution_date = BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)


wait_1 = BashOperator(
    task_id="wait_1", bash_command="wait 1", dag=dag
)


wait_5 = BashOperator(
    task_id="wait_5", bash_command="wait 5", dag=dag
)

wait_10 = BashOperator(
    task_id="wait_10", bash_command="wait 10", dag=dag
)



options = ['branch a', 'branch b', 'branch c', 'branch d']


def random_branch():
    random.choice(options)

branching = BranchPythonOperator(
    task_id= 'branching',
    python_callable=random_branch,
    dag=dag)

end = DummyOperator(task_id="dummy", dag=dag)

print_execution_date >> [ wait_1, wait_5, wait_10] >> branching >> end