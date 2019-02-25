import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
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
    task_id="wait_1", bash_command="sleep 1", dag=dag
)

wait_5 = BashOperator(
    task_id="wait_5", bash_command="sleep 5", dag=dag
)

wait_10 = BashOperator(
    task_id="wait_10", bash_command="sleep 10", dag=dag
)

options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']


def print_weekday(execution_date, **context):
    print(execution_date.strftime("%A"))


weekday_person_to_email = {
    0: "Bob",
    1: "Bob2",
    2: "Bob3",
    3: "Bob4",
    4: "Bob5",
    5: "Bob6",
    6: "Bob7",
}


def get_name(execution_date, **context):
    return "email_" + weekday_person_to_email[execution_date.weekday()]


print_op = PythonOperator(task_id="print_weekday",
                          python_callable=print_weekday,
                          provide_context=True,
                          dag=dag)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=get_name,
    provide_context=True,
    dag=dag)

end = DummyOperator(task_id="dummy", dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)

print_execution_date >> [wait_1, wait_5, wait_10] >> print_op >> branching

for name in weekday_person_to_email.values():
    email_task = DummyOperator(task_id="email_" + name, dag=dag)
    branching >> email_task >> end