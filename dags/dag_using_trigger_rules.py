from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}


with DAG('dag_using_trigger_rules', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    # 0 Success
    # 1 Failed
    
    with TaskGroup('default_rules') as default_rules:
        task_1 = BashOperator(
            task_id="task_1",
            bash_command="exit 0",
            do_xcom_push=False,
        )

        task_2 = BashOperator(
            task_id="task_2",
            bash_command="exit 0",
            do_xcom_push=False,
        )

        task_3 = BashOperator(
            task_id="task_3",
            bash_command="exit 0",
            do_xcom_push=False,
        )

        [task_1, task_2] >> task_3

    with TaskGroup('all_failed') as all_failed:
        task_1 = BashOperator(
            task_id="task_1",
            bash_command="exit 1",
            do_xcom_push=False,
        )

        task_2 = BashOperator(
            task_id="task_2",
            bash_command="exit 1",
            do_xcom_push=False,
        )

        task_3 = BashOperator(
            task_id="task_3",
            bash_command="exit 0",
            do_xcom_push=False,
            trigger_rule ="all_failed"
        )
        [task_1, task_2] >> task_3

    with TaskGroup('all_done') as all_done:
        task_1 = BashOperator(
            task_id="task_1",
            bash_command="exit 1",
            do_xcom_push=False,
        )

        task_2 = BashOperator(
            task_id="task_2",
            bash_command="exit 0",
            do_xcom_push=False,
        )

        task_3 = BashOperator(
            task_id="task_3",
            bash_command="exit 0",
            do_xcom_push=False,
            trigger_rule ="all_done"
        )
        [task_1, task_2] >> task_3

    
    with TaskGroup('one_failed') as one_failed:
        task_1 = BashOperator(
            task_id="task_1",
            bash_command="exit 1",
            do_xcom_push=False,
        )

        task_2 = BashOperator(
            task_id="task_2",
            bash_command="sleep 30",
            do_xcom_push=False,
        )

        task_3 = BashOperator(
            task_id="task_3",
            bash_command="exit 0",
            do_xcom_push=False,
            trigger_rule ="one_failed"
        )
        [task_1, task_2] >> task_3