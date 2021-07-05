from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
   'start_date': datetime(2020, 1, 1),
}

with DAG(
   "parallel_dag_with_tasks_group",
   schedule_interval="@daily",
   default_args=default_args,
   catchup=False
) as dag:

    task_1 = BashOperator(
        task_id="task_1",
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks_1') as processing_tasks_1:

        task_2 = BashOperator(
            task_id="task_2",
            bash_command='sleep 3'
        )

        with TaskGroup('spark_tasks') as spark_tasks:
            # Task ID `task_3` is prefix of spark_tasks
            task_3 = BashOperator(
                task_id="task_3",
                bash_command='sleep 3'
            )

        with TaskGroup('flink_tasks') as flink_tasks:
            # Task ID `task_3` is prefix of flink_tasks
            task_3 = BashOperator(
                task_id="task_3",
                bash_command='sleep 3'
            )

    task_4 = BashOperator(
        task_id="task_4",
        bash_command='sleep 3'
    )
    
    task_1 >> processing_tasks_1 >> task_4