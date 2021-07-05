from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
   'start_date': datetime(2020, 1, 1),
}

with DAG(
   "parallel_dag",
   schedule_interval="@daily",
   default_args=default_args,
   catchup=False
   # concurrency=1, # Exactly same param of dag_concurrency. (tasks in this dag)
   # max_active_runs=1, # Exactly same param of max_active_runs_per_dag, but of this dag.
) as dag:

    task_1 = BashOperator(
        task_id="task_1",
        bash_command='sleep 3'
    )

    task_2 = BashOperator(
        task_id="task_2",
        bash_command='sleep 3'
    )

    task_3 = BashOperator(
        task_id="task_3",
        bash_command='sleep 3'
    )

    task_4 = BashOperator(
        task_id="task_4",
        bash_command='sleep 3'
    )
    
    task_1 >> [task_2, task_3] >> task_4