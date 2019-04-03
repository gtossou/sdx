
from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='sdx_workflow',
    default_args=args,
    schedule_interval="@once"
)

get_data_task = BashOperator(
    task_id='get_data',
    bash_command='python /down.py',
    dag=dag
)
quality_task = BashOperator(
    task_id='quality',
    bash_command='spark-submit /quality.py',
    dag=dag
)

model_task = BashOperator(
    task_id='model',
    bash_command='spark-submit /model.py',
    dag=dag
)

get_data_task >> quality_task
quality_task >> model_task

