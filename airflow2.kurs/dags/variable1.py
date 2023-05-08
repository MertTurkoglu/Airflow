import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG("variable1", default_args=default_args, schedule_interval=timedelta(1), catchup=False)

t1 = BashOperator(task_id="print_path", bash_command="echo {{var.value.source_path}}", dag=dag)
