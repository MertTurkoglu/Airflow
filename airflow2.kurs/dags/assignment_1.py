
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator


args = {
	'owner': 'airflow',
	'start_date': datetime(2019,12,19),
	'retries': 1,
    "retry_delay": timedelta(seconds=10),
}


dag = DAG('Assignment_1', default_args=args,catchup=False)	


task1 = BashOperator(task_id='create_directory', bash_command='mkdir ~/dags/test_dir2', dag=dag)



task2 = BashOperator(task_id='get_shasum', bash_command='shasum ~/dags/test_dir', dag=dag)


task1 >> task2
