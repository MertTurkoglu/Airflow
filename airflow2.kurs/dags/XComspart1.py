import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    # ilk execution tarihinden br önceki gün
    # bu ne demek ben ui da bu dagi on dedğimde direkt açlışcak demek
    # start date 3 ocak olsaydı ben bunu 4 ocakta çalıştırabilir olcaktım eksi 1 de ordan geliyor
}

DAG = DAG(
  dag_id='simple_xcom',
  default_args=args,
  schedule_interval="@daily",
)

def push_function(**kwargs):
    message='This is the pushed message.'
    ti = kwargs['ti']
    ti.xcom_push(key="message", value=message)

def pull_function(**kwargs):
    ti = kwargs['ti']
    pulled_message = ti.xcom_pull(key='message', task_ids="push_task")
    print("Pulled Message: {}".format(pulled_message))

t1 = PythonOperator(
    task_id='push_task',
    python_callable=push_function,
    provide_context=True, 
    # bu paramatre python_callable daki fonskiyonda kwargs ı kullanmayı sağlıyor keyword argumans
    dag=DAG)

t2 = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    provide_context=True,
    dag=DAG)

t1 >> t2
