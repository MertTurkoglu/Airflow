from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator

default_args={

    'owner':'airflow',
    'start_date':datetime(2022,1,4),
    'depends_on_past':True

}

def _push_task_(**kwargs):
    ti=kwargs['ti']
    value=5
    ti.xcom_push(key='pushed_value', value=value)


def _branching_(**kwargs):
    ti=kwargs['ti']
    pulled_value=ti.xcom_pull(key='pushed_value',task_ids='push_task')
    print(pulled_value)
    if pulled_value % 2 == 0 :
        return 'even_task'
    else:
        return 'odd_task'


with DAG( dag_id='branching',default_args=default_args,schedule_interval='@daily',catchup=False) as dag :

    push_task=PythonOperator(

        task_id='push_task',
        python_callable=_push_task_,
        provide_context=True
    )
    
    branching=BranchPythonOperator(

        task_id='branching',
        python_callable=_branching_,
        provide_context=True
    )

    even_task= BashOperator(
        task_id="even_task",
        bash_command='echo "value is even number"',
       
    )

    odd_task = BashOperator(
        task_id="odd_task",
        bash_command='echo "value is odd number"',
    )


    push_task >> branching >> [even_task,odd_task]