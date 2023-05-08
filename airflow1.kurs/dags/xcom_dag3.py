# belirli bir şarta göre taski çalıştırma konusuna devam 

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0) 
    print(f'model\'s accuracy: {accuracy}')
    ti.xcom_push(key='model_accuracy',value=accuracy)
  




def _choose_best_model(ti):
    print('choose best model')
    accuracies=ti.xcom_pull(key='model_accuracy',
    task_ids=[
        'processing_tasks.training_model_a',
        'processing_tasks.training_model_b',
        'processing_tasks.training_model_c'
    ])
    # accuracies değişkenim bir diziydi
    for accuracy in accuracies:
        if accuracy > 5: 
            print(accuracy)
    # accurate değeri 5 den büyükse task idsi accurate olan task idyi çalıştır demek
            return 'accurate'
    
    return 'inaccurate' 
    # 5 den küçükse task id si inaccurate olan taski çalıştır demek
   
   
    
  


with DAG('xcom_dag3', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        
        do_xcom_push=False 

    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
            
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )

    accurate=DummyOperator(
        task_id='accurate'
        
     
    )
    inaccurate=DummyOperator(
        task_id='inaccurate'
        
     
    )
    storing=DummyOperator(
        task_id='storing',
        trigger_rule='none_failed_or_skipped'
        # trigger rules daginden sonra bu paramatreyi ekliceğimi anladım

    )


    downloading_data >> processing_tasks >> choose_best_model
    choose_best_model >>[accurate,inaccurate] >> storing 

    # choose best modelden sonra çalışan task accurate veya inaccurate olcak sonrasında da storing task inin çalışamsını istiyoruz
    # ama atıyorum choose best modelden sonra accurate çalışıyor sonrasında inaccurate ve storing task i skip ediliyor
    # benim beklediğim accurate çalışıyorsa inaccurate in skip edilmesi ve storing taskinde çalışması
    # inaccurate çalışıyorsa accurate in skip edilmesi ve storing task inin çalışması
    # peki bunu nasıl sağlıcaz bir sonraki dag bunun için trigger_rules dagi
   