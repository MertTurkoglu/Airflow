#belirli bir şarta göre taski çalıştırma konusunu anlamak için dag
# accuracy değeri 5 ten büyükse şu taski çalıştır değilse şu taski gibi
# benim processing_task idli task grubumdan accuracy değerleri dönüyordu işte bu dönen değerlere göre istediğim taski çalıştırcam
# bunun için extra bi importa ihtiyacım var BranchPythonoperator
#isaccurate adında taskim olcak choose_best_model taskinden gelen accuracies değişkenine göre bu değer 5 ten büyükse
# bu task çalışcak
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
    accuracies=ti.xcom_pull(key='model_accuracy',task_ids=[
        'processing_tasks.trainig_model_a',
        'processing_tasks.training_model_b',
        'processing_tasks.training_model_c'
    ])
   
    print(accuracies) 
    
  
def _is_accurate():
    return ('accurate') 
    # bu fonksiyonu kullanan taskten sonra task idsi accurate olan taskin çalışmasını istiyoruz


with DAG('xcom_dag2', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        # bash operatörü varsayılan olarak xcom oluşturur ui xcoms diyince de gördük zten oluşturduğunu oluşmasını istemiyorsan
        do_xcom_push=False 
        #peki hangi operatörlerin varsayılan olarak xcom oluşturduğunu nerden bilcem dökümantasyona yada ui xcomsa bakcam     

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

    choose_best_model = PythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )

    is_accurate=BranchPythonOperator(

        task_id='is_accurate',
        python_callable=_is_accurate
    )

    accurate=DummyOperator(
        task_id='accurate'
        
     
    )
    inaccurate=DummyOperator(
        task_id='inaccurate'
        
     
    )


    downloading_data >> processing_tasks >> choose_best_model
    choose_best_model>>is_accurate>>[accurate,inaccurate]
    # ui da dagi çalıştırdığında is accurate den sonra accurate taski çalışcak ve inaccurate taski skip edilcek

    # çünkü is_accurate taskinde _is_accurate() fonskiyonunda kendinden sonraki task den sonra task idsi accurate olan 
    # taskin çalışmasını  ve diğer taskin skip edilmesini istedim log kısmından takip edebilirsin tasklerin çıktılarını
