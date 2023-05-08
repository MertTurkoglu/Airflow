#xcom mantığını anlama dagi


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti): # task instance object
    accuracy = uniform(0.1, 10.0)
     #0.1 ile 10.0 arasında double bir değer döndürür
    print(f'model\'s accuracy: {accuracy}')
    #return accuracy # accuracy değerini airflow db sine atttığımız ksım burası yani xcomun oluştuğu kısım
    #return dediğimizde otomatik olarak airflowun db sine push ediyor
    #xcom oluşturuyor yani varsayılan olarak keyi return_value adında peki kendimiz keye özel bir isim verebilirmiyiz
    #xcom.push() methoduyla hem özel key hemde value verebiliriiz """
    ti.xcom_push(key='model_accuracy',value=accuracy)
    # keyi model_accuracy adında bir xcom oluşcak ve accuracy değerini airflowun db sine push etcek yani eklicek
    # return accuracy dediğimizde de xcom oluşmuştu accuracy değerini airflow db sine göndermişti ama
    #  key return_value adındaydı xcom_push methoduyla istediğimiz key ve value değerini kendimiz verebilriiz




def _choose_best_model(ti):
    print('choose best model')
    accuracies = ti.xcom_pull(key='model_accuracy',
    task_ids = [
        'processing_tasks.training_model_a',
        'processing_tasks.training_model_b',
        'processing_tasks.training_model_c'
    ])
    # accuracies değikenim benim bir dizi  [None, 6.039830149981163, 6.232519290956121]
    print(accuracies)# datayı db den çektiğini kesinleştirmek için görüntüleyelim
    #xcom pull ile de yukarıdaki fonksiyonda airflowun db sine gönderilen accuracy değerini alıyoruz
    # paramatre olarak xcomun keyi ve hangi tasklerden dönen value ları
    # task id yi girerken taskgroup oldukları için taskler, tasklerin idleri  Taskgroupid.taskid oluyor ui xcom da da görebilirsin
    #ui da choose_best_model taskine tıklayıp log diyip accuracy değerlerin adlığını görüntüleebilrsin
    #tüm printler bu ksımda



with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

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


    downloading_data >> processing_tasks >> choose_best_model

    # programın amacı xcomu kullanma mantığı accuracy değeri hangi task de daha iyi ise o değere göre çalışcak demek
    #fonksiyonlar temsili yani asıl işini yapmıyo yani downloading data taski gerçektende data indirmiyo 
    # sadece xcomun çalışma mantığını öğrenmek için
    #_training_model fonskiyounundan bir değer dönüyo bu fonskiyonu 3 task kullanıyo sonrasında
    # choose_best_model taski bu accuracy değeri üreten processing_tasks adlı taskgrubundan accuracy değerini alıyo ve accuracy
    # değeri en iyi olan değere göre taski çalıştırıyor
    #xcom taskler arası data alışverişi sağlar burada processing_task adlı taskten choose_best_model texti accuracy değerini alıyor
    # burdaki uygulama şu şekilde accuracy değeri airflowun db sine push ediliyor ben postresql kullanıyorum db olarak
    #sonrada choose_best_model taski bu değeri  pull ediyor yani çekiyor
    #xcom verisi airflowun db isnde tutulur 
    # eğer airflow db si sqlite ise 2Gb a kadar xcom verisi tutmaya imkan tanır
    # eğer postresql ise 1GB
    # eğer mysql ise 64KB
    #programı çalıştırdığımızda ve dag run oluştuğunda yani dagimiz çalışmayı bitirince yani triggered olduğunda
    #xcomslar oluşcak interface de admin xcoms diyip görüntüleyebiliriz ama öncesinde parallel_dag idli dag içinde xcomslar oluşmuş
    #bunları silelim
    # xcoms kısmında key var  xcomun keyi bu örneğin db den data çekmek için bu keyi kullancam varsayılan olarak hep return_value adında hep
    #peki özel bir isim verebilirmiyiz
    # value datası json olmalı burdaki value accuracy rneğin 1.434376585
    #timestamp xcomun oluşturulduğu zman