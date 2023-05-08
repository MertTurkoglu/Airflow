from warnings import resetwarnings
from airflow.models import DAG
from datetime import  datetime
from airflow.providers.postgres.operators import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
from pandas import json_normalize
from airflow.operators.bash import BashOperator
default_args = {

    'start_date' : datetime(2020, 1, 1)
}

def _processing_user(ti):#ti paramatresi olarak gönderceğim değer task id oluyo bu task id ile xcom_pull()methodunu kullanabiliyorum
    #taskler arası data alışverişi için xcom_pull methodunu kullanabiliyoruz
    users = ti.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('user is empty')
    user =users[0]['results'][0]
    processed_user =json_normalize({
        'firstname':user['name']['first'],
        'lastname':user['name']['last'],
        'country':user['location']['country'],
        'username':user['login']['username'],
        'password':user['login']['password'],
        'email':user['email']
    })# processede_user değişkeni artık bir pandas df bunu csv ye çevirip istediğim konuma kaydedebilirimc
    processed_user.to_csv('/tmp/processed_user.csv',index=False,header=False)
    #satır numarasını da göstercek index false ile göstermesini istemiyom

# dag tanımı
with DAG('user_processing',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False ) as dag:
    # https://airflow.apache.org/docs/apache-airflow-providers/packages-ref.html şu adresten gerekli operatörleri import etmek
    # için örneğin sqliteOperatorü için python environmentinden pip install 'apache-airflow-providers-sqlite'
    # ilk task tablo oluşturma sqliteoperatörü ile
    # ui admin connectiondan conneciton oluşturcaz sqlite_conn_id değeri ile aynı olmalı connectiondaki conn_ıd ile 
    # ve airflowda db nin konumunu vercen host da
    creating_table = PostgresOperator(
        task_id='creating_table',
        sqlite_conn_id='postgres_conn',
        sql='''
             CREATE TABLE IF NOT EXISTS users (
                 firstname TEXT NOT NULL,
                 lastname TEXT NOT NULL,
                 country TEXT NOT NULL,
                 username TEXT NOT NULL,
                 password TEXT NOT NULL,
                 email TEXT  NOT NULL PRIMARY KEY

             );
             '''
    
             
    )
    #sensor operatörünü kullancaz bir sonraki task e geçmeden önce birşeyin olmasını bekler api ye ulaşıp ulaşamadığımızı kontrol etcez
    # aynı şekilde bir conneciton daha datayı url den almak için host kısmına datayı alcağın url i yazcan conn Type HTTP olcak
    # öncesinde pip install 'apache-airflow-providers-http'
    is_api_available=HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'

    )
    extracting_user=SimpleHttpOperator(

        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',#url e herhangi bir data göndermicez
        response_filter=lambda response :json.loads(response.text),#datayı json formatında isiyoruz
        log_response=True
        # test ettikten sonra location street gender name country coordinates registered gibi bir çok değişken var ama
        #bizim oluşturduğumuz tablodaki kolonlar belli o yüzden bir sonraki taskte bu json formatında gelen veriyi tablomuzda
        #kaydedecek şekilde process etmemiz gerekiyor
    )

    # providerı install etmeye gerek yok çünkü varsayılan olarak PythonOperatorü yüklü geliyor zaten
    processing_user=PythonOperator(

        task_id='processing_user',
        python_callable=_processing_user # çağırcağı python fonskiyonun adı

    )
    # şimdi oluşturduğum csv dosyasındaki dataları tabloya insert etmek için bir task oluşturcaz
    #bunun için bash operatörünü kullancaz bunuda install etmeye gerek yok
    #import ederken airflow.providers diyorsak o providers ı install etmemiz gerekir

    storing_user=BashOperator(

        task_id='storing_user',
        bash_command='echo -e ".separator ","\n.import /tmp/processed_user.csv users" | sqlite3 /home/airflow/airflow/airflow.db'
        #echo komutu dosya oluşturmak veya var olan dosyaya data yazmak için kullandığımız bir komut
        # airflow metastore una erişmek için sqlite3 airflow.db

    )
    # tüm taskler bitti ama ui dan graph view a baktığında bir taskler arasında bir bağlılık yok hangi task önce çalışcak sonra hangisi
    #sonra çalışcak tasklerin çalışma sırası belli değil

    creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user

    # şimdi taskler arası bağlılığı oluşturduk
    # şimdi sıra dagi çalıştırmada  ui da dage gidip pause unpause tuşuna basıyoruz şimdi creating table taskinde hata var log
    #kısmına gidiyoruz hata user tablosunun zaten var olduğunu söylüyo çözüm ıf table not exist
    #tüm taskler success olduktan sonra dagi pause edebiliriiz
    #peki birçok dagi birlikte çalıştırmak istersek yada bir taski çalıştırıp belirli bir şarta göre diğer task i çalıştırmak istersek
    #yada belirli zamanlarda çalıştırmak istiyorsam dagi işte bunun için Dag scheduling
    # dag i tanımlarken iki paramatreyi kesin olarak vermemiz gerekiyor start_date ne zaman daginin başlayacağı 2020-01-01 10 am
    # schedule interval de sıklığı belirtiyor atıyorum 10 dakkada bir
    #start date + schedule interval = elapsed bu sürede benim triggerım effective bir şekilde oldu yani saat 10 a 10 geçe her 10 dakkada bir trigger oluşcak demek   
    # start date + shedule interval - shedule interval execution date e eşit

    # peki dag in hata aldığını varsayalım hatayı çöz tekrar başlat bu süreçte ne yapıcaz biz data pipeline ı durdurduğumuzda ne olur 
    # tekrardan trigger ettiğimizde ne olur
    #airflow bu durumlarda otomotik olarak ne yapıyor 
    # backfiiling and catch up geri doldurma ve yakalama

    # bu iki kavramı açıklamak için örnek
    # DAG A adında dagin olsun start date 2020-01-01 shcedele intervald da @daily olsun
    # bu dagi başlattın diyelim dag run 1 oluşcak 01-01 de oluşan dag run ları ui browse dag runs da görebilirsin
    # sonrası gün dag run 2  oluşcak 02-01 de
    # sonrası gün dag run 3 oluşcak 03-01 de bu böyle devam etcek taki sen dagi pause yada stop edene kadar
    # ben 2.dagrun da pause ettim diyelim ve ocağın 5 inde tekrardan başlattım diyelim aradaki dag run 3 ve 4 e nolcak bunlar çalışıp bitcek mi
    # yani triggered olcak mı catchup paramatresi varsayılan olarak true olcağı için evet olcak eğer false olsaydı 
    # dag run 3 ve 4 triggered olmucaktı yani çalışmıcaktı . ocağın 5 inde başlatcağım için dag run 5 oluşcaktı aradaki iki günün dag runu oluşmucak
    # True ise start date den çalıştırdığım güne kadar olan tüm dog run ları da oluşturur
    # false ise sadece çalıştırdığım gününkünü oluşturur ve durdurana kadar devam eder
    # start date 2020-01-01 olsun time interval daily olsun ben bu dagi ilk defa  25 ocakta çalıştırırsam dag run 1 tane olcak oda dag run 25 ama
    # catch up false iken ama true olsaydı ocağın 1 inden 25 ine kadar olan tüm günleride çalıştırcağından 25 tane dag run olcaktı



    




    
    
    

