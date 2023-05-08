import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG("variable", default_args=default_args, schedule_interval=timedelta(1),catchup=False)

t1 = BashOperator(task_id="print_path", bash_command="echo {{var.value.source_path}}", dag=dag)


#değişkenleri key value formatında depolamanın ve almanın bir yoludur
# variable ları ui dan oluşturabilir listeleyebilir ve güncelleyebilir ve silebiliriz
#airflowun metadatabase inde depolanır
# tanımladığın bir variable tüm dagler için geçerli olur
# dag.py de yazdığın bir değişken sadece o python dosyasında geçerli olurken airflow un variable ları tüm daglerde geçerli olur
# neden kullanmaya ihtiyaç duyarız

# diyelimki bir klasörün var ve bu klasörü bir çok dag ,task kullanıyor birden bu klasörü değiştirmen gerekti diyelim 
# ozamn bu klasörü kullandığın tüm daglerde de değiştirmek gerekir 
# işte bu ksımda variable kullanabiliriz

# bu dag da kullandığım variable ı ui dan oluşturdum key i source_path value su /usr/local/airflow

# /usr/local/airflow bu path değişti diyelim tüm daglerde tek tek değiştirmeye gerek yok ui da bu variable ı güncelleyebiliriz