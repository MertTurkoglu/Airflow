# postgresden elasticsearch e data transferi için kendi operatörümüz
from airflow.models import baseoperator #kendi operaötürümüzü yazmak için BaseOperatör sınıfını inheritance etcez
from airflow.providers.postgres.hooks.postgres import PostgresHook
from elasticsearch.client import Elasticsearch
from elasticsearch_plugin.hooks.elastic_hook import ElasticHook

from contextlib import closing
import json

class PostgresToElasticOperator(baseoperator):

    def __init__(self,sql,index,postgres_conn_id='postgres_default',elastic_conn_id='elasticsearch_default',*args,**kwargs):

        #index paramatresi data nerde depolancak 
        #sql paramatresi datayı elastice atcağımız query
        #postgres_conn_id de varsayılan id m bağlantı için
        super(PostgresToElasticOperator,self).__init__(*args,**kwargs)
        self.sql=sql
        self.index=index
        self.postgres_conn_id=postgres_conn_id
        self.elastic_conn_id=elastic_conn_id
    
    def execute(self,context): 
        #kendimiz bir operatör yazdığımızda bu methodu override etmeliyiz
        # taski yazcağımız ksıım burası ve bu method executor tarafından triggered olcak
        #postgresden elasticsearche data transfer ettiğimiz kısıkm burası
        #ElasticHook ve PostgreHook obejelerine ihtiyacım var
        es=ElasticHook(conn_id=self.elastic_conn_id)
        pg=PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # postgres ile bağlantı sağlamak için ve bağlantıyı sonlandırma ile uğraşmamak için closing
        with closing(pg.get.conn()) as conn:
            with closing(conn.cursor()) as cur:
                # cursor da postgres ile etkileşim kurmak için
                cur.itersize=1000 # ne kadar data transfer etcez
                cur.execute(self.sql)
                for row in cur:
                    doc = json.dumps(row,indent=2)
                    es.add_doc(index=self.index,doc_type='external',doc=doc)



# çalıştırmadan önce postgres connectionu oluşturmalıyız ui dan
# daha önceden postgres connectionumum default olarak var bunu silip kendimiz bi tane oluşturalım
# connection sekmesinde extra ya yazdığımız {"cursor":"realdictcursor"} rowları json formatında transfer edebilmemiz için
# şimdi postgres  için şifre oluşturmak lazım
#sudo -u postgres psql diyip ALTER USER postgres PASSWORD 'postgres';
#Select * from connections diyince connectionları görüyoz bu çıktıları göndercez
# biz ne datasını atmak istiyoruz postgres den elasticsearch e
#airflowun  metadatabase indeki connectionları elastic searche atcaz
# python vm deyken -X GET "http://localhost:9200/connections/_search" -H "Content-type: application/json" -d '{"query":{"match_all":{}}}'
# datayı elastic searche gönderceğimiz index mevcut mu ve o indexte data var mı yok mu diye kontrol ediyoruz


