# Le o arquivo json e envia para o serviço de mensageria

import datetime
import pika
import json
import os

rabbitmq_host = os.environ['RABBIT_SVC_SERVICE_HOST']
redis_host = os.environ['REDIS_SVC_SERVICE_HOST']
redis_port = os.environ['REDIS_SVC_SERVICE_PORT']
minio_host = os.environ['MINIO_SVC_SERVICE_HOST']
minio_port = os.environ['MINIO_SVC_SERVICE_PORT']
minio_endpoint = minio_host+":"+minio_port

print("conectando no rabbitmq...", rabbitmq_host)

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host = rabbitmq_host
))

channel = connection.channel()

print("abrindo arquivo de transcações...")

transaction_file = open("transaction.json")

print("carregando transações...")

transactions = json.load(transaction_file)

transaction_file.close()

for transaction in transactions:
    transaction["data"] = str(datetime.datetime.now())
    channel.basic_publish(exchange="amq.fanout",
                          routing_key="",
                          body=json.dumps(transaction)
                          )

# verifica o cache, calcula a media, gera relatório no Min-IO quando encontra a condição de fraude e depois mostra os endereços para download dos relatórios

from minio import Minio
import redis
import io
import os

print("conectando no minio...", minio_endpoint)

cliente = Minio(
    endpoint=minio_endpoint,
    access_key="minioadmin", 
    secret_key="minioadmin",
    secure=False)

print("criando bucket se não existir...")

bucket_name = "relatorios"
if cliente.bucket_exists(bucket_name):
    print("Bucket existe!")
else:
    cliente.make_bucket(bucket_name)

print("conectando ao redis...")

cache = redis.Redis(host=redis_host, port=redis_port, db=0)

print("pesquisando por chave..")

chaves = cache.keys("report*")

for chave in chaves:
    str_chave = chave.decode("utf-8")
    str_chave = str_chave+".txt"
    reports = cache.lrange(chave, 0, 999999)
    value=""
    size=0
    for report in reports:
        str_report=report.decode("utf-8")
        value=value+str_report+"\n"

    size=len(value)
    value_as_bytes=value.encode("utf-8")
    str_reports=io.BytesIO(value_as_bytes)

    result = cliente.put_object(
        bucket_name=bucket_name,
        object_name=str_chave,
        data=str_reports,
        length=size
    )

    print("mostrando o endereço dos relatorios...")

#    get_url = cliente.get_presigned_url(
#        method='GET',
#        bucket_name=bucket_name,
#        object_name= str_chave, )
    get_url = cliente.presigned_get_object(bucket_name, str_chave, expires=datetime.timedelta(days=1), response_headers=None, request_date=None, version_id=None, extra_query_params=None)
    print(f"Download URL: {get_url}")
