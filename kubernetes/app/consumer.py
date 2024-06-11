import pika
import json
import redis
import os

rabbitmq_host = os.environ['RABBIT_SVC_SERVICE_HOST']
redis_host = os.environ['REDIS_SVC_SERVICE_HOST']
redis_port = os.environ['REDIS_SVC_SERVICE_PORT']

print("rabbitmq_host=", rabbitmq_host, "redis_host=", redis_host)

print("conectando ao rabbitmq")

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))

print("conectando ao canal")

channel = connection.channel()

print("criando fila")

queue_name = "fraude_queue"

channel.queue_declare(queue="fraude_queue")
channel.queue_bind(exchange="amq.fanout", queue="fraude_queue")

print("conectando ao redis")

cache = redis.Redis(host=redis_host, port=redis_port, db=0)

def chamado_quando_uma_transacao_eh_consumida(channel, method_frame, header_frame, body):
    transaction = json.loads(body.decode('utf-8'))
    chave = transaction["conta"]
    fraude = 0
    media_lida = cache.lindex(chave, 0)
    if(media_lida==None):
        media = transaction["value"]
        cache.rpush(chave, media)
    else:
        desvio = transaction["value"] / float(media_lida)
        if(desvio > 1.4):
            print("Fraude: ", transaction)
            fraude = 1
            cache.rpush("report-"+str(chave), "Fraude: "+json.dumps(transaction))
        soma = 0
        contador = 0
        res = cache.lrange(chave, 1, 9999)
        for x in res:
            y = json.loads(x)
            if(fraude==1):
                cache.rpush("report-"+str(chave), "Histórico: "+str(x))
            contador = contador + 1
            soma = soma + y["value"]
        contador = contador + 1
        soma = soma + transaction["value"]
        media = soma / contador
        cache.lset(chave, 0, media)

    cache.rpush(chave, json.dumps(transaction))
    
channel.basic_consume(queue=queue_name,
                      on_message_callback=chamado_quando_uma_transacao_eh_consumida, auto_ack=True)

print("Esperando por mensagens. Para sair pressione CTRL+C")
channel.start_consuming()
