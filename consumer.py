import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

def callback(ch, method, properties, body):
    message = body.decode()
    print(f" [x] Received '{message}'")

    # Имитируем выполнение задачи
    time.sleep(message.count(' '))
    print(" [x] Done")

    # Отправляем подтверждение обратно в producer
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id
        ),
        body='Task completed'
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

print(" [*] Waiting for messages. To exit press Ctrl+C")
channel.start_consuming()
