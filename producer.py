import pika
import uuid

class Producer:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def on_response(self, ch, method, properties, body):
        print(f" [x] Received response: {body.decode()}")

    def send_task(self, message):
        correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=correlation_id
            ),
            body=message
        )
        print(f" [x] Sent '{message}'")

        while True:
            self.connection.process_data_events(time_limit=1)

producer = Producer()
producer.send_task("Your task message")
