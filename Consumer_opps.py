import pika

# Singleton Metaclas
class MetaClass(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """ Ensures only one instance of RabbitMQ is created. """
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
        return cls._instance[cls]

# Configuration Class
class RabbitmqConfigure:
    """ Configure my Rabbit Mq Server"""
    def __init__(self, queue='hello', host='localhost', routingKey='hello', exchange=''):
        """ Stores RabbitMQ configuration details. """
        self.queue = queue
        self.host = host
        self.routingKey = routingKey
        self.exchange = exchange

# RabbitMQ Consumer Class
class RabbitMqConsumer(metaclass=MetaClass):
    def __init__(self, server: RabbitmqConfigure):
        """ Initializes RabbitMQ connection and declares queue. """
        self.server = server
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.server.queue)

    def _callback(self, ch, method, properties, body):
        """ Processes received messages. """
        print(f"Received Message: {body.decode()}")

    def consume(self):
        """ Starts consuming messages from the queue. """
        print("Waiting for messages. To exit, press CTRL+C")
        self._channel.basic_consume(queue=self.server.queue, on_message_callback=self._callback, auto_ack=True)
        self._channel.start_consuming()

# Main Execution
if __name__ == "__main__":
    server = RabbitmqConfigure(queue='hello', host='localhost', routingKey='hello', exchange='')
    rabbitmq_consumer = RabbitMqConsumer(server)
    
    # Start consuming messages
    rabbitmq_consumer.consume()
