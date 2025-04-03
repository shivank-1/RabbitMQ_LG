import pika

class RabbitMQConsumer:
    def __init__(self, queue_name='hello', host='localhost'):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def callback(self, ch, method, properties, body):
        print(f"[✔] Received: {body.decode()}")

    def start_consuming(self):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        print("[*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

# Example Usage
if __name__ == "__main__":
    consumer = RabbitMQConsumer()
    consumer.start_consuming()


########################################################################################

#with Meta class

import pika
from abc import ABC, ABCMeta, abstractmethod

class RabbitMQMeta(ABCMeta, type):
    """Metaclass to enforce structure in RabbitMQ-based classes."""
    
    def __new__(cls, name, bases, dct):
        if 'connect' not in dct or not callable(dct['connect']):
            raise TypeError(f"Class {name} must implement a 'connect' method")
        if 'close_connection' not in dct or not callable(dct['close_connection']):
            raise TypeError(f"Class {name} must implement a 'close_connection' method")
        return super().__new__(cls, name, bases, dct)

class RabbitMQBase(ABC, metaclass=RabbitMQMeta):
    """Base class for RabbitMQ connections using the metaclass."""
    
    def __init__(self, queue_name='hello', host='localhost'):
        self.queue_name = queue_name
        self.host = host
        self.connection = None
        self.channel = None

    @abstractmethod
    def connect(self):
        """Method to establish a connection with RabbitMQ."""
        pass

    @abstractmethod
    def close_connection(self):
        """Method to close the RabbitMQ connection."""
        pass

class RabbitMQConsumer(RabbitMQBase):
    """RabbitMQ Consumer for receiving messages."""

    def connect(self):
        """Establishes connection with RabbitMQ and declares a queue."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def callback(self, ch, method, properties, body):
        """Callback function to process received messages."""
        print(f"[✔] Received: {body.decode()}")

    def start_consuming(self):
        """Starts consuming messages from the RabbitMQ queue."""
        if not self.channel:
            raise RuntimeError("RabbitMQ connection not established. Call connect() first.")
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        print("[*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def close_connection(self):
        """Closes the RabbitMQ connection."""
        if self.connection:
            self.connection.close()
            print("[✔] Connection closed.")

# Example Usage
if __name__ == "__main__":
    consumer = RabbitMQConsumer()
    consumer.connect()
    consumer.start_consuming()
