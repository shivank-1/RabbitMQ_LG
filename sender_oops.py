import pika

class RabbitMQProducer:
    def __init__(self, queue_name='hello', host='localhost'):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def send_message(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        print(f"[✔] Sent: {message}")

    def close_connection(self):
        self.connection.close()

# Example Usage
if __name__ == "__main__":
    producer = RabbitMQProducer()
    producer.send_message("Hello, RabbitMQ with OOP!")
    producer.close_connection()



##########################################################################
# with a META Class
# Meta Class- In Python, a metaclass is a class of a class. It defines how classes behave. Just as classes define how instances behave, metaclasses define how classes behave.
# Why use a MetaClass? -> Using a metaclass in this RabbitMQ implementation enforces a structured and consistent design pattern.
#                      -> better for Large Scale Systems ->better code maintainability

# Basic Example:
# # Define a simple metaclass
#class Meta(type):
#   def __new__(cls, name, bases, dct):
#        print(f"Creating class: {name}")
#        return super().__new__(cls, name, bases, dct)

# Use the metaclass in a class definition
#class MyClass(metaclass=Meta):
#    pass

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

class RabbitMQProducer(RabbitMQBase):
    """RabbitMQ Producer for sending messages."""

    def connect(self):
        """Establishes connection with RabbitMQ and declares a queue."""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def send_message(self, message):
        """Sends a message to the RabbitMQ queue."""
        if not self.channel:
            raise RuntimeError("RabbitMQ connection not established. Call connect() first.")
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        print(f"[✔] Sent: {message}")

    def close_connection(self):
        """Closes the RabbitMQ connection."""
        if self.connection:
            self.connection.close()
            print("[✔] Connection closed.")

# Example Usage
if __name__ == "__main__":
    producer = RabbitMQProducer()
    producer.connect()
    producer.send_message("Hello, RabbitMQ with Metaclass!")
    producer.close_connection()
