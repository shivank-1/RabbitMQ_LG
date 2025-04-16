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

# Singleton Metaclass
class MetaClass(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """ Ensures only one instance of RabbitMQ is created. """
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
        return cls._instance[cls]

# Configuring the our RabbitMQ Server
class RabbitmqConfigure:    
         
    # self refers to the instance of the class
    def __init__(self, queue='hello', host='localhost', routingKey='hello', exchange=''):
        """ Stores RabbitMQ configuration details. """
        self.queue = queue    ## assigning value to an instance attribute. (instance attribute-> is a variable belonging to the object of the class)
        self.host = host
        self.routingKey = routingKey
        self.exchange = exchange

# RabbitMQ Producer Class, here we will establish connection, channel and publish messages
class RabbitMq(metaclass=MetaClass):
    #passing the configured RabbitMQ Server as parameter here
    def __init__(self, server: RabbitmqConfigure): # 
        """ Initializes RabbitMQ connection and declares queue. """
        self.server = server
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.server.queue)

    def publish(self, message: str):
        """ Publishes message to the queue. """
        self._channel.basic_publish(
            exchange=self.server.exchange,  ## refering to the exchange stored in self.server
            routing_key=self.server.routingKey,
            body=message
        )
        print(f"Published Message: {message}")
        self._connection.close()

# Main Execution    
if __name__ == "__main__":

    ## Calling a 'class' with its parameters creates a new object (or instance) of that class.
    # REMEMBER here we are not calling the functions of above class but instead the class itself with the parameters of __init__ function
    # init paramters are called class parameter
#V.Imp  ## Therefore 'obec creation' of classes is taking place here## 

    # 1ST server ko configure karke phir next class mei as a parameter bhej diya
    server = RabbitmqConfigure(queue='hello', host='localhost', routingKey='hello', exchange='') 
    rabbitmq = RabbitMq(server)
    
    # Send message/ Calling the function of RabbitMq Class
    rabbitmq.publish("Hello, this is a test message!")



