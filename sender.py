import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) # Creates a connection to the RabbitMQ server running on localhost
channel = connection.channel() # creates a channel for communication

channel.queue_declare(queue='hello') # declares or creates a new queue

channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello, RabbitMQ!')   # sends a message to the queue 'hello'
                                                 # exchange='' means it is using default direct queue

print(" [x] Sent 'Hello, RabbitMQ!'")
connection.close()
