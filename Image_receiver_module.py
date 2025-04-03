import pika
import os
import base64
from datetime import datetime

# Singleton Metaclass
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
    def __init__(self, queue='image_queue', host='localhost', routingKey='image_queue', exchange=''):
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
        
        # Create a directory to store received images if it doesn't exist
        self.image_dir = "received_images"
        os.makedirs(self.image_dir, exist_ok=True)
        print(f"Connected to RabbitMQ at {self.server.host} and declared queue '{self.server.queue}'")

    def _callback(self, ch, method, properties, body):
        """ Processes received Base64-encoded images. """
        try:
            print("📥 Received Image from Producer")
            
            # Decode Base64 to original binary
            image_data = base64.b64decode(body)
            
            # Generate a unique filename using timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{self.image_dir}/image_{timestamp}.png"
            
            # Save the binary data as a PNG file
            with open(filename, 'wb') as img_file:
                img_file.write(image_data)
                
            print(f"✅ Image Saved Successfully as: {filename}")
            
        except Exception as e:
            print(f"Error processing received image: {e}")

    def consume(self):
        """ Starts consuming messages from the queue. """
        print(f"🚀 Waiting for images on queue '{self.server.queue}'... To exit, press CTRL+C")
        self._channel.basic_consume(queue=self.server.queue, on_message_callback=self._callback, auto_ack=True)
        self._channel.start_consuming()

# Main Execution
if __name__ == "__main__":
    server = RabbitmqConfigure(queue='image_queue', host='localhost', routingKey='image_queue', exchange='')
    rabbitmq_consumer = RabbitMqConsumer(server)
    
    try:
        # Start consuming messages
        rabbitmq_consumer.consume()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")