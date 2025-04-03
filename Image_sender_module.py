import pika
import base64  # Encoding image as Base64

# Singleton Metaclass
class MetaClass(type):
    _instance = {}

    def __call__(cls, *args, **kwargs):
        """ Ensures only one instance of RabbitMQ is created. """
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args, **kwargs)
        return cls._instance[cls]

# Configuring the RabbitMQ Server
class RabbitmqConfigure:    
    def __init__(self, queue='image_queue', host='localhost', routingKey='image_queue', exchange=''):
        """ Stores RabbitMQ configuration details. """
        self.queue = queue    
        self.host = host
        self.routingKey = routingKey
        self.exchange = exchange

# RabbitMQ Producer Class
class RabbitMq(metaclass=MetaClass):
    __slots__ = ["server", "_channel", "_connection"]  

    def __init__(self, server: RabbitmqConfigure):  
        """ Initializes RabbitMQ connection and declares queue. """
        self.server = server
        self._connect()  # Initial connection setup

    def _connect(self):
        """ Establishes a new RabbitMQ connection and channel. """
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.server.queue)

    def __enter__(self):
        """ Ensures connection is open when entering the context. """
        print("__enter__: Connection Opened")
        if not self._connection.is_open:
            self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Closes connection when exiting the context. """
        print("__exit__: Connection Closed")
        if self._connection.is_open:
            self._connection.close()

    def publish(self, message: bytes):
        """ Ensures the connection is open before publishing. """
        if not self._connection.is_open or not self._channel.is_open:
            print("🔄 Reconnecting...")
            self._connect()  # Reconnect if needed

        self._channel.basic_publish(
            exchange=self.server.exchange,  
            routing_key=self.server.routingKey,
            body=message
        )
        print("✅ Image Sent to Consumer!")

# Function to read and encode the image
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read())  # Convert to Base64

# Main Execution    
if __name__ == "__main__":
    # Configure the RabbitMQ server
    server = RabbitmqConfigure(queue='image_queue', host='localhost', routingKey='image_queue', exchange='') 
        
    # Encode Image
    image_path = "D:\RabbitMQ\download.png"  # Change this to your image file
    encoded_image = encode_image(image_path)
    
    # Using context manager (with statement)
    with RabbitMq(server) as rabbitmq:
        rabbitmq.publish(encoded_image)
    
    # Publish again outside the context manager
    rabbitmq.publish(encoded_image)
