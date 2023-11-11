import pika
from app import get_anime_details

def callback(ch, method, properties, body):
    anime_id = int(body)
    get_anime_details(anime_id)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='retry_queue')
channel.basic_consume(queue='retry_queue', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit, press Ctrl+C')
channel.start_consuming()