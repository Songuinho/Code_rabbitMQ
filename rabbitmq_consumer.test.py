import logging
from kombu import Connection, Exchange, Queue

# Configuration de RabbitMQ
rabbitmq_url = "amqp://espoir:songuinho@localhost:5672//"
queue_name = "message_queue"
exchange_name = "message_exchange"
routing_key = "message_routing_key"

# Configuration du journal
logging.basicConfig(filename='rabbitmq_consumer.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(message)s')

def process_message(body, message):
    try:
        # Afficher les données dans les logs
        logging.info(f"Message reçu : {body}")
        print(f"Message reçu : {body}")

        # Marquer le message comme traité dans RabbitMQ
        message.ack()

    except Exception as e:
        logging.error(f"Erreur lors du traitement du message: {e}")
        # Rejeter le message pour qu'il puisse être traité à nouveau
        message.requeue()

def consume_from_rabbitmq():
    try:
        with Connection(rabbitmq_url) as conn:
            exchange = Exchange(exchange_name, type='direct')
            queue = Queue(queue_name, exchange, routing_key=routing_key)

            def callback(body, message):
                process_message(body, message)

            with conn.Consumer(queue, callbacks=[callback]):
                logging.info('En attente de messages depuis RabbitMQ...')
                print('En attente de messages depuis RabbitMQ...')

                while True:
                    conn.drain_events()

    except Exception as e:
        logging.error(f"Erreur de connexion à RabbitMQ: {e}")
        print(f"Erreur de connexion à RabbitMQ: {e}")

if __name__ == "__main__":
    consume_from_rabbitmq()
