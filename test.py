import mysql.connector
import logging
from kombu import Connection, Exchange, Queue
import time

# Configuration du journal
logging.basicConfig(filename='script.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(message)s')

# Configuration de RabbitMQ
rabbitmq_url = "amqp://espoir:songuinho@localhost:5672//"
exchange_name = "message_exchange"
queue_name = "message_queue"
routing_key = "message_routing_key"

# Configuration de la base de données MySQL
db_config = {
    'user': 'root',
    'password': 'songuinho',
    'host': 'localhost',
    'database': 'cud'
}

def send_to_rabbitmq(data):
    try:
        logging.debug("Connecting to RabbitMQ")
        with Connection(rabbitmq_url) as conn:
            logging.debug("Connected to RabbitMQ")
            exchange = Exchange(exchange_name, type='direct')
            queue = Queue(queue_name, exchange, routing_key=routing_key)
            producer = conn.Producer(serializer='json')

            logging.debug("Binding and declaring the queue")
            queue.maybe_bind(conn)
            queue.declare()

            logging.debug("Publishing message")
            producer.publish(
                data,
                exchange=exchange,
                routing_key=routing_key,
                declare=[queue]
            )
            logging.debug(f"Message sent to RabbitMQ: {data}")
    except Exception as e:
        logging.error(f"Error sending to RabbitMQ: {e}")
        raise

def fetch_and_process_messages():
    try:
        logging.debug("Connecting to MySQL")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        logging.debug("Connected to MySQL")

        while True:
            try:
                logging.debug("Fetching messages from MySQL")
                query = """
                    SELECT 
                        mq.*,
                        (SELECT GROUP_CONCAT(n.name SEPARATOR ', ') 
                            FROM networks n
                            INNER JOIN networks_agents na ON n.id = na.network_id
                            WHERE na.agent_id = mq.id) AS network_name,
                        (SELECT GROUP_CONCAT(CONCAT(na.phone, ',', na.transactionNumber) SEPARATOR ', ')
                            FROM networks_agents na
                            WHERE na.agent_id = mq.id) AS phone_numbers
                    FROM 
                        message_queue mq
                    WHERE 
                        processed = 0
                """
                cursor.execute(query)
                rows = cursor.fetchall()

                for row in rows:
                    row['network_name'] = row.get('network_name', '')
                    row['phone_numbers'] = row.get('phone_numbers', '')
                    logging.debug(f"Processing row: {row}")
                    send_to_rabbitmq(row)
                    cursor.execute("UPDATE message_queue SET processed = 1 WHERE id = %s", (row['id'],))
                    conn.commit()
                    logging.debug(f"Message marked as processed: {row['id']}")

                time.sleep(5)  # Attendre 5 secondes avant de vérifier à nouveau
            except Exception as e:
                logging.error(f"Error processing messages: {e}")
                raise  # Relancer l'exception pour sortir de la boucle

    except Exception as e:
        logging.error(f"Error fetching and processing messages: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logging.debug("MySQL connection closed")

if __name__ == "__main__":
    fetch_and_process_messages()
