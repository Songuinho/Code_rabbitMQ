import mysql.connector
import logging
import json
from datetime import date, datetime
from decimal import Decimal
from kombu import Connection, Exchange, Queue

# Configuration du journal
logging.basicConfig(filename='errors.log', level=logging.DEBUG, 
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

def json_serial(obj):
    """Serialize date, datetime and decimal objects to JSON-compatible format."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def send_to_rabbitmq(data):
    try:
        logging.debug("Connecting to RabbitMQ")
        with Connection(rabbitmq_url) as conn:
            exchange = Exchange(exchange_name, type='direct')
            queue = Queue(queue_name, exchange, routing_key=routing_key)
            producer = conn.Producer(serializer='json')

            # Déclarez la file d'attente avant d'envoyer des messages
            queue.maybe_bind(conn)
            queue.declare()

            # Convertir les données en JSON avec serialization personnalisée
            json_data = json.dumps(data, default=json_serial, ensure_ascii=False)

            producer.publish(
                json_data,
                exchange=exchange,
                routing_key=routing_key,
                declare=[queue]
            )
            logging.debug(f"Message sent to RabbitMQ: {json_data}")
    except Exception as e:
        logging.error(f"Error sending to RabbitMQ: {e}")
        raise  # Relancer l'exception pour la gérer dans fetch_and_process_messages

def fetch_and_process_messages():
    try:
        logging.debug("Connecting to MySQL")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        logging.debug("Connected to MySQL")

        try:
            logging.debug("Fetching messages from MySQL")
            query = """
                SELECT 
                    mq.*,
                    (SELECT GROUP_CONCAT(DISTINCT CONCAT(na.phone, ',', na.transactionNumber) SEPARATOR ', ')
                        FROM networks_agents na
                        WHERE na.agent_id = mq.agent_id) AS phone_numbers
                FROM 
                    message_queue mq
                WHERE 
                    mq.processed = 0
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                try:
                    taxes_query = """
                        SELECT 
                            nt.name,
                            nt.measurement_unit,
                            nt.unit_price,
                            nt.billing_period
                        FROM 
                            networks_taxes nt
                        JOIN 
                            networks_agents_taxes nat ON nt.id = nat.id_network_tax
                        JOIN 
                            networks_agents na ON nat.id_network_agent = na.id
                        WHERE 
                            na.agent_id = %s
                    """
                    cursor.execute(taxes_query, (row['agent_id'],))
                    taxes = cursor.fetchall()
                    
                    distinct_names = set()
                    distinct_measurement_units = set()
                    distinct_unit_prices = set()
                    distinct_billing_periods = set()

                    for tax in taxes:
                        distinct_names.add(tax['name'])
                        distinct_measurement_units.add(tax['measurement_unit'])
                        distinct_unit_prices.add(tax['unit_price'])
                        distinct_billing_periods.add(tax['billing_period'])

                    row['name'] = list(distinct_names)
                    row['measurement_unit'] = list(distinct_measurement_units)
                    row['unit_price'] = list(distinct_unit_prices)
                    row['billing_period'] = list(distinct_billing_periods)
                    row['phone_numbers'] = row.get('phone_numbers', '')

                    logging.debug(f"Processing row: {json.dumps(row, default=json_serial, ensure_ascii=False, indent=4)}")
                    send_to_rabbitmq(row)
                    
                    cursor.execute("UPDATE message_queue SET processed = 1 WHERE agent_id = %s", (row['agent_id'],))
                    conn.commit()
                    logging.debug(f"Message marked as processed: {row['agent_id']}")

                except Exception as e:
                    logging.error(f"Error processing row with agent_id {row['agent_id']}: {e}")
                    cursor.execute("UPDATE message_queue SET processed = 0 WHERE agent_id = %s", (row['agent_id'],))
                    conn.commit()
                    logging.debug(f"Message marked as not processed due to error: {row['agent_id']}")

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
