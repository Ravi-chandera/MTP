from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# from scripts.check_mongodb import check_mongodb_main
# from scripts.kafka_producer import kafka_producer_main
# from scripts.check_cassandra import check_cassandra_main
# from scripts.kafka_create_topic import kafka_create_topic_main
# from scripts.kafka_consumer_mongodb import kafka_consumer_mongodb_main
# from scripts.kafka_consumer_cassandra import kafka_consumer_cassandra_main


"""check_mongodb.py"""
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_mongodb_main():
    mongodb_uri = 'mongodb://root:root@mongo:27017/'
    database_name = 'email_database'
    collection_name = 'email_collection'

    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    sample_email = 'sample_email@my_email.com'

    result = collection.find_one({'email': sample_email})
    data_dict = {}

    if result:
        logger.info(f"Data found for email: {result['email']}")
        logger.info(f"OTP: {result['otp']}")
        
        data_dict['email'] = result.get('email')
        data_dict['otp'] = result.get('otp')
        
        client.close()
    else:
        data_dict['email'] = ''
        data_dict['otp'] = ''

        client.close()
    return data_dict


"""*********************************************************************************************************************"""

"""kafka_producer.py"""
import logging
from confluent_kafka import Producer
import time

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers):
        """
        Initializes the Kafka producer with the given bootstrap servers.
        """
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers
        }
        self.producer = Producer(self.producer_config)

    def produce_message(self, topic, key, value):
        """
        Produces a message to the specified Kafka topic with the given key and value.
        """
        self.producer.produce(topic, key=key, value=value)
        self.producer.flush()

def kafka_producer_main():
    bootstrap_servers = 'kafka1:19092,kafka2:19093,kafka3:19094'
    kafka_producer = KafkaProducerWrapper(bootstrap_servers)
    
    topic = "email_topic"
    key = "sample_email@my_email.com"
    value = "1234567"
    
    start_time = time.time()
    
    try:
        while True:
            kafka_producer.produce_message(topic, key, value)
            logger.info("Produced message")
            
            elapsed_time = time.time() - start_time
            if elapsed_time >= 20:  # Stop after 20 seconds
                break
            
            time.sleep(5)  # Sleep for 5 seconds between producing messages
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Stopping producer.")
    finally:
        kafka_producer.producer.flush()
        logger.info("Producer flushed.")

# if __name__ == "__main__":
#     kafka_producer_main()


"""*********************************************************************************************************************"""

# """check_cassandra.py"""
# from cassandra.cluster import Cluster
# import logging
# from airflow.exceptions import AirflowException

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# class CassandraConnector:
#     def __init__(self, contact_points):
#         self.cluster = Cluster(contact_points)
#         self.session = self.cluster.connect()

#     def select_data(self, email):
#         query = "SELECT * FROM email_namespace.email_table WHERE email = %s"
#         result = self.session.execute(query, (email,))
        
#         data_dict = {}
        
#         for row in result:
#             data_dict['email'] = row.email
#             data_dict['otp'] = row.otp
#             logger.info(f"Email: {row.email}, OTP: {row.otp}")
        
#         if len(data_dict) == 0:
#             data_dict['email'] = ''
#             data_dict['otp'] = ''
        
#         return data_dict

#     def close(self):
#         self.cluster.shutdown()


# def check_cassandra_main():
#     cassandra_connector = CassandraConnector(['cassandra'])

#     sample_email = 'sample_email@my_email.com'

#     data_dict = cassandra_connector.select_data(sample_email)

#     cassandra_connector.close()

#     logger.info(f"Data found for email: {data_dict['email']}")
#     logger.info(f"OTP: {data_dict['otp']}")
    
#     return data_dict


"""*********************************************************************************************************************"""

"""kafka_create_topic.py"""

from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

admin_config = {
    'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
    'client.id': 'kafka_admin_client'
}

admin_client = AdminClient(admin_config)

def kafka_create_topic_main():
    """Checks if the topic email_topic exists or not. If not, creates the topic."""
    topic_name = 'email_topic'

    existing_topics = admin_client.list_topics().topics
    if topic_name in existing_topics:
        return "Exists"
    
    # Create the new topic
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
    admin_client.create_topics([new_topic])
    return "Created"


if __name__ == "__main__":
    result = kafka_create_topic_main()
    logger.info(result)

"""*********************************************************************************************************************"""
"""kafka_consumer_mongodb.py"""
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import time
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBConnector:
    def __init__(self, mongodb_uri, database_name, collection_name):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.collection_name = collection_name

    def create_collection(self):
        # Check if the collection already exists
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name)
            logger.info(f"Created collection: {self.collection_name}")
        else:
            logger.warning(f"Collection {self.collection_name} already exists")

    def insert_data(self, email, otp):
        document = {
            "email": email,
            "otp": otp
        }
        self.db[self.collection_name].insert_one(document)

    def close(self):
        self.client.close()

class KafkaConsumerWrapperMongoDB:
    def __init__(self, kafka_config, topics):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe(topics)

    def consume_and_insert_messages(self):
        start_time = time.time()
        try:
            while True:
                elapsed_time = time.time() - start_time
                if elapsed_time >= 30:
                    break
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info('Reached end of partition')
                    else:
                        logger.warning('Error: {}'.format(msg.error()))
                else:
                    email = msg.key().decode('utf-8')
                    otp = msg.value().decode('utf-8')

                    existing_document = self.db[self.collection_name].find_one({"email": email, "otp": otp})
                    if existing_document:
                        logger.warning(f"Document with Email={email}, OTP={otp} already exists in the collection.")
                    else:
                        mongodb_connector.insert_data(email, otp)
                        logger.info(f'Received and inserted: Email={email}, OTP={otp}')

        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt. Closing consumer.")
        finally:
            mongodb_connector.close()

    def close(self):
        self.consumer.close()

mongodb_uri = 'mongodb://root:root@mongo:27017/'
database_name = 'email_database'
collection_name = 'email_collection'
mongodb_connector = MongoDBConnector(mongodb_uri, database_name, collection_name)

def kafka_consumer_mongodb_main():
    mongodb_connector.create_collection()

    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094', 
        'group.id': 'consumer_group',
        'auto.offset.reset': 'earliest'
    }

    topics = ['email_topic']

    kafka_consumer = KafkaConsumerWrapperMongoDB(kafka_config, topics)
    kafka_consumer.consume_and_insert_messages()


# if __name__ == '__main__':
#     kafka_consumer_mongodb_main()


"""*********************************************************************************************************************"""

# """kafka_consumer_cassandra.py"""
# from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
# from cassandra.cluster import Cluster
# import time
# import logging

# logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
# logger = logging.getLogger(__name__)

# class CassandraConnector:
#     def __init__(self, contact_points):
#         self.cluster = Cluster(contact_points)
#         self.session = self.cluster.connect()
#         self.create_keyspace()
#         self.create_table()

#     def create_keyspace(self):
#         self.session.execute("""
#             CREATE KEYSPACE IF NOT EXISTS email_namespace
#             WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
#         """)

#     def create_table(self):
#         self.session.execute("""
#             CREATE TABLE IF NOT EXISTS email_namespace.email_table (
#                 email text PRIMARY KEY,
#                 otp text
#             )
#         """)

#     def insert_data(self, email, otp):
#         self.session.execute("""
#             INSERT INTO email_namespace.email_table (email, otp)
#             VALUES (%s, %s)
#         """, (email, otp))

#     def shutdown(self):
#         self.cluster.shutdown()


# def fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs):
#     consumer = Consumer(kafka_config)
#     consumer.subscribe([topic])

#     start_time = time.time()
#     try:
#         while True:
#             elapsed_time = time.time() - start_time
#             if elapsed_time >= run_duration_secs:
#                 break
            
#             msg = consumer.poll(1.0)
#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     logger.info('Reached end of partition')
#                 else:
#                     logger.error('Error: {}'.format(msg.error()))
#             else:
#                 email = msg.key().decode('utf-8')
#                 otp = msg.value().decode('utf-8')


#                 query = "SELECT email FROM email_namespace.email_table WHERE email = %s"
#                 existing_email = cassandra_connector.session.execute(query, (email,)).one()

#                 if existing_email:
#                     logger.warning(f'Skipped existing email: Email={email}')
#                 else:
#                     cassandra_connector.insert_data(email, otp)
#                     logger.info(f'Received and inserted: Email={email}, OTP={otp}')
                            
#     except KeyboardInterrupt:
#         logger.info("Received KeyboardInterrupt. Closing consumer.")
#     finally:
#         consumer.close()


# def kafka_consumer_cassandra_main():
#     cassandra_connector = CassandraConnector(['cassandra'])

#     cassandra_connector.create_keyspace()
#     cassandra_connector.create_table()

#     kafka_config = {
#         'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
#         'group.id': 'cassandra_consumer_group',
#         'auto.offset.reset': 'earliest'
#     }

#     topic = 'email_topic'
#     run_duration_secs = 30

#     fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs)

#     cassandra_connector.shutdown()

# if __name__ == '__main__':
#     kafka_consumer_cassandra_main()


"""*********************************************************************************************************************"""


start_date = datetime(2022, 10, 19, 12, 20)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# email_cassandra = check_cassandra_main()['email']
# otp_cassandra = check_cassandra_main()['otp']
email_mongodb = check_mongodb_main()['email']
otp_mongodb = check_mongodb_main()['otp']

def decide_branch():
    create_topic = kafka_create_topic_main()
    if create_topic == "Created":
        return "topic_created"
    else:
        return "topic_already_exists"


with DAG('airflow_kafka_cassandra_mongodb', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    create_new_topic = BranchPythonOperator(task_id='create_new_topic', python_callable=decide_branch)
    
    # kafka_consumer_cassandra = PythonOperator(task_id='kafka_consumer_cassandra', python_callable=kafka_consumer_cassandra_main,
    #                          retries=2, retry_delay=timedelta(seconds=10),
    #                          execution_timeout=timedelta(seconds=45))
    
    kafka_consumer_mongodb = PythonOperator(task_id='kafka_consumer_mongodb', python_callable=kafka_consumer_mongodb_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))
    
    kafka_producer = PythonOperator(task_id='kafka_producer', python_callable=kafka_producer_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))
    
    # check_cassandra = PythonOperator(task_id='check_cassandra', python_callable=check_cassandra_main,
    #                          retries=2, retry_delay=timedelta(seconds=10),
    #                          execution_timeout=timedelta(seconds=45))
    
    check_mongodb = PythonOperator(task_id='check_mongodb', python_callable=check_mongodb_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))

    topic_created = DummyOperator(task_id="topic_created")

    topic_already_exists = DummyOperator(task_id="topic_already_exists")

    # send_email_cassandra = EmailOperator(
    #     task_id='send_email_cassandra',
    #     to=email_cassandra,
    #     subject='One-Time-Password',
    #     html_content=f"""
    #             <html>
    #             <body>
    #             <h1>Your OTP</h1>
    #             <p>{otp_cassandra}</p>
    #             </body>
    #             </html>
    #             """
    #     )

    send_email_mongodb = EmailOperator(
        task_id='send_email_mongodb',
        to=email_mongodb,
        subject='One-Time-Password',
        html_content=f"""
                <html>
                <body>
                <h1>You can find your One Time Password below</h1>
                <p>{otp_mongodb}</p>
                </body>
                </html>
                """
        )

    # send_slack_cassandra = SlackWebhookOperator(
    # task_id='send_slack_cassandra',
    # slack_webhook_conn_id = 'slack_webhook',
    # message=f"""
    #         :red_circle: New e-mail and OTP arrival
    #         :email: -> {email_cassandra}
    #         :ninja: -> {otp_cassandra}
    #         """,
    # channel='#data-engineering',
    # username='airflow'
    # )

    send_slack_mongodb = SlackWebhookOperator(
    task_id='send_slack_mongodb',
    slack_webhook_conn_id = 'slack_webhook',
    message=f"""
            :red_circle: New e-mail and OTP arrival
            :email: -> {email_mongodb}
            :ninja: -> {otp_mongodb}
            """,
    channel='#data-engineering',
    username='airflow'
    )

    create_new_topic >> [topic_created, topic_already_exists] >> kafka_producer
    # kafka_consumer_cassandra >> check_cassandra >> send_email_cassandra >> send_slack_cassandra
    kafka_consumer_mongodb >> check_mongodb >> send_email_mongodb >> send_slack_mongodb