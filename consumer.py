from google.cloud import pubsub_v1
import psycopg2
import os

project_id = "karansirproject"
subscription_name = "my-subscription"

def callback(message):
    # Assuming message data is text (encoded as UTF-8)
    message_data = message.data.decode("utf-8")

    # Example: Connect to PostgreSQL and save the message
    connection = psycopg2.connect(
        host="34.28.145.28",
        port=5432,
        database="my-database",
        user="postgres",
        password="Swap@db1"
    )
    cursor = connection.cursor()
    cursor.execute("INSERT INTO messages (message) VALUES (%s)", (message_data,))
    connection.commit()

    message.ack()

if __name__ == "__main__":
    credentials_path = r"C:\Users\Swapnil Verma\Downloads\karansirproject-db6658f807ec.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    subscriber.subscribe(subscription_path, callback=callback)

    # Keep the consumer running to continuously process messages
    while True:
        pass
