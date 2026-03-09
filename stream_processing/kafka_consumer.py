import json
from kafka import KafkaConsumer
import psycopg2


# Kafka settings
KAFKA_TOPIC = "nba_player_stats"
KAFKA_BROKER = "localhost:9092"


# PostgreSQL settings
DB_CONFIG = {
    "dbname": "nba_db",
    "user": "nba_user",
    "password": "nba_password",
    "host": "localhost",
    "port": "5432"
}


def connect_db():
    """
    Connect to PostgreSQL database
    """
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def insert_player_stats(conn, data):
    """
    Insert player stats into PostgreSQL
    """

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO player_stats
    (player_id, player_name, team, points, assists, rebounds, steals, blocks, minutes)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    values = (
        data["player_id"],
        data["player_name"],
        data["team"],
        data["points"],
        data["assists"],
        data["rebounds"],
        data["steals"],
        data["blocks"],
        data["minutes"]
    )

    cursor.execute(insert_query, values)
    conn.commit()

    cursor.close()


def create_consumer():
    """
    Create Kafka consumer
    """

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    return consumer


def main():

    print("Starting Kafka consumer...")

    consumer = create_consumer()

    conn = connect_db()

    for message in consumer:

        data = message.value

        print(f"Received player: {data['player_name']}")

        insert_player_stats(conn, data)


if __name__ == "__main__":
    main()