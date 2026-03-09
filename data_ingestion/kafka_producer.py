import json 
import time
from kafka import KafkaProducer
from nba_api.stats.endpoints import leaguedashplayerstats

kafka_broker = 'localhost:9092'
topic_name = 'nba_player_stats'

def create_producer():
    """
    Initialize Kafka producer
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    return producer

def fetch_player_stats(season="2023-24"):
    """
    Fetch player stats from NBA API
    """

    stats = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star="Regular Season"
    )

    df = stats.get_data_frames()[0]
    df.columns = df.columns.str.lower()

    return df


def stream_stats(producer):
    """
    Send player stats to Kafka topic
    """

    df = fetch_player_stats()

    for _, row in df.iterrows():

        message = {
            "player_id": int(row["player_id"]),
            "player_name": row["player_name"],
            "team": row["team_abbreviation"],
            "points": float(row["pts"]),
            "assists": float(row["ast"]),
            "rebounds": float(row["reb"]),
            "steals": float(row["stl"]),
            "blocks": float(row["blk"]),
            "minutes": float(row["min"])
        }

        producer.send(TOPIC_NAME, value=message)

        print(f"Sent: {message['player_name']}")

    producer.flush()


def main():

    print("Starting Kafka producer...")

    producer = create_producer()

    while True:
        print("Fetching and streaming player stats...")
        stream_stats(producer)

        print("Sleeping for 60 seconds...")
        time.sleep(60)


if __name__ == "__main__":
    main()