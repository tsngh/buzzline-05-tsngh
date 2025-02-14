"""
consumer_tsngh.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
from collections import defaultdict
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
import time


# import external modules
from kafka import KafkaConsumer

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_case import init_db, insert_message

sentiment_data = defaultdict(list)

# Data structure to store sentiment data by category
sentiment_data = defaultdict(list)

# Set up the plot
fig, ax = plt.subplots(figsize=(12, 6))
plt.ion()

def update_chart(frame):
    ax.clear()
    categories = list(sentiment_data.keys())
    avg_sentiments = [sum(sentiments)/len(sentiments) if sentiments else 0 for sentiments in sentiment_data.values()]

    bars = ax.bar(categories, avg_sentiments)
    ax.set_xlabel("Categories")
    ax.set_ylabel("Average Sentiment")
    ax.set_title("Real-Time Average Sentiment by Category- Tesheena S.")
    ax.set_ylim(0, 1)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom')

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

def process_message(message: dict) -> dict:
    logger.info(f"Processing message: {message}")
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        
        # Update sentiment data for visualization
        category = processed_message["category"]
        sentiment = processed_message["sentiment"]
        sentiment_data[category].append(sentiment)
        
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def consume_messages_from_kafka(topic, kafka_url, group, sql_path, interval_secs):
    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}")
            sys.exit(13)

    logger.info("Step 4. Process messages.")
    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        last_update_time = time.time()
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, sql_path)
            
            # Update chart every 5 seconds
            current_time = time.time()
            if current_time - last_update_time >= 5:
                plt.draw()
                plt.pause(0.001)
                last_update_time = current_time

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise

def main():
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Set up the sentiment analysis plot.")
    ani = FuncAnimation(fig, update_chart, interval=5000)  # Update every 5 seconds

    logger.info("STEP 5. Begin consuming and storing messages.")
    try:
        plt.show(block=False)  # Show the plot without blocking
        consume_messages_from_kafka(topic, kafka_url, group_id, sqlite_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.close()
        logger.info("Consumer shutting down.")

if __name__ == "__main__":
    main()
