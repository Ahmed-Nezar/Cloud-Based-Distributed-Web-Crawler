import subprocess
import boto3
from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_HOST
import mysql.connector
import os
import threading
import time
import uuid

# Initialize SQS client
sqs = boto3.client('sqs', region_name='eu-north-1')  # Set the correct region
indexer_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/IndexerQueue.fifo'

# Initialize Elasticsearch client
es = Elasticsearch(
    ELASTICSEARCH_HOST,
    api_key=None,
    headers={"accept": "application/json", "Content-Type": "application/json"}
)

# Initialize MySQL database connection
db = mysql.connector.connect(
    host="172.31.28.123",
    user="Admin",
    password="1234",
    database="INDEXER"
)
db_cursor = db.cursor()

lock = threading.Lock()
stop_event = threading.Event()

def load_index():
    """Load index from MySQL database into a dictionary."""
    index = {}
    db_cursor.execute("SELECT url, content, indexed_obj_id FROM indexed_pages")
    for url, content, indexed_obj_id in db_cursor.fetchall():
        index[url] = (content, {'_id': indexed_obj_id})  # simulate old structure
    print("[INDEXER] Index loaded from database.")
    return index

def process_message(index):
    response = sqs.receive_message(
        QueueUrl=indexer_queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )

    if 'Messages' not in response:
        return

    for message in response['Messages']:
        try:
            data = eval(message['Body'])  # ⚠️ Replace with safer deserialization if possible
            url = data.get('url')
            content = data.get('text')

            if url and content:
                indexed_obj = es.index(index="webpages", document={"url": url, "content": content})

                with lock:
                    index[url] = (content, indexed_obj)
                    print(f"[INDEXER] Indexed {url}")

                    # Save to MySQL
                    try:
                        sql = """
                        INSERT INTO indexed_pages (url, content, indexed_obj_id)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            content = VALUES(content),
                            indexed_obj_id = VALUES(indexed_obj_id)
                        """
                        db_cursor.execute(sql, (url, content, indexed_obj['_id']))
                        db.commit()
                        print(f"[INDEXER] Saved {url} into database.")
                    except Exception as db_error:
                        print(f"[INDEXER] Database insert failed: {db_error}")

            # Delete message from SQS
            sqs.delete_message(
                QueueUrl=indexer_queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )

        except Exception as e:
            print(f"[INDEXER] Failed to process message: {e}")

def index_worker(index):
    while not stop_event.is_set():
        process_message(index)

def main():
    print("[INDEXER] Started with threading.")
    index = load_index()

    num_threads = 2
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=index_worker, args=(index,))
        threads.append(t)
        t.start()

    try:
        subprocess.Popen(["python3", "auto_index_monitor2.py"])
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("[INDEXER] Ctrl+C detected! Stopping threads...")
        stop_event.set()

        # Optionally wait for threads to finish
        for t in threads:
            t.join()

        db_cursor.close()
        db.close()

        print("[INDEXER] Clean exit.")

if __name__ == "__main__":
    main()
