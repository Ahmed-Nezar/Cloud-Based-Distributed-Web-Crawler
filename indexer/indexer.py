import subprocess
import boto3
import mysql.connector
import threading
import time
import uuid
import requests
import socket
from bs4 import BeautifulSoup

# Constants
MASTER_API = "http://172.31.21.118:5000"
NODE_ROLE = "indexer"
NODE_ID = socket.gethostname()

def get_private_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

NODE_IP = get_private_ip()

# Globals
urls_indexed = 0
active_threads = 0
thread_status_map = {}  # thread_name -> status
lock = threading.Lock()
stop_event = threading.Event()

# AWS SQS setup
sqs = boto3.client('sqs', region_name='eu-north-1')
indexer_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/IndexerQueue.fifo'

# MySQL setup
db = mysql.connector.connect(
    host="172.31.28.123",
    user="Admin",
    password="1234",
    database="INDEXER"
)
db_cursor = db.cursor()

# 🔁 Heartbeat
def send_heartbeat():
    while not stop_event.is_set():
        try:
            with lock:
                threads_info = [{"id": k, "status": v} for k, v in thread_status_map.items()]
                payload = {
                    "node_id": NODE_ID,
                    "role": NODE_ROLE,
                    "ip": NODE_IP,
                    "url_count": urls_indexed,
                    "active_threads": active_threads,
                    "threads_info": threads_info
                }
            requests.post(f"{MASTER_API}/api/heartbeat", json=payload, timeout=3)
        except Exception as e:
            print(f"[INDEXER] Heartbeat failed: {e}")
        time.sleep(2)

# Clean HTML
def clean_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)

# Worker logic
def process_message(index):
    global urls_indexed, active_threads
    thread_name = threading.current_thread().name

    try:
        response = sqs.receive_message(
            QueueUrl=indexer_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        if 'Messages' not in response:
            return

        for message in response['Messages']:
            with lock:
                active_threads += 1
                thread_status_map[thread_name] = "Processing message..."

            try:
                data = eval(message['Body'])  # ⚠️ Replace in production
                url = data.get('url')
                raw_html = data.get('text')

                if url and raw_html:
                    with lock:
                        thread_status_map[thread_name] = f"Indexing {url}"

                    cleaned_text = clean_html(raw_html)

                    with lock:
                        urls_indexed += 1

                    sql = """
                        INSERT INTO indexed_pages (url, content, indexed_obj_id)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            content = VALUES(content),
                            indexed_obj_id = VALUES(indexed_obj_id)
                    """
                    db_cursor.execute(sql, (url, cleaned_text, "dummy-id"))
                    db.commit()

                sqs.delete_message(
                    QueueUrl=indexer_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except Exception as e:
                print(f"[INDEXER] Failed to process: {e}")
            finally:
                with lock:
                    active_threads -= 1
                    thread_status_map[thread_name] = "Idle"

    except Exception as outer:
        print(f"[INDEXER] SQS error: {outer}")

# Indexer worker
def index_worker(index):
    while not stop_event.is_set():
        process_message(index)

# Load index (not used now)
def load_index():
    print("[INDEXER] No local content index loaded (using DB only).")
    return {}

# Entry point
def main():
    print("[INDEXER] Starting...")
    index = load_index()

    threads = []
    for i in range(2):
        t = threading.Thread(target=index_worker, args=(index,), name=f"Thread-{i+1}")
        threads.append(t)
        t.start()

    hb_thread = threading.Thread(target=send_heartbeat, daemon=True)
    hb_thread.start()

    subprocess.Popen(["python3", "auto_index_monitor.py"])

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        stop_event.set()
        for t in threads:
            t.join()

    db_cursor.close()
    db.close()
    print("[INDEXER] Clean exit.")

if __name__ == "__main__":
    main()
