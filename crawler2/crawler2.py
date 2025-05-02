import boto3
import requests
from bs4 import BeautifulSoup
import uuid
import time
from urllib.parse import urljoin
import threading
import json
import socket

# === Configuration ===
MASTER_API = "http://172.31.21.118:5000"  # Master node IP
NODE_ROLE = "crawler"
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

# === AWS SQS Setup ===
sqs = boto3.client('sqs', region_name='eu-north-1')
crawler_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/TaskQueue.fifo'
indexer_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/IndexerQueue.fifo'

# === Globals ===
url_count = 0
thread_status_map = {}
lock = threading.Lock()

# === Helper: Should Crawler2 Run? ===
def should_run():
    try:
        res = requests.get(f"{MASTER_API}/api/crawler1-status", timeout=3)
        status = res.json().get("active", True)
        return not status
    except:
        return False  # default to idle if unreachable

# === Heartbeat Thread ===
def send_heartbeat():
    global url_count
    while True:
        try:
            with lock:
                threads_info = [{"id": name, "status": status} for name, status in thread_status_map.items()]
                payload = {
                    "node_id": NODE_ID,
                    "role": NODE_ROLE,
                    "ip": NODE_IP,
                    "url_count": url_count,
                    "threads_info": threads_info
                }
            requests.post(f"{MASTER_API}/api/heartbeat", json=payload, timeout=3)
        except Exception as e:
            print(f"[CRAWLER2][HEARTBEAT] Failed to send heartbeat: {e}")
        time.sleep(2)

# === Main Crawl Function ===
def crawl_url():
    global url_count
    thread_name = threading.current_thread().name

    print("[CRAWLER2] Standby crawler waiting for Crawler1 failure...")

    while True:
        with lock:
            thread_status_map[thread_name] = "Waiting for master signal..."

        if not should_run():
            # print("[CRAWLER2] Crawler1 is active — staying idle")
            # time.sleep(2)
            continue

        print("[CRAWLER2] Crawler1 down — taking over crawling")

        response = sqs.receive_message(
            QueueUrl=crawler_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=3
        )

        if 'Messages' not in response:
            continue

        for message in response['Messages']:
            raw_body = message['Body']
            body = json.loads(raw_body)
            url = body.get('url')
            depth = body.get('depth', 0)
            max_depth = body.get('max_depth', 0)

            if depth > max_depth:
                sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=message['ReceiptHandle'])
                continue

            print(f"[CRAWLER2] Crawling URL: {url}")

            with lock:
                thread_status_map[thread_name] = f"Crawling {url}"

            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                if url.startswith("//"):
                    url = "https:" + url

                if url.startswith('#') or url.startswith('javascript:') or url.strip() == '':
                    print(f"[CRAWLER2] Skipping: {url}")
                    sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=message['ReceiptHandle'])
                    continue

                r = requests.get(url, headers=headers, timeout=5)
                soup = BeautifulSoup(r.text, 'html.parser')
                base_url = url

                text = soup.get_text()
                links = [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)]

                result = {'url': url, 'text': text, 'links': links}
                dedup_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))

                if text.strip():
                    sqs.send_message(QueueUrl=indexer_queue_url, MessageBody=str(result),
                                     MessageGroupId='1', MessageDeduplicationId=dedup_id)

                print(f"[CRAWLER2] Crawled {url} with {len(links)} links")

                with lock:
                    url_count += 1

                if depth + 1 <= max_depth:
                    for link in links:
                        sqs.send_message(
                            QueueUrl=crawler_queue_url,
                            MessageBody=json.dumps({"url": link, "depth": depth + 1, "max_depth": max_depth}),
                            MessageGroupId='1',
                            MessageDeduplicationId=str(uuid.uuid5(uuid.NAMESPACE_DNS, link))
                        )
            except Exception as e:
                print(f"[CRAWLER2] Failed to crawl {url}: {e}")

            sqs.delete_message(
                QueueUrl=crawler_queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )

            with lock:
                thread_status_map[thread_name] = "Idle"

# === Main Launcher ===
def start_crawler2():
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

    crawl_thread = threading.Thread(target=crawl_url, name="Thread-1")
    crawl_thread.start()

    crawl_thread.join()

if __name__ == "__main__":
    start_crawler2()
