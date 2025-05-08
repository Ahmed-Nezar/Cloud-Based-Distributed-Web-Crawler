import boto3
import requests
from bs4 import BeautifulSoup
import uuid
import time
from urllib.parse import urljoin
import threading
import json
import socket

# Constants
MASTER_API = "http://172.31.21.118:5000"
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
urls_crawled = 0
active_threads = 0
thread_status_map = {}  # thread_name -> status
lock = threading.Lock()
stop_event = threading.Event()

# AWS SQS
sqs = boto3.client('sqs', region_name='eu-north-1')
crawler_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/TaskQueueStandard'
indexer_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/IndexerQueueStandard'

# HEARTBEAT
def send_heartbeat():
    while not stop_event.is_set():
        with lock:
            count = urls_crawled  # snapshot under lock
            threads_info = [{"id": k, "status": v} for k, v in thread_status_map.items()]

        try:
            payload = {
                "node_id": NODE_ID,
                "role": NODE_ROLE,
                "ip": NODE_IP,
                "url_count": count,
                "active_threads": active_threads,
                "threads_info": threads_info
            }
            requests.post(f"{MASTER_API}/api/heartbeat", json=payload, timeout=3)
        except Exception as e:
            print(f"[HEARTBEAT ERROR] {e}")

        time.sleep(2)

# Crawl Logic
def crawl_url():
    global urls_crawled, active_threads

    thread_name = threading.current_thread().name

    while not stop_event.is_set():
        with lock:
            active_threads += 1
            thread_status_map[thread_name] = "Waiting for task..."

        try:
            response = sqs.receive_message(
                QueueUrl=crawler_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=3
            )

            if 'Messages' not in response:
                continue

            for message in response['Messages']:
                try:
                    body = json.loads(message['Body'])
                    url = body.get('url')
                    depth = body.get('depth', 0)
                    max_depth = body.get('max_depth', 0)
                    restrict_domain = body.get('restrict_domain', False)
                    domain_prefix = body.get('domain_prefix', '')

                    with lock:
                        thread_status_map[thread_name] = f"Crawling {url} (depth {depth})"
                    print(f"Crawling {url} (depth {depth})")
                    if depth > max_depth:
                        sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=message['ReceiptHandle'])
                        continue

                    headers = {'User-Agent': 'Mozilla/5.0'}
                    if url.startswith("//"):
                        url = "https:" + url
                    if url.startswith('#') or url.startswith('javascript:') or url.strip() == '':
                        sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=message['ReceiptHandle'])
                        continue

                    r = requests.get(url, headers=headers, timeout=5)
                    soup = BeautifulSoup(r.text, 'html.parser')
                    text = soup.get_text()
                    base_url = url
                    links = [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)]

                    # Apply domain restriction
                    if restrict_domain:
                        links = [link for link in links if link.startswith(domain_prefix)]

                    if text.strip():
                        result = {'url': url, 'text': text, 'links': links}
                        dedup_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{url}:{depth}"))  # 👈 depth-aware
                        sqs.send_message(
                            QueueUrl=indexer_queue_url,
                            MessageBody=str(result)
                        )

                    with lock:
                        urls_crawled += 1

                    if depth + 1 <= max_depth:
                        for link in links:
                            sqs.send_message(
                                QueueUrl=crawler_queue_url,
                                MessageBody=json.dumps({"url": link, "depth": depth + 1, "max_depth": max_depth,"restrict_domain": restrict_domain,"domain_prefix": domain_prefix }),
                            )

                except Exception as e:
                    print(f"[CRAWLER] Failed to crawl: {e}")

                sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=message['ReceiptHandle'])

        finally:
            with lock:
                thread_status_map[thread_name] = "Idle"
                active_threads -= 1

        print("[DEBUG] Incrementing URL count:", urls_crawled)
# Launch Threads
def start_crawlers(num_threads):
    threads = []

    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

    for i in range(num_threads):
        t = threading.Thread(target=crawl_url, name=f"Thread-{i+1}")
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    start_crawlers(num_threads=3)
