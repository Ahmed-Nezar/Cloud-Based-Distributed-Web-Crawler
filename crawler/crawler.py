import boto3
import requests
from bs4 import BeautifulSoup
import uuid
import time
from urllib.parse import urljoin
import threading
import json

# Initialize SQS client
sqs = boto3.client('sqs', region_name='eu-north-1')  # Set the correct region
crawler_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/TaskQueue.fifo'
indexer_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/IndexerQueue.fifo'
master_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/MasterQueue.fifo'

max_depth = 0

def crawl_url():
    print("[CRAWLER] Started and Ready")
    while True:
        # Poll the SQS queue for a new URL
        response = sqs.receive_message(
            QueueUrl=crawler_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10  # Long polling for better efficiency
        )

        if 'Messages' in response:
            for message in response['Messages']:

                raw_body = message['Body']
                body = json.loads(raw_body)  # Parse JSON string into Python dict
                url = body.get('url')
                depth = body.get('depth', 0)

                        # CHECK MAX DEPTH EARLY
                if depth > max_depth:
                    print(f"[CRAWLER] Skipping {url} because depth {depth} exceeds max_depth {max_depth}")
                    sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=message['ReceiptHandle'])
                    continue

                print(f"[CRAWLER] Crawling URL: {url}")

                # Basic politeness (crawl delay)
                time.sleep(2)  # Add a 2-second delay between requests to avoid overloading the server

                try:
                    headers = {'User-Agent': 'Mozilla/5.0'}

                    # Normalize scheme-relative URLs
                    if url.startswith("//"):
                        url = "https:" + url

                    # Skip junk URLs
                    if url.startswith('#') or url.startswith('javascript:') or url.strip() == '':
                        print(f"[CRAWLER] Skipping non-crawlable link: {url}")
                        sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=message['ReceiptHandle'])
                        continue

                    r = requests.get(url, headers=headers, timeout=5)
                    soup = BeautifulSoup(r.text, 'html.parser')
                    base_url = url

                    text = soup.get_text()
                    links = [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)]

                    result = {'url': url, 'text': text, 'links': links}

                    # Create a unique MessageDeduplicationId based on the URL (or its hash)
                    message_deduplication_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))  # Using UUID5 based on URL

                    # Send the crawled data to the indexer queue
                    if text.strip():
                        sqs.send_message(QueueUrl=indexer_queue_url, MessageBody=str(result), MessageGroupId='1', MessageDeduplicationId=message_deduplication_id)

                    print(f"[CRAWLER] Crawled {url} with {len(links)} links")
                    # Send extracted URLs back to the master node (task management)

                    # Only continue if not exceeding max depth
                    if depth + 1 <= max_depth:

                        for link in links:
                            sqs.send_message(
                                QueueUrl=master_queue_url,
                                MessageBody=json.dumps({"url": link, "depth": depth + 1}),
                                MessageGroupId='1',
                                MessageDeduplicationId=str(uuid.uuid5(uuid.NAMESPACE_DNS, link))
                            )
                except Exception as e:
                    print(f"[CRAWLER] Failed to crawl {url}: {e}")

                # Delete the message from the queue after processing
                sqs.delete_message(
                    QueueUrl=crawler_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )

def start_crawlers(num_threads):
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=crawl_url)
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    num_threads = 5  # Set the number of threads (crawlers) to run concurrently
    start_crawlers(num_threads)
