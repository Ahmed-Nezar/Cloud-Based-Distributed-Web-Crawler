import boto3
import uuid
import time
import json

# Seed URLs to crawl
seed_urls = [
    "https://www.python.org/",
#    "https://www.wikipedia.org/",
#    "https://www.example.com/"
]

# Initialize SQS client
sqs = boto3.client('sqs', region_name='eu-north-1')  # Set the correct region
queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/TaskQueue.fifo'
master_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/MasterQueue.fifo'


def main():
    print("[MASTER] Starting task dispatch...")

    for url in seed_urls:
        print(f"[MASTER] Dispatching crawl task: {url}")
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"url": url, "depth": 0}),
            MessageGroupId='1',
            MessageDeduplicationId=url  # Optional: Use URL to ensure deduplication
        )

    print("[MASTER] Finished dispatching all tasks.")

    # Start receiving URLs from the Crawler Nodes (via the Master Queue)
    receive_urls_from_crawler()

def receive_urls_from_crawler():
    while True:
        print("[MASTER] Waiting for URLs from Crawler Nodes...")

        # Poll the master queue for URLs that were extracted by Crawler Nodes
        response = sqs.receive_message(
            QueueUrl=master_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10  # Long polling for better efficiency
        )

        if 'Messages' in response:
            for message in response['Messages']:

                raw_body = message['Body']
                body = json.loads(raw_body)  # Parse JSON string into Python dict
                extracted_url = body.get('url')
                extracted_depth = body.get('depth', 0)

                print(f"[MASTER] Received extracted URL from Crawler: {extracted_url} at depth {extracted_depth}")

                # Process the URL (you can decide what to do with it, e.g., send more tasks)
                # For now, let's just print the URL and reassign it to Crawler if needed
                assign_url_to_crawler(extracted_url, extracted_depth)

                # Delete the message from the queue after processing
                sqs.delete_message(
                    QueueUrl=master_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
        else:
                print("[MASTER] No messages received from Crawler.")

        time.sleep(1)  # Poll every 5 seconds

def assign_url_to_crawler(url, depth):
    print(f"[MASTER] Reassigning URL to Crawler for further crawling: {url}")
    # Reassign the URL to the Crawler Node (task assignment)
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"url": url, "depth": depth}),
        MessageGroupId='1',  # FIFO Queue requires MessageGroupId
        MessageDeduplicationId=str(uuid.uuid5(uuid.NAMESPACE_DNS, url))  # Deduplication based on URL
    )


if __name__ == "__main__":
    main()
