from flask import Flask, request, jsonify
import boto3
import mysql.connector
import json
import re
import uuid

app = Flask(__name__)

# AWS Setup
sqs = boto3.client('sqs', region_name='eu-north-1')
task_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/TaskQueue.fifo'

# Helpers
def adjust_url(url):
    if not url.startswith('http'):
        url = 'https://' + url
    return url

def is_valid_url(string):
    url_pattern = re.compile(r'^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(/.*)?$')
    return bool(url_pattern.match(string))

# Routes
@app.route('/api/search', methods=['GET'])
def search_keyword():
    keyword = request.args.get('keyword', '').strip().lower()
    if not keyword:
        return jsonify({'error': 'Keyword is required'}), 400

    try:
        # Fresh MySQL connection
        db = mysql.connector.connect(
            host="172.31.28.123",
            user="Admin",
            password="1234",
            database="INDEXER"
        )
        db_cursor = db.cursor()

        sql = "SELECT urls FROM keyword_index WHERE keyword = %s"
        db_cursor.execute(sql, (keyword,))
        row = db_cursor.fetchone()

        if row:
            urls_json = row[0]
            urls = json.loads(urls_json)
            return jsonify({'keyword': keyword, 'urls': urls})
        else:
            return jsonify({'keyword': keyword, 'urls': []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try:
            db_cursor.close()
            db.close()
        except:
            pass

@app.route('/api/crawl', methods=['POST'])
def submit_url():
    data = request.get_json()
    url = data.get('url', '').strip()
    max_depth = data.get('max_depth', 2)

    try:
        max_depth = int(max_depth)
    except ValueError:
        max_depth = 2

    if not url or not is_valid_url(url):
        return jsonify({'error': 'Invalid URL'}), 400

    url = adjust_url(url)

    try:
        sqs.send_message(
            QueueUrl=task_queue_url,
            MessageBody=json.dumps({"url": url, "depth": 0, "max_depth": max_depth}),
            MessageGroupId='1',
            MessageDeduplicationId=str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        )
        return jsonify({'message': f"URL '{url}' submitted for crawling with max depth {max_depth}."}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
