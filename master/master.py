from flask import Flask, request, jsonify
import boto3
import mysql.connector
import json
import re
import uuid
from datetime import datetime

# NLP & TF-IDF
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

nltk.download('punkt')
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

app = Flask(__name__)

# AWS SQS Setup
sqs = boto3.client('sqs', region_name='eu-north-1')
task_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/TaskQueueStandard'

# Local heartbeat cache
last_known_counts = {}

def get_db():
    return mysql.connector.connect(
        host="172.31.28.123",
        user="Admin",
        password="1234",
        database="INDEXER"
    )

# ================= 🔁 HEARTBEAT =================
@app.route('/api/heartbeat', methods=['POST'])
def receive_heartbeat():
    data = request.get_json()
    node_id = data.get("node_id")
    role = data.get("role")
    ip = data.get("ip")
    url_count = data.get("url_count", 0)
    threads_info = data.get("threads_info", [])

    if not all([node_id, role, ip]):
        return jsonify({"error": "Missing fields"}), 400

    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO heartbeat (node_id, role, ip, last_seen, url_count)
            VALUES (%s, %s, %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE
                last_seen = NOW(),
                ip = VALUES(ip),
                url_count = VALUES(url_count)
        """, (node_id, role, ip, url_count))
        db.commit()

        last_known_counts[node_id] = {
            "url_count": url_count,
            "last_seen": datetime.utcnow(),
            "threads_info": threads_info
        }

        return jsonify({"message": "Heartbeat received"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        db.close()

# ================= 📊 STATUS =================
@app.route('/api/status', methods=['GET'])
def get_status():
    detailed = request.args.get("detailed", "false").lower() == "true"

    try:
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT node_id, role, ip, url_count, last_seen FROM heartbeat ORDER BY role, last_seen DESC")
        rows = cursor.fetchall()

        now = datetime.utcnow()
        result = []

        for row in rows:
            node_id = row["node_id"]
            current_count = row["url_count"]
            last_seen_db = row["last_seen"]
            role = row["role"]
            ip = row["ip"]
            time_diff = (now - last_seen_db).total_seconds()

            status = "idle" if time_diff <= 5 else "not active"

            item = {
                "node_id": node_id,
                "role": role,
                "ip": ip,
                "url_count": current_count,
                "last_seen": last_seen_db.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status
            }

            if detailed:
                cached = last_known_counts.get(node_id)
                if cached and "threads_info" in cached:
                    item["threads_info"] = cached["threads_info"]

            result.append(item)

        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        db.close()

# ================= 🔒 STATIC FALLBACK ENDPOINTS =================

@app.route("/api/crawler1-status", methods=["GET"])
def crawler1_status():
    try:
        for node_id, info in last_known_counts.items():
            if "ip-172-31-29-60" in node_id.lower():
                time_diff = (datetime.utcnow() - info["last_seen"]).total_seconds()
                return jsonify({"active": time_diff <= 4})
        return jsonify({"active": False})
    except:
        return jsonify({"active": False})

@app.route("/api/crawler2-status", methods=["GET"])
def crawler2_status():
    try:
        for node_id, info in last_known_counts.items():
            if "ip-172-31-10-236" in node_id.lower():
                time_diff = (datetime.utcnow() - info["last_seen"]).total_seconds()
                return jsonify({"active": time_diff <= 4})
        return jsonify({"active": False})
    except:
        return jsonify({"active": False})

@app.route("/api/indexer1-status", methods=["GET"])
def indexer1_status():
    try:
        for node_id, info in last_known_counts.items():
            if "ip-172-31-29-127" in node_id.lower():
                time_diff = (datetime.utcnow() - info["last_seen"]).total_seconds()
                return jsonify({"active": time_diff <= 5})
        return jsonify({"active": False})
    except:
        return jsonify({"active": False})

# ================= 🔎 SEARCH (TF-IDF) =================
@app.route('/api/search', methods=['GET'])
def search_keyword():
    query = request.args.get('keyword', '').strip().lower()
    if not query:
        return jsonify({'error': 'Keyword is required'}), 400

    try:
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT url, content FROM indexed_pages")
        pages = cursor.fetchall()

        urls = []
        docs = []
        for page in pages:
            text = page["content"]
            if text and text.strip():
                urls.append(page["url"])
                docs.append(text)

        if not docs:
            return jsonify({'keyword': query, 'urls': []})

        def tokenize(text):
            tokens = word_tokenize(text.lower())
            return ' '.join([w for w in tokens if w.isalnum() and w not in stop_words])

        clean_docs = [tokenize(doc) for doc in docs]
        clean_query = tokenize(query)

        vectorizer = TfidfVectorizer()
        doc_vectors = vectorizer.fit_transform(clean_docs + [clean_query])
        cosine_scores = cosine_similarity(doc_vectors[-1], doc_vectors[:-1]).flatten()

        results = sorted(zip(urls, cosine_scores), key=lambda x: x[1], reverse=True)
        filtered = [url for url, score in results if score > 0.05][:20]
        return jsonify({'keyword': query, 'urls': filtered})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        db.close()

# ================= 🌐 CRAWL =================
def adjust_url(url):
    if not url.startswith('http'):
        url = 'https://' + url
    return url

def is_valid_url(string):
    url_pattern = re.compile(r'^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(/.*)?$')
    return bool(url_pattern.match(string))

@app.route('/api/crawl', methods=['POST'])
def submit_url():
    data = request.get_json()
    url = data.get('url', '').strip()
    max_depth = data.get('max_depth', 2)
    restrict_domain = data.get('domain_restricted', False)

    try:
        max_depth = int(max_depth)
    except ValueError:
        max_depth = 2

    if not url or not is_valid_url(url):
        return jsonify({'error': 'Invalid URL'}), 400

    url = adjust_url(url)

    try:
        domain_prefix = url.split('/')[0] + '//' + url.split('/')[2] if restrict_domain else None

        payload = {
            "url": url,
            "depth": 0,
            "max_depth": max_depth,
            "restrict_domain": restrict_domain,
            "domain_prefix": domain_prefix
        }

        sqs.send_message(
            QueueUrl=task_queue_url,
            MessageBody=json.dumps(payload)
        )
        return jsonify({
            'message': f"URL '{url}' submitted for crawling with max depth {max_depth}. Domain restriction: {restrict_domain}"
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ================= 🩺 HEALTH =================
@app.route('/ping', methods=['GET'])
def ping():
    return "pong", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=False)
