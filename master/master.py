from flask import Flask, request, jsonify
import boto3
import mysql.connector
import json
import re
import uuid
from datetime import datetime, timedelta

app = Flask(__name__)

# AWS SQS Setup (still used for crawl endpoint)
sqs = boto3.client('sqs', region_name='eu-north-1')
task_queue_url = 'https://sqs.eu-north-1.amazonaws.com/441832714601/TaskQueue.fifo'

# In-memory cache for url_count tracking
last_known_counts = {}

# --- Helpers ---
def adjust_url(url):
    if not url.startswith('http'):
        url = 'https://' + url
    return url

def is_valid_url(string):
    url_pattern = re.compile(r'^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(/.*)?$')
    return bool(url_pattern.match(string))

def get_db():
    return mysql.connector.connect(
        host="172.31.28.123",
        user="Admin",
        password="1234",
        database="INDEXER"
    )

# ========== 🔁 HEARTBEAT ==========
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

        # Update local cache for tracking
        last_known_counts[node_id] = {
            "url_count": url_count,
            "last_seen": datetime.utcnow(),
            "threads_info": threads_info
        }

        return jsonify({"message": "Heartbeat received"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cursor.close()
            db.close()
        except:
            pass

# ========== 📊 STATUS ==========
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

            # Default fallback
            status = "not active"

            if time_diff <= 10:
                cached = last_known_counts.get(node_id)
                if cached:
                    previous_count = cached["url_count"]
                    print("Yasser prev:", previous_count, "Nezar Now:", current_count)
                    if current_count > previous_count:
                        status = "running"
                    else:
                        status = "idle"
                else:
                    status = "idle"

            item = {
                "node_id": node_id,
                "role": role,
                "ip": ip,
                "url_count": current_count,
                "last_seen": last_seen_db.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status
            }

            # Optional: threads_info shown for visual purposes only
            if detailed:
                cached = last_known_counts.get(node_id)
                if cached and "threads_info" in cached:
                    item["threads_info"] = cached["threads_info"]

            result.append(item)

        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cursor.close()
            db.close()
        except:
            pass

# ========== 🔎 SEARCH ==========
@app.route('/api/search', methods=['GET'])
def search_keyword():
    keyword = request.args.get('keyword', '').strip().lower()
    if not keyword:
        return jsonify({'error': 'Keyword is required'}), 400

    try:
        db = get_db()
        cursor = db.cursor()

        sql = "SELECT urls FROM keyword_index WHERE keyword = %s"
        cursor.execute(sql, (keyword,))
        row = cursor.fetchone()

        if row:
            urls = json.loads(row[0])
            return jsonify({'keyword': keyword, 'urls': urls})
        else:
            return jsonify({'keyword': keyword, 'urls': []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try:
            cursor.close()
            db.close()
        except:
            pass

# ========== 🌐 CRAWL ==========
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
            MessageDeduplicationId=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{url}:{max_depth}"))
        )
        return jsonify({'message': f"URL '{url}' submitted for crawling with max depth {max_depth}."}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ========== HEALTH ==========
@app.route('/ping', methods=['GET'])
def ping():
    return "pong", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
