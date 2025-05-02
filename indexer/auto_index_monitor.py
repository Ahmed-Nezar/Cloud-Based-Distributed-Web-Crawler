import time
import re
import mysql.connector
from collections import defaultdict
import json

# Database connection
db = mysql.connector.connect(
    host="172.31.28.123",
    user="Admin",
    password="1234",
    database="INDEXER"
)
db_cursor = db.cursor()

def build_inverted_index():
    inverted = defaultdict(set)

    db_cursor.execute("SELECT url, content FROM indexed_pages")
    for url, content in db_cursor.fetchall():
        words = re.findall(r'\b[a-zA-Z]{3,}\b', content.lower())  # Only words 3+ characters
        for word in set(words):
            inverted[word].add(url)

    return dict(inverted)

def update_keyword_index():
    print("[MONITOR] Rebuilding keyword inverted index...")

    inverted_index = build_inverted_index()

    # Clear old keyword_index table
    db_cursor.execute("DELETE FROM keyword_index")
    db.commit()

    # Insert new inverted index
    for keyword, urls in inverted_index.items():
        urls_json = json.dumps(list(urls))
        try:
            sql = "INSERT INTO keyword_index (keyword, urls) VALUES (%s, %s)"
            db_cursor.execute(sql, (keyword, urls_json))
        except Exception as e:
            print(f"[MONITOR] Failed inserting {keyword}: {e}")

    db.commit()
    print("[MONITOR] ✅ Keyword inverted index updated.")

def get_last_change_signature():
    """Return a quick signature of the indexed_pages content (count + max id)."""
    db.reconnect(attempts=1, delay=0)
    db_cursor.execute("SELECT COUNT(*), MAX(id) FROM indexed_pages")
    count, max_id = db_cursor.fetchone()
    return (count, max_id)

def monitor_index(interval=3):
    print("[MONITOR] Watching for changes in indexed_pages database... (every {}s)".format(interval))

    last_signature = get_last_change_signature()

    while True:
        try:
            current_signature = get_last_change_signature()

            if current_signature != last_signature:
                update_keyword_index()
                last_signature = current_signature
            else:
                print("[MONITOR] No changes detected. Skipping rebuild.")

            time.sleep(interval)
        except Exception as e:
            print(f"[MONITOR] ⚠️ Error: {e}")
            time.sleep(interval)

if __name__ == "__main__":
    monitor_index()
