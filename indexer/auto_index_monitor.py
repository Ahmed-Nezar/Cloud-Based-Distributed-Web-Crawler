import time
import re
import mysql.connector
from collections import defaultdict
import json

# === MySQL Connection (Auto-Retry)
def connect_db():
    return mysql.connector.connect(
        host="172.31.28.123",
        user="Admin",
        password="1234",
        database="INDEXER"
    )

db = connect_db()
db_cursor = db.cursor()

# === Keyword Index Construction
def build_inverted_index():
    global db, db_cursor
    try:
        db.ping(reconnect=True, attempts=1, delay=0)
        inverted = defaultdict(set)

        db_cursor.execute("SELECT url, content FROM indexed_pages")
        for url, content in db_cursor.fetchall():
            words = re.findall(r'\b[a-zA-Z]{3,}\b', content.lower())
            for word in set(words):
                inverted[word].add(url)

        return dict(inverted)
    except Exception as e:
        print(f"[MONITOR] Error in build_inverted_index: {e}")
        reconnect_db()
        return {}

def update_keyword_index():
    global db, db_cursor
    print("[MONITOR] Rebuilding keyword inverted index...")

    inverted_index = build_inverted_index()

    try:
        db_cursor.execute("DELETE FROM keyword_index")
        db.commit()

        for keyword, urls in inverted_index.items():
            urls_json = json.dumps(list(urls))
            try:
                db_cursor.execute("INSERT INTO keyword_index (keyword, urls) VALUES (%s, %s)", (keyword, urls_json))
            except Exception as e:
                print(f"[MONITOR] Failed inserting {keyword}: {e}")
        db.commit()
        print("[MONITOR] Keyword inverted index updated.")
    except Exception as e:
        print(f"[MONITOR] Error in update_keyword_index: {e}")
        reconnect_db()

def get_last_change_signature():
    global db, db_cursor
    try:
        db.ping(reconnect=True, attempts=1, delay=0)
        db_cursor.execute("SELECT COUNT(*), MAX(id) FROM indexed_pages")
        return db_cursor.fetchone()
    except Exception as e:
        print(f"[MONITOR] Error in get_last_change_signature: {e}")
        reconnect_db()
        return (0, 0)

def reconnect_db():
    global db, db_cursor
    try:
        db.close()
    except:
        pass
    db = connect_db()
    db_cursor = db.cursor()
    print("[MONITOR]  Reconnected to MySQL")

# === Monitor Loop
def monitor_index(interval=3):
    print(f"[MONITOR] Watching for updates... (every {interval}s)")
    last_signature = get_last_change_signature()

    while True:
        try:
            current_signature = get_last_change_signature()
            if current_signature != last_signature:
                update_keyword_index()
                last_signature = current_signature
            else:
                print("[MONITOR] No change detected.")
            time.sleep(interval)
        except Exception as e:
            print(f"[MONITOR] Loop error: {e}")
            reconnect_db()
            time.sleep(interval)

if __name__ == "__main__":
    monitor_index()
