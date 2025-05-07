# Cloud-Based Distributed Web Crawler

A scalable, fault-tolerant distributed web crawling system built with Python, Flask, AWS SQS, and MySQL. This system monitors node heartbeats, supports failover crawlers and indexers, performs keyword-based search using TF-IDF, and allows real-time UI monitoring.

---

## Collaborators

- Ahmed Nezar  
- Mohamed Yasser  
- AbdulRahman Hesham  
- Kirollos Ehab  

---

## Technologies Used

- **Python 3.12+**
- **Flask** – for backend API & monitoring UI
- **MySQL** – for indexing and storing heartbeat data
- **AWS SQS** – task distribution queues
- **Bootstrap + jQuery** – monitoring client UI
- **TF-IDF + NLTK** – for keyword-based content search

---

## Project Structure

```
Cloud-Based-Distributed-Web-Crawler/
│
├── master/                 # Master Node API (Flask)
│   └── master.py
│
├── crawler/                # Primary Crawler Node
│   └── crawler.py
│
├── crawler2/               # Secondary Crawler Node
│   └── crawler2.py
│
├── crawler3/               # Passive Crawler (Auto-scaling Backup)
│   └── crawler3.py
│
├── indexer/                # Indexer Node
│   ├── indexer.py
│   ├── auto_index_monitor.py
│   └── config.py
│
├── indexer2/               # Passive Indexer (Fault-tolerant)
│   ├── indexer2.py
│   └── auto_index_monitor.py
│
├── client/                 # Monitoring + Crawler Control UI
│   └── client.py
│
└── README.md
```

---

## ⚙️ MySQL Setup

1. **Install MySQL** (on Ubuntu):
   ```bash
   sudo apt update
   sudo apt install mysql-server
   ```

2. **Secure MySQL (optional)**:
   ```bash
   sudo mysql_secure_installation
   ```

3. **Login & Create Database**:
   ```bash
   sudo mysql -u root -p
   ```

   ```sql
   CREATE DATABASE INDEXER;
   CREATE USER 'Admin'@'%' IDENTIFIED BY '1234';
   GRANT ALL PRIVILEGES ON INDEXER.* TO 'Admin'@'%';
   FLUSH PRIVILEGES;
   USE INDEXER;

   CREATE TABLE indexed_pages (
     id INT AUTO_INCREMENT PRIMARY KEY,
     url TEXT,
     content LONGTEXT,
     indexed_obj_id VARCHAR(255),
     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );

   CREATE TABLE heartbeat (
     node_id VARCHAR(255) PRIMARY KEY,
     role VARCHAR(50),
     ip VARCHAR(50),
     last_seen DATETIME,
     url_count INT
   );

   CREATE TABLE keyword_index (
     keyword VARCHAR(255) PRIMARY KEY,
     urls LONGTEXT
   );
   ```

4. **Allow remote access**:
   - Edit `/etc/mysql/mysql.conf.d/mysqld.cnf` and set:
     ```
     bind-address = 0.0.0.0
     ```
   - Restart:
     ```bash
     sudo systemctl restart mysql
     ```

5. **AWS**: Allow port `3306` in security group.

---

## How to Run the Project

### 1. Clone the Repository
```bash
git clone https://github.com/Ahmed-Nezar/Cloud-Based-Distributed-Web-Crawler
cd Cloud-Based-Distributed-Web-Crawler
```

### 2. Launch EC2 Instances
- Master Node
- Client Node
- Crawler 1
- Crawler 2
- Crawler 3 
- Indexer 1
- Indexer 2
- StorageDB

### 3. Python Environment
Install dependencies on each instance:
```bash
sudo apt install python3-pip
pip3 install -r requirements.txt
```

Basic packages include:
- flask
- boto3
- mysql-connector-python
- beautifulsoup4
- nltk
- scikit-learn

Make sure NLTK corpora are downloaded where needed:
```python
import nltk
nltk.download('punkt')
nltk.download('stopwords')
```

### 4. Run Each Component

| Node       | Script                         |
|------------|--------------------------------|
| Master     | `python3 master/master.py`     |
| Client     | `python3 client/client.py`     |
| Crawler 1  | `python3 crawler/crawler.py`   |
| Crawler 2  | `python3 crawler2/crawler2.py` |
| Crawler 3  | `python3 crawler3/crawler3.py` |
| Indexer 1  | `python3 indexer/indexer.py`   |
| Indexer 2  | `python3 indexer2/indexer2.py` |

---

## Architecture Diagram
![image](https://github.com/user-attachments/assets/0a12be94-08ed-4259-96ba-e46cb339b294)


## Features Summary

- ✅ Multi-threaded Crawler & Indexer Nodes
- ✅ Heartbeat Status Reporting (Running, Idle, Not Active)
- ✅ Auto-failover for Crawler3 and Indexer2
- ✅ Domain-Restricted Crawling
- ✅ TF-IDF based keyword search API
- ✅ Client UI to monitor & trigger crawl/search actions
- ✅ MySQL-powered storage and heartbeat persistence

---
