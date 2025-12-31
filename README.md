# 🛡️ Real-Time Ethereum Fraud Detection Pipeline

## 📌 Project Overview
An end-to-end data engineering pipeline designed to monitor the Ethereum blockchain in real-time. The system ingests live blocks, processes transactions using **Apache Spark**, detects high-value "Whale" movements (>50 ETH) and potential anomalies, and visualizes the insights on a **Power BI** dashboard.

## 🏗️ Architecture
The project follows the **Medallion Architecture**:
1.  **Bronze Layer (Ingestion):** `Web3.py` producer fetches live blocks and streams raw JSON data to **Apache Kafka**.
2.  **Silver Layer (Processing):** **PySpark Structured Streaming** consumes Kafka topics, performs schema enforcement, and applies security rules.
3.  **Gold Layer (Storage & Analytics):** Processed data is sunk into **PostgreSQL** for historical analysis and connected to Power BI for real-time monitoring.

## 🛠️ Tech Stack
* **Language:** Python 3.9
* **Stream Processing:** Apache Spark 3.5.1 (PySpark)
* **Message Broker:** Apache Kafka & Zookeeper
* **Database:** PostgreSQL 15
* **Blockchain Client:** Web3.py & Alchemy API
* **Infrastructure:** Docker & Docker Compose
* **Visualization:** Power BI (DirectQuery)

## 🚀 How to Run

### 1. Prerequisites
* Docker Desktop installed
* Python 3.9+

### 2. Setup
Clone the repo and configure environment variables:
```bash
git clone [https://github.com/Younesghz1993/ethereum-fraud-detection-pipeline.git](https://github.com/Younesghz1993/ethereum-fraud-detection-pipeline.git)
cd ethereum-fraud-detection-pipeline
cp .env.example .env
# Edit .env and add your Alchemy API Key
```
### 3. Start Services
docker-compose up -d

### 4. Run Pipeline
# Terminal 1: Producer
python blockchain_producer.py

# Terminal 2: Spark Processor
python blockchain_spark_processor.py

📈 Results
Real-time latency: < 2 seconds.

Handles high throughput of Ethereum Mainnet transactions.

Successfully detects and logs Whale transactions to the Gold Layer.

Created by Younes
