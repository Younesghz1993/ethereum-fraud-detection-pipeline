import os
import json
import time
from web3 import Web3
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
ALCHEMY_URL = os.getenv("ALCHEMY_URL")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")

# Initialize Web3 connection to Ethereum via Alchemy
web3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

# Kafka Producer configuration
kafka_config = {'bootstrap.servers': KAFKA_SERVER if KAFKA_SERVER else 'localhost:9092'}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    """ Callback called once message delivered to Kafka """
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

print("--- Starting Bronze Layer: Real-time Data Ingestion ---")

# Track the last processed block to prevent data gaps or duplicates
try:
    last_processed_block = web3.eth.block_number
except Exception as e:
    print(f"Connection Error: Ensure your ALCHEMY_URL is correct. Details: {e}")
    exit()

while True:
    try:
        current_block = web3.eth.block_number
        
        if current_block > last_processed_block:
            # Process all blocks generated since the last check
            for b_num in range(last_processed_block + 1, current_block + 1):
                print(f"Processing Block: {b_num}")
                block = web3.eth.get_block(b_num, full_transactions=True)
                
                for tx in block.transactions:
                    # Enrich payload with technical metadata for security analysis
                    tx_payload = {
                        'tx_hash': tx['hash'].hex(),
                        'sender': tx['from'],
                        'receiver': tx['to'],
                        'value_eth': float(web3.from_wei(tx['value'], 'ether')),
                        'gas': tx['gas'],
                        'gas_price': tx['gasPrice'],
                        'nonce': tx['nonce'],
                        'block_num': b_num,
                        'timestamp': int(time.time())
                    }
                    
                    # Produce data to Kafka topic 'raw_transactions'
                    producer.produce(
                        'raw_transactions',
                        json.dumps(tx_payload).encode('utf-8'),
                        callback=delivery_report
                    )
                
                producer.flush() # Ensure all messages are sent for this block
                last_processed_block = b_num
                
        time.sleep(10) # Wait for new block mining
        
    except Exception as e:
        print(f"Runtime Error in Producer: {e}")
        time.sleep(5)