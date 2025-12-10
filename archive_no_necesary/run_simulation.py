
import boto3
import json
import time
import random
import uuid
from datetime import datetime
import sys

# --- Configuration ---
try:
    with open("infra_config.json", "r") as f:
        infra_config = json.load(f)
except FileNotFoundError:
    print("Error: infra_config.json not found.")
    sys.exit(1)

AWS_REGION = infra_config["AWS_REGION"]
QUEUE_URL = infra_config["QUEUE_URL"]

# sqs client
try:
    sqs_client = boto3.client('sqs', region_name=AWS_REGION)
    print(f"Connected to SQS Queue: {QUEUE_URL}")
except Exception as e:
    print(f"Error connecting to SQS: {e}")
    sys.exit(1)

# Parameters
USER_IDS = [f"user_{i}" for i in range(1, 1000)]
MERCHANT_IDS = [f"merchant_{i}" for i in range(1, 200)]
NORMAL_AMOUNT_RANGE = (10.00, 150.00)
RISKY_AMOUNT_RANGE = (500.00, 3000.00)
INITIAL_GEO_LOCATION = {"lat": (34.0, 36.0), "lon": (-118.0, -116.0)}
DRIFT_GEO_LOCATION = {"lat": (40.5, 41.0), "lon": (-74.0, -73.5)}
BLACKLIST_IP = "10.10.10.10"
BLACKLIST_CARD = "4000123456789012"
BASE_FRAUD_PROBABILITY = 0.01

def generate_transaction(user_id, amount_range, geo_location, is_drift_scenario=False):
    record = {
        "transactionId": str(uuid.uuid4()),
        "userId": user_id,
        "merchantId": random.choice(MERCHANT_IDS),
        "amount": round(random.uniform(*amount_range), 2),
        "latitude": round(random.uniform(*geo_location["lat"]), 6),
        "longitude": round(random.uniform(*geo_location["lon"]), 6),
        "timestamp": datetime.utcnow().isoformat(),
        "ipAddress": f"192.168.1.{random.randint(10, 200)}",
        "cardHash": str(random.randint(1000000000000000, 9999999999999999))
    }
    
    is_fraud = False
    if random.random() < BASE_FRAUD_PROBABILITY:
        is_fraud = True
    
    if record["amount"] > 1000.00 or record["cardHash"] == BLACKLIST_CARD:
        is_fraud = True
        
    if is_drift_scenario:
        if random.random() < 0.20:
            is_fraud = True
            record["ipAddress"] = BLACKLIST_IP 

    record["isFraud"] = is_fraud
    return record

def send_to_sqs(transaction_record):
    try:
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(transaction_record)
        )
        return True
    except Exception as e:
        print(f"Error sending to SQS: {e}")
        return False

def main():
    print(f"Starting simulation. Queue: {QUEUE_URL}")
    TOTAL_RECORDS = 20
    
    for i in range(TOTAL_RECORDS):
        is_drift = False 
        current_amount_range = NORMAL_AMOUNT_RANGE
        current_geo = INITIAL_GEO_LOCATION
        
        if i % 5 == 0:
             current_amount_range = RISKY_AMOUNT_RANGE 
        
        transaction = generate_transaction(random.choice(USER_IDS), current_amount_range, current_geo, is_drift)
        
        if send_to_sqs(transaction):
            status = "FRAUD (Simulated)" if transaction['isFraud'] else "LEGITIMATE"
            print(f"[{i+1}/{TOTAL_RECORDS}] Sent {status} | User: {transaction['userId']} | Amount: {transaction['amount']}")
        
        time.sleep(0.5)

    print("Simulation finished.")

if __name__ == "__main__":
    main()
