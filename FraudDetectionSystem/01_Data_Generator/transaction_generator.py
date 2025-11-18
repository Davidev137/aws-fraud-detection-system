# transaction_generator.py
# Simulates real-time banking transactions and sends them to an AWS Kinesis Data Stream.

import boto3
import json
import random
import time
from datetime import datetime

# --- Configuration ---
STREAM_NAME = "fraud-detection-stream"
AWS_REGION = "us-east-1" # Specify your AWS region

# Initialize the Kinesis client
try:
    kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)
    print("Successfully connected to Kinesis.")
except Exception as e:
    print(f"Error connecting to Kinesis: {e}")
    exit()

# --- Data Simulation Parameters ---
USER_IDS = [f"user_{i}" for i in range(1, 50)]
MERCHANT_IDS = [f"merchant_{i}" for i in range(1, 100)]
NORMAL_AMOUNT_RANGE = (5, 150)
HIGH_AMOUNT_RANGE = (500, 2000) # For drift scenario
INITIAL_GEO_LOCATION = {"lat": (34.0, 36.0), "lon": (-118.0, -116.0)} # Los Angeles area
DRIFT_GEO_LOCATION = {"lat": (40.5, 41.0), "lon": (-74.0, -73.5)} # New York area

def generate_transaction(user_id, amount_range, geo_location):
    """Generates a single transaction record."""
    return {
        "transactionId": f"txn_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        "userId": user_id,
        "merchantId": random.choice(MERCHANT_IDS),
        "amount": round(random.uniform(*amount_range), 2),
        "latitude": round(random.uniform(*geo_location["lat"]), 6),
        "longitude": round(random.uniform(*geo_location["lon"]), 6),
        "timestamp": datetime.utcnow().isoformat()
    }

def send_to_kinesis(transaction_record):
    """Sends a JSON record to the specified Kinesis stream."""
    try:
        kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(transaction_record),
            PartitionKey=transaction_record["userId"] # Use userId as partition key
        )
        return True
    except Exception as e:
        print(f"Error sending record to Kinesis: {e}")
        return False

def main():
    """Main loop to generate and send transactions."""
    start_time = time.time()
    print("Starting transaction generation...")

    while True:
        elapsed_time_minutes = (time.time() - start_time) / 60

        # --- Drift Scenario Logic ---
        # After 10 minutes, introduce a change in data patterns.
        if elapsed_time_minutes > 10:
            # Simulate a fraud ring starting in a new location with higher amounts
            current_user = f"fraud_ring_user_{random.randint(1, 5)}"
            current_amount_range = HIGH_AMOUNT_RANGE
            current_geo = DRIFT_GEO_LOCATION
        else:
            # Normal operations
            current_user = random.choice(USER_IDS)
            current_amount_range = NORMAL_AMOUNT_RANGE
            current_geo = INITIAL_GEO_LOCATION

        # Generate and send the transaction
        transaction = generate_transaction(current_user, current_amount_range, current_geo)

        if send_to_kinesis(transaction):
            print(f"Sent transaction {transaction['transactionId']} for user {transaction['userId']}")

        # Control the rate of transaction generation
        time.sleep(random.uniform(0.1, 0.5)) # Send a transaction every 100-500ms

if __name__ == "__main__":
    main()
