# lambda_function.py
# The core processing logic for the real-time fraud detection system.

import json
import base64
import random
import joblib
import boto3
import os
from datetime import datetime
from decimal import Decimal

# (Best practice) Use a separate module for graph logic
from graph_logic.graph_queries import is_linked_to_fraud

# --- Environment & AWS Clients ---
# These would be set in the Lambda environment variables
USER_PROFILE_TABLE = os.environ.get("USER_PROFILE_TABLE", "UserProfileTable")
CHAMPION_MODEL_PATH = "champion/model_v1.joblib"
CHALLENGER_MODEL_PATH = "challenger/model_v2.joblib"
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:ACCOUNT_ID:fraud-alerts-topic")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/ACCOUNT_ID/fraud-cases-queue")

# Initialize clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
sqs = boto3.client('sqs')
user_profile_table = dynamodb.Table(USER_PROFILE_TABLE)

# --- Model Loading ---
# Load models into memory outside the handler for reuse across invocations (Lambda optimization)
try:
    champion_model = joblib.load(CHAMPION_MODEL_PATH)
    challenger_model = joblib.load(CHALLENGER_MODEL_PATH)
    print("Successfully loaded Champion and Challenger models.")
except Exception as e:
    print(f"FATAL: Could not load models. {e}")
    # This would cause the Lambda initialization to fail, which is intended.
    champion_model = None
    challenger_model = None

def get_user_profile(user_id):
    """Fetches the user's profile from DynamoDB."""
    try:
        response = user_profile_table.get_item(Key={'userId': user_id})
        return response.get('Item', {})
    except Exception as e:
        print(f"Error fetching profile for user {user_id}: {e}")
        return {}

def feature_engineering(transaction, user_profile):
    """Creates model features from raw transaction data and user profile."""
    # Time-based features
    txn_time = datetime.fromisoformat(transaction['timestamp'])
    transaction['hour_of_day'] = txn_time.hour
    transaction['day_of_week'] = txn_time.weekday()

    # User history features
    transaction['transaction_count_30min'] = user_profile.get('transaction_count_30min', 0)

    # Geospatial anomaly (distance from last known location)
    last_lat = user_profile.get('last_latitude')
    last_lon = user_profile.get('last_longitude')
    if last_lat and last_lon:
        # Simple distance calculation (not geographically precise but good for a model)
        geo_distance = ((transaction['latitude'] - last_lat)**2 + (transaction['longitude'] - last_lon)**2)**0.5
        transaction['geo_distance_anomaly'] = geo_distance
    else:
        transaction['geo_distance_anomaly'] = 0

    return transaction

def make_prediction(features_df):
    """Performs A/B testing to get a model prediction."""
    if random.random() < 0.90: # 90% traffic to champion
        model = champion_model
        model_version = "v1_champion"
    else: # 10% to challenger
        model = challenger_model
        model_version = "v2_challenger"

    prediction_proba = model.predict_proba(features_df)[:, 1][0]
    is_fraud = prediction_proba > 0.8 # Decision threshold

    return is_fraud, prediction_proba, model_version

def lambda_handler(event, context):
    """Main Lambda handler triggered by Kinesis."""
    if not champion_model or not challenger_model:
        print("Models are not loaded. Aborting.")
        return {'statusCode': 500, 'body': 'Internal Server Error: Models failed to load.'}

    for record in event['Records']:
        try:
            payload_b64 = record['kinesis']['data']
            payload = json.loads(base64.b64decode(payload_b64))
            print(f"Processing transaction: {payload['transactionId']}")

            # 1. Feature Engineering
            user_profile = get_user_profile(payload['userId'])
            features = feature_engineering(payload, user_profile)

            # 2. Graph-based Fraud Check
            is_graph_fraud = is_linked_to_fraud(payload['userId'])

            # 3. ML Model Inference (A/B Test)
            model_features = [
                features['amount'], features['latitude'], features['longitude'],
                features['hour_of_day'], features['day_of_week']
            ]
            is_ml_fraud, confidence, model_version = make_prediction([model_features])

            # 4. Final Decision Logic
            # Combine signals: if either graph or ML model flags it, it's fraud.
            final_decision_is_fraud = is_graph_fraud or is_ml_fraud

            print(f"Decision: {'FRAUD' if final_decision_is_fraud else 'LEGITIMATE'}. "
                  f"Confidence: {confidence:.4f}. Model: {model_version}. Graph Flag: {is_graph_fraud}")

            if final_decision_is_fraud:
                # 5a. Fraud Actions
                # Send to SQS for Step Functions workflow
                sqs.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(payload, default=str)
                )
                # Send alert to SNS
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject=f"Fraud Alert for User {payload['userId']}",
                    Message=f"Transaction {payload['transactionId']} for ${payload['amount']} was flagged as fraudulent."
                )
            else:
                # 5b. Legitimate Actions
                # Update user profile (this would be more complex in reality)
                user_profile_table.update_item(
                    Key={'userId': payload['userId']},
                    UpdateExpression="SET last_latitude = :lat, last_longitude = :lon, last_transaction_time = :ts",
                    ExpressionAttributeValues={
                        ':lat': Decimal(str(payload['latitude'])),
                        ':lon': Decimal(str(payload['longitude'])),
                        ':ts': payload['timestamp']
                    }
                )

        except Exception as e:
            print(f"Error processing record: {e}")
            # Continue to next record

    return {'statusCode': 200, 'body': 'Processing complete.'}
