# lambda_shap.py
# Asynchronous Lambda function to calculate SHAP values for fraud explainability.

import json
import joblib
import shap
import pandas as pd
import boto3
import os

# --- Environment & AWS Clients ---
FRAUD_CASES_TABLE = os.environ.get("FRAUD_CASES_TABLE", "FraudCasesTable")
EXPLAINABILITY_BUCKET = os.environ.get("EXPLAINABILITY_BUCKET", "your-unique-bucket-name-fraud-detection")
CHAMPION_MODEL_PATH = "champion/model_v1.joblib" # Assumes model is deployed with Lambda

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
fraud_cases_table = dynamodb.Table(FRAUD_CASES_TABLE)

# --- Model & Explainer Loading ---
# Load the model and create a SHAP explainer outside the handler for reuse.
try:
    # We only explain decisions from the champion model for consistency
    model = joblib.load(CHAMPION_MODEL_PATH)
    # Use the TreeExplainer for XGBoost models
    explainer = shap.TreeExplainer(model)
    print("Successfully loaded model and SHAP explainer.")
except Exception as e:
    print(f"FATAL: Could not load model or create explainer. {e}")
    model = None
    explainer = None

def calculate_shap_values(features):
    """Calculates SHAP values for a given set of features."""
    if not explainer:
        raise ValueError("SHAP explainer not initialized.")

    # Convert single prediction to a DataFrame
    feature_names = ['amount', 'latitude', 'longitude', 'hour_of_day', 'day_of_week']
    features_df = pd.DataFrame([features], columns=feature_names)

    # Get SHAP values
    shap_values = explainer.shap_values(features_df)

    # Format the output for easy interpretation
    explanation = {
        "base_value": explainer.expected_value,
        "shap_values": dict(zip(feature_names, shap_values[0].tolist()))
    }
    return explanation

def lambda_handler(event, context):
    """
    Main handler, triggered by SQS (which gets messages from the main Lambda).
    """
    if not model or not explainer:
        print("Model/Explainer not loaded. Aborting.")
        return {'statusCode': 500}

    for record in event['Records']: # SQS messages arrive in a list
        try:
            transaction = json.loads(record['body'])
            transaction_id = transaction['transactionId']
            case_id = f"case_{transaction_id}"
            print(f"Generating SHAP explanation for case: {case_id}")

            # 1. Prepare features for the model (same as in the main Lambda)
            # In a real system, you'd pass these engineered features in the message
            # or recalculate them consistently.
            from datetime import datetime
            txn_time = datetime.fromisoformat(transaction['timestamp'])
            hour = txn_time.hour
            day = txn_time.weekday()

            model_features = [
                transaction['amount'], transaction['latitude'], transaction['longitude'],
                hour, day
            ]

            # 2. Calculate SHAP values
            shap_explanation = calculate_shap_values(model_features)

            # 3. Store the explanation report
            report = {
                "caseId": case_id,
                "transactionDetails": transaction,
                "shapExplanation": shap_explanation,
                "reportGeneratedAt": datetime.utcnow().isoformat()
            }

            # Save report to S3
            s3_key = f"explainability-reports/{case_id}.json"
            s3.put_object(
                Bucket=EXPLAINABILITY_BUCKET,
                Key=s3_key,
                Body=json.dumps(report, default=str)
            )

            # Update the DynamoDB case table with the location of the report
            fraud_cases_table.update_item(
                Key={'caseId': case_id},
                UpdateExpression="SET shapReportS3Url = :url, status = :stat",
                ExpressionAttributeValues={
                    ':url': f"s3://{EXPLAINABILITY_BUCKET}/{s3_key}",
                    ':stat': "EXPLAINED"
                }
            )
            print(f"Successfully generated and stored SHAP report for {case_id}")

        except Exception as e:
            print(f"Error processing SHAP explanation for a record: {e}")

    return {'statusCode': 200}
