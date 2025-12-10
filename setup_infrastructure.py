
import boto3
import json
import time
import os

# --- Configuration ---
AWS_REGION = "us-east-1"
PROJECT_NAME = "fraud-detection-system"
# Append a random suffix or user-specific ID to ensure uniqueness if needed
BUCKET_NAME = f"{PROJECT_NAME}-datalake-{int(time.time())}" 
QUEUE_NAME = "FraudTransactionsQueue"
DYNAMODB_TABLE_USER = "UserProfileTable"
DYNAMODB_TABLE_CASES = "FraudCasesTable"
IAM_ROLE_NAME = "FraudDetectionRole"
ATHENA_DB = "fraud_detection_db"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/athena-query-results/"

def create_s3_bucket(s3_client):
    print(f"Creating S3 bucket: {BUCKET_NAME}...")
    try:
        if AWS_REGION == "us-east-1":
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
        print(f"S3 Bucket created: {BUCKET_NAME}")
        return True
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")
        return False

def create_sqs_queue(sqs_client):
    print(f"Creating SQS Queue: {QUEUE_NAME}...")
    try:
        response = sqs_client.create_queue(QueueName=QUEUE_NAME)
        queue_url = response['QueueUrl']
        print(f"SQS Queue created: {queue_url}")
        return queue_url
    except Exception as e:
        print(f"Error creating SQS queue: {e}")
        return None

def create_dynamodb_tables(dynamodb_client):
    print("Creating DynamoDB tables...")
    
    # UserProfileTable
    try:
        dynamodb_client.create_table(
            TableName=DYNAMODB_TABLE_USER,
            KeySchema=[{'AttributeName': 'userId', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'userId', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"Table created: {DYNAMODB_TABLE_USER}")
    except Exception as e:
        if "ResourceInUseException" in str(e):
            print(f"Table {DYNAMODB_TABLE_USER} already exists.")
        else:
            print(f"Error creating {DYNAMODB_TABLE_USER}: {e}")

    # FraudCasesTable
    try:
        dynamodb_client.create_table(
            TableName=DYNAMODB_TABLE_CASES,
            KeySchema=[{'AttributeName': 'caseId', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'caseId', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"Table created: {DYNAMODB_TABLE_CASES}")
    except Exception as e:
        if "ResourceInUseException" in str(e):
            print(f"Table {DYNAMODB_TABLE_CASES} already exists.")
        else:
            print(f"Error creating {DYNAMODB_TABLE_CASES}: {e}")

def create_athena_resources(athena_client):
    print(f"Creating Athena Database: {ATHENA_DB}...")
    try:
        # 1. Create Database
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB};"
        athena_client.start_query_execution(
            QueryString=create_db_query,
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        print(f"Athena Database creation initiated: {ATHENA_DB}")
        time.sleep(5) # Wait for DB creation

        # 2. Create Table
        print("Creating Athena Table...")
        create_table_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.transactions (
            transactionId STRING,
            userId STRING,
            merchantId STRING,
            amount DOUBLE,
            latitude DOUBLE,
            longitude DOUBLE,
            timestamp STRING,
            ipAddress STRING,
            cardHash STRING,
            isFraud BOOLEAN
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION 's3://{BUCKET_NAME}/transactions/raw/';
        """
        athena_client.start_query_execution(
            QueryString=create_table_query,
            QueryExecutionContext={'Database': ATHENA_DB},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        print("Athena Table creation initiated.")
        return True
    except Exception as e:
        print(f"Error creating Athena resources: {e}")
        return False

def main():
    print("Starting Infrastructure Setup (SQS Version)...")
    
    # Initialize clients
    s3 = boto3.client('s3', region_name=AWS_REGION)
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    athena = boto3.client('athena', region_name=AWS_REGION)
    sts = boto3.client('sts')
    
    account_id = sts.get_caller_identity()["Account"]
    print(f"Account ID: {account_id}")
    
    # 1. Create S3 Bucket
    create_s3_bucket(s3)
    
    # 2. Create SQS Queue
    queue_url = create_sqs_queue(sqs)
    
    # 3. Create DynamoDB Tables
    create_dynamodb_tables(dynamodb)
    
    # 4. Create Athena Resources
    create_athena_resources(athena)
        
    print("\nSetup Complete!")
    print(f"S3 Bucket: {BUCKET_NAME}")
    print(f"SQS Queue: {QUEUE_NAME}")
    
    # Save config to file for other scripts to use
    config = {
        "BUCKET_NAME": BUCKET_NAME,
        "QUEUE_NAME": QUEUE_NAME, # Changed from STREAM_NAME
        "QUEUE_URL": queue_url,
        "AWS_REGION": AWS_REGION,
        "DYNAMODB_TABLE_USER": DYNAMODB_TABLE_USER,
        "DYNAMODB_TABLE_CASES": DYNAMODB_TABLE_CASES,
        "ATHENA_DB": ATHENA_DB,
        "ATHENA_OUTPUT": ATHENA_OUTPUT
    }
    with open("infra_config.json", "w") as f:
        json.dump(config, f)
    print("Configuration saved to infra_config.json")

if __name__ == "__main__":
    main()
