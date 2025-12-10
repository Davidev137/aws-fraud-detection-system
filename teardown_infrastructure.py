import boto3
import json
import time

# --- Configuration ---
# Try to load from config file, otherwise use defaults/hardcoded (which might fail if bucket name changed)
try:
    with open("infra_config.json", "r") as f:
        config = json.load(f)
        BUCKET_NAME = config["BUCKET_NAME"]
        STREAM_NAME = config["STREAM_NAME"]
        AWS_REGION = config["AWS_REGION"]
        DYNAMODB_TABLE_USER = config["DYNAMODB_TABLE_USER"]
        DYNAMODB_TABLE_CASES = config["DYNAMODB_TABLE_CASES"]
except FileNotFoundError:
    print("⚠️ infra_config.json not found. Using default names. This might fail for unique S3 buckets.")
    AWS_REGION = "us-east-1"
    STREAM_NAME = "FraudTransactionsStream"
    DYNAMODB_TABLE_USER = "UserProfileTable"
    DYNAMODB_TABLE_CASES = "FraudCasesTable"
    # Cannot guess bucket name if it had a timestamp, so user might need to input it manually if config is lost
    BUCKET_NAME = "fraud-detection-system-datalake-UNKNOWN" 
    ATHENA_DB = "fraud_detection_db"

ATHENA_OUTPUT_LOCATION = f"s3://{BUCKET_NAME}/athena-query-results/"
FIREHOSE_NAME = "fraud-detection-firehose"
IAM_ROLE_NAME = "FraudDetectionFirehoseRole"

def delete_athena_resources(athena_client):
    print(f"Deleting Athena Database: {ATHENA_DB}...")
    try:
        # Drop Table
        athena_client.start_query_execution(
            QueryString=f"DROP TABLE IF EXISTS {ATHENA_DB}.transactions;",
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        time.sleep(2)
        
        # Drop Database
        athena_client.start_query_execution(
            QueryString=f"DROP DATABASE IF EXISTS {ATHENA_DB};",
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        print(f"✅ Athena resources deletion initiated: {ATHENA_DB}")
    except Exception as e:
        print(f"❌ Error deleting Athena resources: {e}")

def delete_s3_bucket(s3_client):
    print(f"Deleting S3 bucket: {BUCKET_NAME}...")
    try:
        # Must empty bucket first
        objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        if 'Contents' in objects:
            print(f"  Emptying {len(objects['Contents'])} objects...")
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            s3_client.delete_objects(Bucket=BUCKET_NAME, Delete=delete_keys)
        
        s3_client.delete_bucket(Bucket=BUCKET_NAME)
        print(f"✅ S3 Bucket deleted: {BUCKET_NAME}")
    except Exception as e:
        print(f"❌ Error deleting S3 bucket: {e}")

def delete_kinesis_stream(kinesis_client):
    print(f"Deleting Kinesis Stream: {STREAM_NAME}...")
    try:
        kinesis_client.delete_stream(StreamName=STREAM_NAME)
        print(f"Waiting for stream {STREAM_NAME} to be deleted...")
        waiter = kinesis_client.get_waiter('stream_not_exists')
        waiter.wait(StreamName=STREAM_NAME)
        print(f"✅ Kinesis Stream deleted: {STREAM_NAME}")
    except Exception as e:
        print(f"❌ Error deleting Kinesis stream: {e}")

def delete_dynamodb_tables(dynamodb_client):
    print("Deleting DynamoDB tables...")
    for table in [DYNAMODB_TABLE_USER, DYNAMODB_TABLE_CASES]:
        try:
            dynamodb_client.delete_table(TableName=table)
            print(f"✅ Table deleted: {table}")
        except Exception as e:
            print(f"❌ Error deleting {table}: {e}")

def delete_firehose(firehose_client):
    print(f"Deleting Firehose: {FIREHOSE_NAME}...")
    try:
        firehose_client.delete_delivery_stream(DeliveryStreamName=FIREHOSE_NAME)
        print(f"✅ Firehose deleted: {FIREHOSE_NAME}")
    except Exception as e:
        print(f"❌ Error deleting Firehose: {e}")

def delete_iam_role(iam_client):
    print(f"Deleting IAM Role: {IAM_ROLE_NAME}...")
    try:
        # Detach policies first
        policies = iam_client.list_role_policies(RoleName=IAM_ROLE_NAME)
        for policy_name in policies['PolicyNames']:
            iam_client.delete_role_policy(RoleName=IAM_ROLE_NAME, PolicyName=policy_name)
            print(f"  Detached policy: {policy_name}")
            
        iam_client.delete_role(RoleName=IAM_ROLE_NAME)
        print(f"✅ IAM Role deleted: {IAM_ROLE_NAME}")
    except Exception as e:
        print(f"❌ Error deleting IAM role: {e}")

def main():
    print("⚠️  WARNING: This will DELETE all resources created by the setup script.")
    confirm = input("Are you sure? (type 'yes' to confirm): ")
    if confirm.lower() != 'yes':
        print("Aborting.")
        return

    # Initialize clients
    s3 = boto3.client('s3', region_name=AWS_REGION)
    kinesis = boto3.client('kinesis', region_name=AWS_REGION)
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    iam = boto3.client('iam', region_name=AWS_REGION)
    firehose = boto3.client('firehose', region_name=AWS_REGION)
    athena = boto3.client('athena', region_name=AWS_REGION)
    
    # 0. Delete Athena (before S3)
    delete_athena_resources(athena)
    
    # 1. Delete Firehose (depends on S3 and Kinesis)
    delete_firehose(firehose)
    
    # 2. Delete Kinesis
    delete_kinesis_stream(kinesis)
    
    # 3. Delete S3
    delete_s3_bucket(s3)
    
    # 4. Delete DynamoDB
    delete_dynamodb_tables(dynamodb)
    
    # 5. Delete IAM Role
    delete_iam_role(iam)
    
    print("\n✅ Teardown Complete!")
    
    # Remove config file
    if os.path.exists("infra_config.json"):
        os.remove("infra_config.json")
        print("🗑️  infra_config.json removed.")

if __name__ == "__main__":
    main()
