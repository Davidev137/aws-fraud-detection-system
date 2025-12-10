
import boto3
import json
import os
import time
import zipfile
import io

# --- Configuration ---
# Load infra config
try:
    with open("infra_config.json", "r") as f:
        infra_config = json.load(f)
except FileNotFoundError:
    print("Error: infra_config.json not found. Run setup_infrastructure.py first.")
    exit(1)

AWS_REGION = infra_config["AWS_REGION"]
QUEUE_NAME = infra_config["QUEUE_NAME"] 
QUEUE_URL = infra_config["QUEUE_URL"]
USER_PROFILE_TABLE = infra_config["DYNAMODB_TABLE_USER"]
FRAUD_CASES_TABLE = infra_config["DYNAMODB_TABLE_CASES"]
LAMBDA_ROLE_NAME = "FraudDetectorLambdaRole"
LAMBDA_FUNCTION_NAME = "FraudDetectorProcessor"
SNS_TOPIC_NAME = "FraudAlertsTopic"

# Clients
iam = boto3.client("iam", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
sns = boto3.client("sns", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]

def create_sns_topic():
    print(f"Creating SNS Topic: {SNS_TOPIC_NAME}...")
    try:
        response = sns.create_topic(Name=SNS_TOPIC_NAME)
        topic_arn = response['TopicArn']
        print(f"SNS Topic created: {topic_arn}")
        return topic_arn
    except Exception as e:
        print(f"Error creating SNS topic: {e}")
        return None

def create_lambda_role():
    print(f"Creating IAM Role: {LAMBDA_ROLE_NAME}...")
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    try:
        role = iam.create_role(
            RoleName=LAMBDA_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        role_arn = role['Role']['Arn']
        print(f"IAM Role created: {role_arn}")

        # Attach policies
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "arn:aws:logs:*:*:*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:Query",
                        "dynamodb:Scan"
                    ],
                    "Resource": [
                        f"arn:aws:dynamodb:{AWS_REGION}:{account_id}:table/{USER_PROFILE_TABLE}",
                        f"arn:aws:dynamodb:{AWS_REGION}:{account_id}:table/{FRAUD_CASES_TABLE}",
                        f"arn:aws:dynamodb:{AWS_REGION}:{account_id}:table/UserGraphTable" 
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes"
                    ],
                    "Resource": f"arn:aws:sqs:{AWS_REGION}:{account_id}:{QUEUE_NAME}"
                },
                {
                    "Effect": "Allow",
                    "Action": "sns:Publish",
                    "Resource": f"arn:aws:sns:{AWS_REGION}:{account_id}:{SNS_TOPIC_NAME}" 
                }
            ]
        }

        iam.put_role_policy(
            RoleName=LAMBDA_ROLE_NAME,
            PolicyName="FraudDetectorLambdaPolicy",
            PolicyDocument=json.dumps(policy_doc)
        )
        print("IAM Policy attached.")
        print("Waiting 10s for IAM propagation...")
        time.sleep(10)
        return role_arn

    except Exception as e:
        if "EntityAlreadyExists" in str(e):
            print(f"Role {LAMBDA_ROLE_NAME} already exists.")
            role = iam.get_role(RoleName=LAMBDA_ROLE_NAME)
            return role['Role']['Arn']
        print(f"Error creating IAM role: {e}")
        return None

def create_lambda_package():
    print("Creating Lambda deployment package...")
    mem_zip = io.BytesIO()
    
    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # 1. Add lambda_function.py
        zf.write("FraudDetectionSystem/02_Lambda_Processor/lambda_function.py", "lambda_function.py")
        
        # 2. Add graph_logic folder
        graph_logic_dir = "FraudDetectionSystem/02_Lambda_Processor/graph_logic"
        for root, dirs, files in os.walk(graph_logic_dir):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, "FraudDetectionSystem/02_Lambda_Processor")
                zf.write(filepath, arcname)
        
        # 3. Add models (champion/challenger)
        ml_dir = "FraudDetectionSystem/03_ML_Model"
        for folder in ["champion", "challenger"]:
            folder_path = os.path.join(ml_dir, folder)
            if os.path.exists(folder_path):
                for root, dirs, files in os.walk(folder_path):
                    for file in files:
                        filepath = os.path.join(root, file)
                        arcname = os.path.relpath(filepath, ml_dir)
                        zf.write(filepath, arcname)
            else:
                print(f"Warning: {folder} directory not found in {ml_dir}")

    print("Deployment package created.")
    return mem_zip.getvalue()

def deploy_lambda(role_arn, sns_topic_arn, zip_content):
    print(f"Deploying Lambda Function: {LAMBDA_FUNCTION_NAME}...")
    
    env_vars = {
        "USER_PROFILE_TABLE": USER_PROFILE_TABLE,
        "FRAUD_CASES_TABLE": FRAUD_CASES_TABLE,
        "SNS_TOPIC_ARN": sns_topic_arn
    }

    layers = [] 

    try:
        # check if exists
        try:
            lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)
            print("Function exists. Updating code...")
            lambda_client.update_function_code(
                FunctionName=LAMBDA_FUNCTION_NAME,
                ZipFile=zip_content
            )
            print("Code updated. Waiting for update to complete...")
            time.sleep(5)
            print("Updating configuration...")
            lambda_client.update_function_configuration(
                FunctionName=LAMBDA_FUNCTION_NAME,
                Role=role_arn,
                Environment={'Variables': env_vars},
                Runtime="python3.12",
                Timeout=30,
                MemorySize=512
            )
        except lambda_client.exceptions.ResourceNotFoundException:
            print("Creating new function...")
            lambda_client.create_function(
                FunctionName=LAMBDA_FUNCTION_NAME,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.lambda_handler",
                Code={'ZipFile': zip_content},
                Environment={'Variables': env_vars},
                Timeout=30,
                MemorySize=512,
                Layers=layers
            )
        
        print(f"Lambda Function deployed: {LAMBDA_FUNCTION_NAME}")
        return True
    
    except Exception as e:
        print(f"Error deploying Lambda: {e}")
        return False

def add_sqs_trigger():
    print(f"Adding SQS Trigger...")
    try:
        # Get queue ARN
        # SQS ARN format: arn:aws:sqs:region:account_id:queue_name
        # We can construct it manually or fetch attributes
        response = sqs.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=['QueueArn'])
        queue_arn = response['Attributes']['QueueArn']
        
        # List event source mappings
        mappings = lambda_client.list_event_source_mappings(
            FunctionName=LAMBDA_FUNCTION_NAME,
            EventSourceArn=queue_arn
        )
        
        if not mappings['EventSourceMappings']:
            lambda_client.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=LAMBDA_FUNCTION_NAME,
                BatchSize=10
            )
            print("SQS Trigger added.")
        else:
            print("SQS Trigger already exists.")
            
    except Exception as e:
        print(f"Error adding trigger: {e}")

def main():
    print("Starting Lambda Deployment (SQS Version)...")
    
    # 1. Create SNS
    sns_topic_arn = create_sns_topic()
    if not sns_topic_arn:
         sns_topic_arn = f"arn:aws:sns:{AWS_REGION}:{account_id}:{SNS_TOPIC_NAME}"

    # 2. Create IAM Role
    role_arn = create_lambda_role()
    if not role_arn:
        return

    # 3. Create Package
    zip_content = create_lambda_package()

    # 4. Deploy Function
    if deploy_lambda(role_arn, sns_topic_arn, zip_content):
        # 5. Add Trigger
        add_sqs_trigger()
        print("\nLambda Deployment Complete!")

if __name__ == "__main__":
    main()
