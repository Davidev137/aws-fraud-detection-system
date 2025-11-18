# app.py
# A local Streamlit dashboard for visualizing the fraud detection system's metrics.

import streamlit as st
import boto3
import pandas as pd
import time

# --- AWS & Athena Configuration ---
ATHENA_DATABASE = "fraud_detection_db"
ATHENA_OUTPUT_LOCATION = "s3://your-unique-bucket-name-fraud-detection/athena-query-results/"
# This is the S3 path where Firehose stores the raw transaction data
S3_DATA_PATH = "s3://your-unique-bucket-name-fraud-detection/transactions/raw/"

# Initialize boto3 client for Athena
# Note: You need to have your AWS credentials configured locally for this to work.
athena_client = boto3.client('athena')

def run_athena_query(query):
    """Executes a query on Athena and returns the results as a Pandas DataFrame."""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        query_execution_id = response['QueryExecutionId']

        # Poll for query completion
        state = 'RUNNING'
        while state in ['RUNNING', 'QUEUED']:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = result['QueryExecution']['Status']['State']
            if state == 'FAILED':
                st.error(f"Athena query failed: {result['QueryExecution']['Status']['StateChangeReason']}")
                return None
            elif state == 'SUCCEEDED':
                break
            time.sleep(2)

        # Get results
        results_paginator = athena_client.get_paginator('get_query_results')
        results_iter = results_paginator.paginate(
            QueryExecutionId=query_execution_id,
            PaginationConfig={'PageSize': 1000}
        )

        data = []
        columns = None
        for results_page in results_iter:
            if not columns:
                columns = [col['Label'] for col in results_page['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            for row in results_page['ResultSet']['Rows'][1:]: # Skip header row
                data.append([field.get('VarCharValue') for field in row['Data']])

        return pd.DataFrame(data, columns=columns)

    except Exception as e:
        st.error(f"An error occurred while querying Athena: {e}")
        return None

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("Fraud Detection System Dashboard")

# Placeholder for auto-refreshing data
placeholder = st.empty()

# You would need to create this database and table in Athena, pointing to your S3 data.
# Example DDL:
# CREATE EXTERNAL TABLE transactions (
#   transactionId STRING,
#   userId STRING,
#   ...
# ) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
# LOCATION 's3://your-bucket/transactions/raw/';

while True:
    with placeholder.container():
        # --- Metrics ---
        st.header("Real-Time Metrics")

        # For this example, we'll use placeholder queries.
        # You would replace these with actual Athena SQL queries.

        # 1. Transactions Per Minute
        st.subheader("Transactions Per Minute (TPM)")
        tpm_query = "SELECT count(*)/5.0 FROM transactions WHERE from_iso8601_timestamp(timestamp) > now() - interval '5' minute;"
        # tpm_df = run_athena_query(tpm_query) # Uncomment when Athena is set up
        # st.metric("TPM", f"{tpm_df.iloc[0,0]:.2f}" if tpm_df is not None else "N/A")
        st.metric("TPM", f"{random.randint(100, 200)}") # Placeholder

        # 2. Fraud Rate
        st.subheader("Fraud Rate (%)")
        fraud_rate_query = "SELECT (sum(case when isFraud = '1' then 1 else 0 end) * 100.0) / count(*) FROM transactions;"
        # fraud_rate_df = run_athena_query(fraud_rate_query)
        # st.metric("Fraud Rate", f"{fraud_rate_df.iloc[0,0]:.2f}%" if fraud_rate_df is not None else "N/A")
        st.metric("Fraud Rate", f"{random.uniform(0.5, 2.5):.2f}%") # Placeholder

        # 3. Model A/B Comparison (this would require logging model version with the transaction)
        st.subheader("Model Performance (A vs B)")
        col1, col2 = st.columns(2)
        col1.metric("Champion (V1) Fraud Rate", f"{random.uniform(0.6, 1.5):.2f}%")
        col2.metric("Challenger (V2) Fraud Rate", f"{random.uniform(0.7, 1.8):.2f}%")

        # 4. Top Fraudulent Merchants
        st.subheader("Top 5 Merchants with Highest Fraud")
        top_merchants_query = "SELECT merchantId, count(*) as fraud_count FROM transactions WHERE isFraud = '1' GROUP BY merchantId ORDER BY fraud_count DESC LIMIT 5;"
        # top_merchants_df = run_athena_query(top_merchants_query)
        # st.dataframe(top_merchants_df if top_merchants_df is not None else pd.DataFrame())
        st.dataframe(pd.DataFrame({
            "merchantId": [f"merchant_{random.randint(1,100)}" for _ in range(5)],
            "fraud_count": sorted([random.randint(5, 50) for _ in range(5)], reverse=True)
        }))

    time.sleep(15) # Refresh every 15 seconds
