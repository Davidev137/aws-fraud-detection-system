
import streamlit as st
import boto3
import pandas as pd
import json
import time
import os
from datetime import datetime

# --- Configuration ---
# Helper to find config if run from different directories
CONFIG_PATH = "infra_config.json"
if not os.path.exists(CONFIG_PATH):
    # Try going up one level if run from subdir
    CONFIG_PATH = "../infra_config.json"
    if not os.path.exists(CONFIG_PATH):
        # Try two levels
        CONFIG_PATH = "../../infra_config.json"

try:
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
except FileNotFoundError:
    st.error("Infrastructure config not found. Run setup_infrastructure.py first.")
    st.stop()

AWS_REGION = config.get("AWS_REGION", "us-east-1")
FRAUD_TABLE_NAME = config.get("DYNAMODB_TABLE_CASES", "FraudCasesTable")

# --- AWS Clients ---
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(FRAUD_TABLE_NAME)

# --- Functions ---
def get_recent_fraud_cases(limit=100):
    try:
        # Scan is okay for demo. For prod, use GSI on timestamps.
        response = table.scan(Limit=limit)
        items = response.get('Items', [])
        return items
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return []

# --- Custom CSS for "Premium" feel ---
st.set_page_config(page_title="Fraud Detection Monitor", page_icon="🛡️", layout="wide")

st.markdown("""
<style>
    .main {
        background-color: #f5f5f5;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 {
        color: #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

# --- Main Layout ---
st.title("🛡️ Amazon Fraud Detection System")
st.markdown(f"**Status**: Connected to `{FRAUD_TABLE_NAME}` in `{AWS_REGION}`")

# Refresh button
if st.button('Refresh Data 🔄'):
    st.rerun()

# Data Fetching
data = get_recent_fraud_cases()

if not data:
    st.warning("No fraud cases found yet. Run the simulation script to generate traffic!")
else:
    df = pd.DataFrame(data)
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    
    total_cases = len(df)
    high_risk_cases = len(df[df['riskScore'] > 0.8]) if 'riskScore' in df.columns else 0
    avg_risk = df['riskScore'].mean() if 'riskScore' in df.columns else 0
    
    col1.metric("Fraud Cases Detected", total_cases)
    col2.metric("Avg Risk Score", f"{avg_risk:.2f}")
    col3.metric("High Risk Cases (>0.8)", high_risk_cases)
    
    # Tables and Charts
    st.subheader("🚨 Recent Fraud Alerts")
    
    # Reorder columns if they exist
    cols = ['caseId', 'userId', 'timestamp', 'riskScore', 'status']
    available_cols = [c for c in cols if c in df.columns]
    remaining_cols = [c for c in df.columns if c not in cols]
    
    st.dataframe(df[available_cols + remaining_cols].sort_values(by='timestamp', ascending=False), use_container_width=True)
    
    if 'riskScore' in df.columns:
        st.subheader("Risk Score Distribution")
        st.bar_chart(df['riskScore'])

# Auto-refresh logic (Simple)
time.sleep(8) # Refresh every 8 seconds
st.rerun()
