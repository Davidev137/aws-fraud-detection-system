
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score
import joblib
import os
import sys
import numpy as np

# --- Setup ---
# Create directories if they don't exist
os.makedirs('champion', exist_ok=True)
os.makedirs('challenger', exist_ok=True)

# Path to the dataset
# Adjust path to match script location
DATASET_PATH = 'dataset/historical_transactions.csv'

# --- DATA MOCKUP ---
if not os.path.exists(DATASET_PATH):
    print("WARNING: Creating synthetic dataset for testing purposes.")
    num_samples = 10000
    np.random.seed(42)
    
    data = {
        'timestamp': pd.date_range(start='2024-01-01', periods=num_samples, freq='min'),
        'user_id': np.random.randint(1000, 9999, num_samples),
        'amount': np.random.lognormal(mean=4, sigma=1, size=num_samples),
        'latitude': np.random.uniform(30, 40, num_samples),
        'longitude': np.random.uniform(-100, -90, num_samples),
        'isFraud': np.random.choice([0, 1], size=num_samples, p=[0.95, 0.05]) # 5% fraud
    }
    df = pd.DataFrame(data)
    
    # Introduce a slight pattern
    df.loc[(df['amount'] > df['amount'].quantile(0.95)) & (df['user_id'] < 3000), 'isFraud'] = 1
    
    os.makedirs('dataset', exist_ok=True)
    df.to_csv(DATASET_PATH, index=False)
    print(f"Synthetic dataset saved to: {DATASET_PATH}")
# --- END DATA MOCKUP ---

# Load the dataset
try:
    df = pd.read_csv(DATASET_PATH)
    print("Dataset loaded successfully.")
except FileNotFoundError:
    print(f"Error: '{DATASET_PATH}' not found. Exiting.")
    sys.exit()

# ## 2. Feature Engineering
# Convert timestamp to datetime object
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Create time-based features
df['hour_of_day'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.dayofweek

features = ['amount', 'latitude', 'longitude', 'hour_of_day', 'day_of_week']
target = 'isFraud'

X = df[features]
y = df[target]

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
print(f"Data split into {len(X_train)} training samples and {len(X_test)} testing samples.")
print(f"Fraud ratio in test set: {y_test.mean():.4f}")

# ## 3. Champion Model (V1) Training
print("\n--- Training Champion Model (V1) ---")
champion_model = xgb.XGBClassifier(
    objective='binary:logistic',
    eval_metric='auc',
    n_estimators=100,
    learning_rate=0.1,
    max_depth=3,
    random_state=42,
    scale_pos_weight=len(y_train[y_train==0]) / len(y_train[y_train==1])
)

champion_model.fit(X_train, y_train)

# Evaluate the champion model
y_pred_proba_champ = champion_model.predict_proba(X_test)[:, 1]
y_pred_champ = (y_pred_proba_champ > 0.5).astype(int)
print(f"Champion Model Accuracy: {accuracy_score(y_test, y_pred_champ):.4f}")
print(f"Champion Model AUC: {roc_auc_score(y_test, y_pred_proba_champ):.4f}")

# Save the model
joblib.dump(champion_model, 'champion/model_v1.joblib')
print("Champion model saved to 'champion/model_v1.joblib'")


# ## 4. Challenger Model (V2) Training
print("\n--- Training Challenger Model (V2) ---")
challenger_model = xgb.XGBClassifier(
    objective='binary:logistic',
    eval_metric='auc',
    n_estimators=150,    # More estimators (Challenger)
    learning_rate=0.05,  # Lower learning rate (Challenger)
    max_depth=4,         # Deeper trees (Challenger)
    random_state=123,    # Different random state
    scale_pos_weight=len(y_train[y_train==0]) / len(y_train[y_train==1])
)

challenger_model.fit(X_train, y_train)

# Evaluate the challenger model
y_pred_proba_chall = challenger_model.predict_proba(X_test)[:, 1]
y_pred_chall = (y_pred_proba_chall > 0.5).astype(int)
print(f"Challenger Model Accuracy: {accuracy_score(y_test, y_pred_chall):.4f}")
print(f"Challenger Model AUC: {roc_auc_score(y_test, y_pred_proba_chall):.4f}")

# Save the model
joblib.dump(challenger_model, 'challenger/model_v2.joblib')
print("Challenger model saved to 'challenger/model_v2.joblib'")
