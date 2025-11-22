# transaction_generator.py
# Simula transacciones bancarias en tiempo real con patrones de fraude.

import boto3
import json
import random
import time
from datetime import datetime, timedelta
import uuid

# --- Configuration ---
# ¡IMPORTANTE! Asegúrate de que este nombre coincida con el creado en AWS CLI.
STREAM_NAME = "FraudTransactionsStream" 
AWS_REGION = "us-east-1" 
# Probabilidad base de que una transacción sea catalogada como Fraude (5%)
BASE_FRAUD_PROBABILITY = 0.05 

# Initialize the Kinesis client
try:
    # Usar el perfil 'default' configurado en la CLI
    kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)
    print("Conexión a Kinesis exitosa. Generando transacciones...")
except Exception as e:
    print(f"Error al conectar a Kinesis. Revisa tu 'aws configure': {e}")
    # Usamos sys.exit() para parar la ejecución si no podemos conectar.
    import sys
    sys.exit()

# --- Data Simulation Parameters ---
# IDs que usaremos
USER_IDS = [f"user_{i}" for i in range(1, 1000)] # 1000 usuarios
MERCHANT_IDS = [f"merchant_{i}" for i in range(1, 200)]
# Rangos de monto
NORMAL_AMOUNT_RANGE = (10.00, 150.00)
RISKY_AMOUNT_RANGE = (500.00, 3000.00) 
# Coordenadas geográficas
INITIAL_GEO_LOCATION = {"lat": (34.0, 36.0), "lon": (-118.0, -116.0)} # California (USA)
DRIFT_GEO_LOCATION = {"lat": (40.5, 41.0), "lon": (-74.0, -73.5)} # New York (USA)
# IPs y Tarjetas (para simular reglas de Blacklist/Grafos)
BLACKLIST_IP = "10.10.10.10"
BLACKLIST_CARD = "4000123456789012"

def generate_transaction(user_id, amount_range, geo_location, is_drift_scenario=False):
    """Genera un registro de transacción con lógica de fraude."""
    
    # 1. Base de la transacción
    record = {
        "transactionId": str(uuid.uuid4()),
        "userId": user_id,
        "merchantId": random.choice(MERCHANT_IDS),
        "amount": round(random.uniform(*amount_range), 2),
        "latitude": round(random.uniform(*geo_location["lat"]), 6),
        "longitude": round(random.uniform(*geo_location["lon"]), 6),
        "timestamp": datetime.utcnow().isoformat(),
        # Datos para reglas y grafos
        "ipAddress": f"192.168.1.{random.randint(10, 200)}",
        "cardHash": str(random.randint(1000000000000000, 9999999999999999))
    }
    
    # 2. Lógica de Fraude (Etiqueta "isFraud" para medir el rendimiento del ML)
    is_fraud = False
    
    # Fraude aleatorio
    if random.random() < BASE_FRAUD_PROBABILITY:
        is_fraud = True
        
    # Patrón de Fraude A: Fraude de alto monto o Blacklist
    if record["amount"] > 1000.00 or record["cardHash"] == BLACKLIST_CARD:
        is_fraud = True
        
    # Patrón de Fraude B (Concept Drift - El Fraude ha cambiado de ubicación/patrón)
    if is_drift_scenario:
        # El fraude en esta fase está en la nueva ubicación (New York) y es de alto riesgo
        if random.random() < 0.20: # 20% de probabilidad de fraude en la fase Drift
            is_fraud = True
            record["ipAddress"] = BLACKLIST_IP # Introduce una IP blacklisteada en el nuevo patrón

    record["isFraud"] = is_fraud
    
    return record

def send_to_kinesis(transaction_record):
    """Envía el registro JSON al stream Kinesis especificado."""
    try:
        kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(transaction_record).encode('utf-8'), # Codificar a bytes
            PartitionKey=transaction_record["userId"] # Clave para asegurar el orden por usuario
        )
        return True
    except Exception as e:
        # Esto imprimirá un error si el stream no existe o si las credenciales fallan
        print(f"Error al enviar registro a Kinesis: {e}")
        return False

def main():
    """Bucle principal para generar y enviar transacciones."""
    start_time = time.time()
    print(f"Comenzando el envío al stream '{STREAM_NAME}'...")

    while True:
        elapsed_time_minutes = (time.time() - start_time) / 60
        
        # --- Lógica de Drift (Cambio de patrón después de 10 minutos) ---
        is_drift = elapsed_time_minutes > 10

        if is_drift:
            # Fase de Drift: Patrones cambian a una nueva ubicación/monto
            current_user = random.choice(USER_IDS)
            current_amount_range = RISKY_AMOUNT_RANGE
            current_geo = DRIFT_GEO_LOCATION
            drift_message = f" [DRIFT: {elapsed_time_minutes:.1f}m]"
        else:
            # Fase Normal: Operaciones estándar
            current_user = random.choice(USER_IDS)
            current_amount_range = NORMAL_AMOUNT_RANGE
            current_geo = INITIAL_GEO_LOCATION
            drift_message = ""

        # Generar y enviar la transacción
        transaction = generate_transaction(current_user, current_amount_range, current_geo, is_drift)

        if send_to_kinesis(transaction):
            status = "FRAUDE SIMULADO" if transaction['isFraud'] else "LEGÍTIMA"
            print(f"Enviado {status} | User: {transaction['userId']} | Monto: {transaction['amount']} | Geo: {current_geo['lat'][0]:.1f}{drift_message}")
        
        # Control de la tasa: ~4 transacciones por segundo (240 por minuto)
        # Ajusta el 'sleep' si necesitas más de 1000 tx/min. Para Free Tier, esta tasa es segura.
        time.sleep(random.uniform(0.1, 0.3)) 

if __name__ == "__main__":
    main()