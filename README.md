# Sistema de Detección de Fraude en Tiempo Real con AWS (Edición SQS)

![AWS](https://img.shields.io/badge/AWS-Serverless-orange)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Status](https://img.shields.io/badge/Status-Portfolio%20Ready-green)

Un pipeline de datos completo para detectar transacciones financieras fraudulentas en tiempo real, optimizado para bajo costo y simplicidad.

## 🏗️ Arquitectura

El sistema utiliza una arquitectura desacoplada diseñada para fiabilidad y facilidad de pruebas:

1.  **Ingesta de Datos**: `AWS SQS` (Simple Queue Service) almacena temporalmente el alto volumen de transacciones.
2.  **Procesamiento**: `fraud_detector_service.py` (Lambda Local) procesa transacciones, realiza ingeniería de características y verificaciones de fraude usando modelos de ML.
3.  **Almacenamiento**: `DynamoDB` guarda perfiles de usuario y casos de fraude detectados.
4.  **Visualización**: El dashboard de `Streamlit` provee monitoreo en tiempo real del fraude detectado.

## 🚀 Características Clave

*   **Detección en Tiempo Real**: Latencia < 1 segundo.
*   **Machine Learning**: Usa modelos XGBoost (Campeón/Retador) para predecir la probabilidad de fraude.
*   **Costo Optimizado**: Usa colas SQS Estándar y DynamoDB bajo demanda para mantenerse dentro del AWS Free Tier.
*   **Infraestructura como Código**: Scripts de Python usando `boto3` para aprovisionar toda la pila tecnológica.

## 🛠️ Requisitos Previos

*   **AWS CLI**: Instalado y configurado (`aws configure`).
*   **Python 3.13+**: (Referenciado en `.venv`).
*   **Entorno Virtual**: Activado o accesible vía `.venv\Scripts\python.exe`.

## 🏁 Guía de Ejecución (Paso a Paso)

Sigue estos pasos para ejecutar el sistema completo.

### 1. Preparar Entorno e Infraestructura
Primero, asegura que las dependencias estén instaladas y los recursos de AWS existan.

```powershell
# Instalar librerías
.venv\Scripts\python.exe -m pip install -r requirements.txt

# Crear Cola SQS y Tablas DynamoDB
.venv\Scripts\python.exe setup_infrastructure.py

# Generar Modelos de ML (si faltan)
.venv\Scripts\python.exe FraudDetectionSystem/03_ML_Model/train_models.py
```

### 2. Ejecutar el Sistema (El Método de 3 Terminales)
Necesitas abrir **3 terminales separadas** para simular un ambiente de microservicios real.

#### Terminal 1: Servicio Detector de Fraude 🧠
Este servicio escucha la Cola SQS, carga los modelos de ML y procesa las transacciones entrantes.
```powershell
.venv\Scripts\python.exe FraudDetectionSystem/02_Lambda_Processor/fraud_detector_service.py
```
*Espera hasta ver: "🎧 Listening to SQS Queue..."*

#### Terminal 2: Generador de Transacciones 💳
Este script simula el comportamiento de compra de usuarios. Inyecta transacciones legítimas y fraude ocasional (tasa del 20%).
```powershell
.venv\Scripts\python.exe FraudDetectionSystem/01_Data_Generator/transaction_generator.py
```
*Verás: "Enviado LEGÍTIMA" o "Enviado FRAUDE SIMULADO"*

#### Terminal 3: Dashboard de Visualización 📊
Visualización en tiempo real de los casos de fraude detectados.
```powershell
.venv\Scripts\python.exe -m streamlit run dashboard.py
```
*Esto abrirá tu navegador predeterminado en http://localhost:8501*

## ⚠️ Limpieza (Teardown)
Para evitar costos, borra los recursos cuando termines.
```powershell
.venv\Scripts\python.exe teardown_infrastructure.py
```

## 📂 Estructura del Proyecto
*   `setup_infrastructure.py`: Aprovisionamiento de infraestructura.
*   `FraudDetectionSystem/01_Data_Generator/`: Simulación de tráfico.
*   `FraudDetectionSystem/02_Lambda_Processor/`: Lógica central y consumidor SQS.
*   `FraudDetectionSystem/03_ML_Model/`: Scripts de entrenamiento y artefactos de modelos.
*   `dashboard.py`: Frontend en Streamlit.
