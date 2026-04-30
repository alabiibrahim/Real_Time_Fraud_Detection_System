# 🔍 Real-Time Fraud Detection System

> **A production-grade fraud detection pipeline built from scratch — streaming 6.3M transactions through Apache Kafka, scoring them with an XGBoost + Graph ML ensemble, and presenting live results on an analyst dashboard.**

<br>

[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![XGBoost](https://img.shields.io/badge/XGBoost-1.7+-FF6600?style=flat-square)](https://xgboost.readthedocs.io)
[![Streamlit](https://img.shields.io/badge/Streamlit-Latest-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

---

## 📊 Key Results

| Metric | Value |
|--------|-------|
| Transactions processed | 6,362,620 |
| ROC-AUC | **0.9999** |
| Precision-Recall AUC | **0.9835** |
| Fraud recall rate | **95%** |
| Fraud precision rate | **89%** |
| Fraud cases caught | 4,027 / 4,254 |
| Scoring latency | < 50ms per transaction |
| Build time (20hrs/week) | ~10 weeks |

---

## 🏗️ System Architecture

```
CSV Dataset
    │
    ▼
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────────────────┐
│  Producer   │────▶│   Apache Kafka   │────▶│          Consumer               │
│  (Python)   │     │  "transactions"  │     │                                 │
│             │     │     topic        │     │  ┌─────────────────────────┐    │
└─────────────┘     └──────────────────┘     │  │  Feature Engineering    │    │
                                             │  └────────────┬────────────┘    │
                                             │               │                 │
                                             │  ┌────────────▼────────────┐    │
                                             │  │   XGBoost Classifier    │    │
                                             │  │   (70% weight)          │    │
                                             │  └────────────┬────────────┘    │
                                             │               │                 │
                                             │  ┌────────────▼────────────┐    │
                                             │  │  Graph ML Ring Score    │    │
                                             │  │   (30% weight)          │    │
                                             │  └────────────┬────────────┘    │
                                             │               │                 │
                                             │  ┌────────────▼────────────┐    │
                                             │  │   Ensemble Decision     │    │
                                             │  │  FRAUD / REVIEW / OK    │    │
                                             │  └────────────┬────────────┘    │
                                             └───────────────┼─────────────────┘
                                                             │
                                                             ▼
                                                    ┌─────────────────┐
                                                    │   alerts.json   │
                                                    └────────┬────────┘
                                                             │
                                                             ▼
                                                    ┌─────────────────┐
                                                    │    Streamlit    │
                                                    │   Dashboard     │
                                                    │ localhost:8501  │
                                                    └─────────────────┘
```

---

## 🚀 Features

- **Real-time streaming** — Apache Kafka processes transactions as a live event stream, not a batch file
- **Ensemble fraud scoring** — XGBoost classifier (70%) + Graph ML ring scores (30%) combined for maximum detection coverage
- **Graph fraud ring detection** — NetworkX identifies organised fraud rings that tabular models miss entirely
- **SHAP explainability** — every flagged transaction includes a plain-English explanation of why it was flagged
- **Live analyst dashboard** — Streamlit dashboard auto-refreshes every 5 seconds with metrics, charts, and review queue
- **Feedback loop** — analyst decisions are logged and form the training data for weekly model retraining
- **Dockerised infrastructure** — one-command startup, runs identically on any machine

---

## 📁 Project Structure

```
fraud-detection-system/
│
├── docker/
│   └── docker-compose.yml          # Kafka + ZooKeeper infrastructure
│
├── producer/
│   └── producer.py                 # Streams CSV transactions into Kafka
│
├── consumer/
│   └── consumer.py                 # Scores transactions with ensemble model
│
├── model/
│   ├── train_model.py              # XGBoost training + SHAP generation
│   ├── fraud_model.pkl             # Trained model (generated)
│   ├── feature_cols.pkl            # Feature column list (generated)
│   ├── shap_global.png             # SHAP feature importance chart (generated)
│   └── shap_dot.png                # SHAP dot plot (generated)
│
├── graph/
│   └── graph_detector.py           # Graph ML fraud ring detection
│
├── dashboard/
│   ├── app.py                      # Streamlit dashboard
│   └── alert_store.py              # Alert queue manager
│
├── data/
│   ├── fraud_detect_data.csv       # PaySim dataset (add manually)
│   ├── fraud_model.pkl             # Model copy for Docker access
│   ├── feature_cols.pkl            # Feature copy for Docker access
│   ├── ring_scores.pkl             # Graph scores (generated)
│   └── alerts.json                 # Live alert queue (generated)
│
└── start.bat                       # One-click startup (Windows)
```

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.8+
- Anaconda / Miniconda
- Docker Desktop
- 8GB RAM minimum (16GB recommended for 6M rows)

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/fraud-detection-system.git
cd fraud-detection-system
```

### 2. Create conda environment

```bash
conda create -n fraud-detection-system python=3.9
conda activate fraud-detection-system
```

### 3. Install dependencies

```bash
pip install kafka-python pandas numpy xgboost shap scikit-learn joblib networkx streamlit plotly
```

### 4. Download the dataset

Download the **PaySim dataset** from Kaggle:
[https://www.kaggle.com/datasets/ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1)

Place `fraud_detect_data.csv` in the `data/` folder.

### 5. Update file paths

In the following files, update the file paths to match your system:
- `producer/producer.py` — `DATA_FILE_PATH`
- `model/train_model.py` — CSV path and model output paths
- `graph/graph_detector.py` — CSV path and output paths
- `dashboard/alert_store.py` — `ALERT_FILE` path
- `consumer/consumer.py` — model and ring score paths

---

## 🔧 One-Time Build (Run Before First Use)

### Step 1 — Train the XGBoost model

```bash
conda activate fraud-detection-system
python model/train_model.py
```

This will:
- Load and engineer features from the dataset
- Train XGBoost with class imbalance handling (`scale_pos_weight=1284`)
- Evaluate on a time-based test split
- Generate SHAP explainability plots
- Save `fraud_model.pkl` and `feature_cols.pkl`

Expected output:
```
Loaded: (6362620, 11)
Fraud rate: 0.1291%
Features engineered.
Train: (5090096, 13) | Test: (1272524, 13)
scale_pos_weight: 1284.7
[0]     validation_0-aucpr: 0.91712
[338]   validation_0-aucpr: 0.98318
Training complete.
ROC-AUC: 0.9999 | PR-AUC: 0.9835
```

### Step 2 — Build the fraud ring graph

```bash
python graph/graph_detector.py
```

This will:
- Build a directed transaction graph (nodes = accounts, edges = transactions)
- Run community detection to find connected account clusters
- Score each account for ring risk (0–1)
- Save `ring_scores.pkl`

### Step 3 — Copy model files to data folder

```bash
# Windows Command Prompt
copy model\fraud_model.pkl data\
copy model\feature_cols.pkl data\
copy model\ring_scores.pkl data\
```

---

## ▶️ Running the System

Open **4 separate terminals** (Anaconda Prompt recommended on Windows):

```bash
# Terminal 1 — Start infrastructure
cd docker
docker compose up -d

# Terminal 2 — Start consumer (wait for "Consumer running..." before starting producer)
conda activate fraud-detection-system
python consumer/consumer.py

# Terminal 3 — Start producer
conda activate fraud-detection-system
python producer/producer.py

# Terminal 4 — Start dashboard
conda activate fraud-detection-system
cd dashboard
streamlit run app.py
```

Open your browser at **http://localhost:8501**

---

## 🖥️ Dashboard Preview

The Streamlit dashboard includes:

| Component | Description |
|-----------|-------------|
| **Metric cards** | Total alerts, fraud count, review count, amount at risk, pending decisions |
| **Alerts over time** | Line chart of alerts per minute — detects fraud spikes |
| **Score distribution** | Histogram of final scores by status |
| **Fraud by type** | Bar chart confirming model focuses on CASH_OUT and TRANSFER |
| **XGBoost vs Graph scatter** | Shows where graph ML adds signal beyond XGBoost alone |
| **Alert queue** | Expandable cards — confirm fraud, mark legitimate, or escalate |
| **Recent alerts table** | Last 100 alerts with all scores and analyst decisions |

---

## 🤖 Model Details

### Feature Engineering

| Feature | Signal Captured |
|---------|----------------|
| `amount_to_balance_ratio` | How much of the account was moved — fraudsters drain accounts completely |
| `orig_drained` | Binary: did the origin balance hit zero? Near-certain fraud signal |
| `orig_balance_change` | Net change in origin account — always large and negative for fraud |
| `dest_balance_change` | Net change in destination — mule accounts receive and forward funds |
| `zero_orig_before` | Was origin already empty? Flags ghost/front accounts |
| `zero_dest_before` | Was destination empty? New mule accounts start at zero |
| `type_encoded` | Transaction type encoded — fraud only occurs in TRANSFER and CASH_OUT |

### Ensemble Scoring

```
final_score = (0.70 × xgb_score) + (0.30 × graph_ring_score)

FRAUD   → final_score > 0.70
REVIEW  → 0.40 < final_score ≤ 0.70
OK      → final_score ≤ 0.40
```

### Why Graph ML?

XGBoost scores each transaction in isolation. Fraudsters operate in rings — multiple accounts coordinating transfers and cash-outs. Graph ML identifies these structural patterns:

- **High fan-out ratio** — one account sending to many recipients (money mule hub)
- **Large community size** — tightly connected account clusters (organised ring)
- **Fraud connections** — direct links to confirmed fraud transactions

A transaction scoring `0.32` on XGBoost (would be cleared) but `0.90` on the graph produces a final score of `0.49` — correctly flagged for **REVIEW**. That is the extra protection layer graph ML adds.

### SHAP Explainability

Every prediction includes SHAP-based explanation:

```
Transaction #X — Fraud probability: 94.2%
Top contributing features:
  amount_to_balance_ratio    +0.62  (amount was 12x account balance)
  orig_drained               +0.21  (origin account fully emptied)
  type_encoded               +0.08  (TRANSFER — high-risk type)
  step                       +0.03
  oldbalanceOrg              -0.01
```

---

## 📈 Performance Metrics

```
              precision    recall  f1-score   support

       Legit       1.00      1.00      1.00   1268270
       Fraud       0.89      0.95      0.92      4254

    accuracy                           1.00   1272524

ROC-AUC: 0.9999
PR-AUC:  0.9835

Confusion Matrix:
  True Positives  (caught fraud):    4,027
  False Negatives (missed fraud):      227
  False Positives (wrongly flagged):   483
```

---

## 🛠️ Tech Stack

| Tool | Version | Role |
|------|---------|------|
| Apache Kafka | 7.4.0 | Real-time message streaming |
| Docker Compose | Latest | Infrastructure orchestration |
| Python | 3.8+ | Core language |
| Pandas | Latest | Data loading and feature engineering |
| XGBoost | 1.7+ | Fraud classification model |
| SHAP | 0.43+ | Model explainability |
| Scikit-learn | Latest | Evaluation metrics |
| NetworkX | Latest | Graph construction and analysis |
| Joblib | Latest | Model serialisation |
| kafka-python | Latest | Kafka client |
| Streamlit | Latest | Dashboard framework |
| Plotly | Latest | Interactive charts |

---

## 🗺️ Roadmap

- [ ] Replace file-based alert store with PostgreSQL for concurrent access
- [ ] Add automated weekly retraining pipeline with Apache Airflow
- [ ] Implement model drift monitoring and alerting
- [ ] Add SMOTE for improved rare fraud pattern detection
- [ ] Deploy to AWS (MSK + ECS + RDS + S3)
- [ ] Add Isolation Forest as a third ensemble member
- [ ] REST API endpoint for real-time scoring integration

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgements

- **Dataset**: [PaySim — A financial mobile money simulator](https://www.kaggle.com/datasets/ealaxi/paysim1) by Edgar Lopez-Rojas
- **Inspiration**: Real-world fraud detection systems used at banks and fintech companies globally

---

<p align="center">
  Built as a financial analyst portfolio project — from zero to production in 10 weeks.
</p>
