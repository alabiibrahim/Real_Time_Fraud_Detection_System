# Real_Time_Fraud_Detection_System_



- [ProjectOverview](#ProjectOverview)
- [Architecture](#Architecture)
- [Phase1](#Phase1)
- [Phase2](#Phase2)
- [Phase3](#Phase3)
- [Phase4](#Phase4)
- [ExceutionOrder](#ExceutionOrder)
- [Tools](#Tools)
- [Methodology](#Methodology)
- [Result](#Result)




![Visual](main/images/py1)

![Visual](main/images/py2)




## Project Overview

**Business Problem**

Financial institutions process millions of transactions daily. Manually reviewing all of them is impossible. This system automatically scores every transaction in real time,
flags suspicious activity, and presents it to an analyst for a final decision to reduce fraud losses, while minimising false alarms that frustrate legitimate customers.

**Objective**

This project builds a production-grade, real-time fraud detection system entirely from scratch covering data streaming, machine learning, graph analysis, explainability, and a live analyst dashboard. 

Analyze transaction-level data to flag suspicious patterns using anomaly detection and graph-based analysis. Visualize fraud networks and build an alert pipeline.

**Results**

| Measurable outcome | Results |
| --- | --- |
| Processed transactions (CSV) | 6,362,620 |
| ROC-AUC | 0.9999 |
| PR-AUC | 0.9834 |
| Fraud precision rate | 89% |
| Fraud recall rate | 95% |
| Fraud cases detected | 4,027 |
| Scoring latency | <50ms |


## Architecture


CSV Dataset ‣ Producer ‣ Kafka (Topic) ‣ Consumer ‣ 

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

## Tools

| Tools | Purpose |
| --- | --- | 
| Docker | Stream live data (docker containers) |
| Python (Pandas, Numpy, XGBoost, SHAP, Joblib, Scikit-Learn) | Data cleaning, feature engineering, train model |
| Apache-Kafka | Data process pipeline | 
| Streamlit | In place of Power-BI for dahsboard | 


## Methodology

- Setup Foundation & Data Pipeline. (Goal: To have a live transactions stream running.)
    - kafka locally on docker-desktop.
    - Stream fraud_detect datasets as transactions.
    - Build Feature engineering pipeline.
    - Store feature in a simple features store.

- Buil detection models. (Goal: Models scoring transactions in real time.)
    - Train Isolation Forest to score anomalies.
    - Trained XG-boost classifier and tune precision.
    - Build weighted score aggregate.
    - Add SHAP explainability layer.

- Graph ML & rule engine. (Goal: To make sure full pipeline is working.)
   - Build transactions grapgh with NetworkX
   - Detect Fraud ring using community detection.
   - Write velocity and threshold rule engine.
   - Integrate graph scores into esemble.

- Dashboard feedback loop (Streaming and restraining).
   - Build streamlit analyst dashboard.
   - Alert queue with block/allow decision.
   - Label feedback - model retaining loop.
   - 


## Result


## Challenges & Solution


**Challenge**
- The data pipeline processing performance was terrible and I noticed it takes 7-8 hrs to send 280,000 data, with that math, It's certain to achieve the full sending of 6m+ rows data will take 7-8 days maiking the project goal impossible.

**Solution**
- Instead of sending data rows by rows, I did a batch processing.
- Made changes to the chunk size to balance memory usage.
- Lastly, I noticed the timer is also contributing to this so i reduced it from 0.1 seconds to 0.001 seconds.

**Result**
- What was predicted to take 7-8 days before data fully sent now takes less than 2hrs. 95% improvement.
- Increased in data transmission of 35k rows per hour to 50k rows per minutes. 85% transmission speed.
- Real time fraud detection was successfully enabled.
