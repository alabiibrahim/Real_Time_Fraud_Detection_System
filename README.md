# Real_Time_Fraud_Detection_System_



- [ProjectGoal](#ProjectGoal)
- [Tools](#Tools)
- [Methodology](#Methodology)
- [Result](#Result)




![Visual](main/images/py1)

![Visual](main/images/py2)




## ProjectGoal

Analyze transaction-level data to flag suspicious patterns using anomaly detection and graph-based analysis. Visualize fraud networks and build an alert pipeline.



## Tools

| Tools | Purpose |
| --- | --- | 
| Docker | Stream live data (docker containers) |
| Python | train model, xgboost |
| Kafka | Batch files transfers | 
| SHAP | Classifier | 
| Streamlit | In place of Power-BI for dahsboard | 


## Methodology

- Setup Fondation & Data Pipeline. (Goal: To have a live transaction stream running.)
    - kafka locally on docker-desktop.
    - Stream fraud_detect datasets as transactions.
    - Build Feature engineering pipeline.
    - store feature in a simple features store.

- Buil detection models. (Goal: Models scoring transactions in real time.)
    - Train Isolation Forest to score anomalies.
    - Trained XG-boost classifier and tune precision.
    - Build weighted score aggregate.
    - Add SHAP explainability layer.


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
