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


## Result


## Challenges & Solution
**Challenge**
- After creating a kafka, and docker containers I started live stream and noticed it takes 7-8 hrs to send 280,000 data, with that math, It's certain to achieve the full sending of 6m+ rows data will take 7-8 days.

**Solution**
- I went through the code and noticed the timer to send each rows lapses on 1sec per row. I fix that by rewriting/resetting it to millisecond per row. What was predict to take 7-8 days of streaming,  now takes less than 2 hrs without breakages or data loss.
