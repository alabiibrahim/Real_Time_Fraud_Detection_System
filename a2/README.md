
```python 
# dashboard/app.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os
import sys
import time
from datetime import datetime

sys.path.append('C:/Users/HP/fraud-detection-system/dashboard')
from alert_store import load_alerts, update_decision

#1: page config 
st.set_page_config(
    page_title = "Fraud Detection Dashboard",
    # page_icon  = "🔍",
    layout     = "wide"
)

st.title("Real-Time Fraud Detection Dashboard")
st.caption(f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

#2: auto-refresh every 5 seconds 
refresh_rate = st.sidebar.slider("Auto-refresh (seconds)", 3, 30, 5)
placeholder  = st.empty()

def render_dashboard():
    alerts = load_alerts()

    if not alerts:
        st.info("No alerts yet. Confirm consumer is running and streaming data.")
        return

    df = pd.DataFrame(alerts)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['amount']    = df['amount'].astype(float)

    #3: metric cards 
    total       = len(df)
    fraud_count = len(df[df['status'] == 'FRAUD'])
    review_count= len(df[df['status'] == 'REVIEW'])
    total_amt   = df[df['status'] == 'FRAUD']['amount'].sum()
    pending     = len(df[df['decision'] == 'pending'])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Alerts",       f"{total:,}")
    col2.metric("Fraud Flagged",       f"{fraud_count:,}")
    col3.metric("Under Review",        f"{review_count:,}")
    col4.metric("Amount at Risk",      f"${total_amt:,.0f}")
    col5.metric("Pending Decisions",   f"{pending:,}")

    st.divider()

    #3: charts row
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Alerts over time")
        df_time = df.set_index('timestamp').resample('1min')['status'].count().reset_index()
        df_time.columns = ['timestamp', 'count']
        fig = px.line(
            df_time, x='timestamp', y='count',
            labels={'count': 'Alerts per minute', 'timestamp': ''},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=280)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Score distribution")
        fig2 = px.histogram(
            df, x='final_score', color='status',
            nbins=40,
            color_discrete_map={'FRAUD': '#E24B4A', 'REVIEW': '#EF9F27'},
            labels={'final_score': 'Final fraud score', 'count': 'Transactions'}
        )
        fig2.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=280)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    #4:  fraud by transaction type and score correlation.
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Fraud by transaction type")
        type_counts = df[df['status'] == 'FRAUD']['type'].value_counts().reset_index()
        type_counts.columns = ['type', 'count']
        fig3 = px.bar(
            type_counts, x='type', y='count',
            color='count',
            color_continuous_scale='Reds',
            labels={'count': 'Fraud cases', 'type': 'Transaction type'}
        )
        fig3.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=280, showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    with col_b:
        st.subheader("XGBoost vs graph score")
        fig4 = px.scatter(
            df[df['status'].isin(['FRAUD', 'REVIEW'])],
            x='xgb_score', y='graph_score',
            color='status',
            size='amount',
            size_max=20,
            color_discrete_map={'FRAUD': '#E24B4A', 'REVIEW': '#EF9F27'},
            labels={'xgb_score': 'XGBoost score', 'graph_score': 'Graph ring score'}
        )
        fig4.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=280)
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()

    #5: alert queue — analyst review 
    st.subheader("Alert queue - pending analyst review")

    pending_df = df[df['decision'] == 'pending'].sort_values(
        'final_score', ascending=False
    ).head(50).reset_index()

    if pending_df.empty:
        st.success("No pending alerts - all reviewed.")
    else:
        for i, row in pending_df.iterrows():
            with st.expander(
                f"[{row['status']}] {row['user_id']} — "
                f"${row['amount']:,.2f} — score: {row['final_score']:.3f}"
            ):
                col1, col2, col3 = st.columns(3)
                col1.metric("XGBoost score", f"{row['xgb_score']:.3f}")
                col2.metric("Graph score",   f"{row['graph_score']:.3f}")
                col3.metric("Final score",   f"{row['final_score']:.3f}")

                st.write(f"**Type:** {row['type']} | "
                         f"**Recipient:** {row['recipient_id']} | "
                         f"**Time:** {row['timestamp']}")

                btn_col1, btn_col2, btn_col3 = st.columns(3)
                original_index = row['index']

                if btn_col1.button("Confirm fraud",    key=f"fraud_{i}"):
                    update_decision(original_index, 'confirmed_fraud')
                    st.rerun()
                if btn_col2.button("Mark legitimate",  key=f"legit_{i}"):
                    update_decision(original_index, 'confirmed_legit')
                    st.rerun()
                if btn_col3.button("Escalate",         key=f"escalate_{i}"):
                    update_decision(original_index, 'escalated')
                    st.rerun()

    st.divider() 

    #6: recent alerts table 
    st.subheader("Recent alerts")
    recent = df.sort_values('timestamp', ascending=False).head(100)[[
        'timestamp', 'user_id', 'amount', 'type',
        'xgb_score', 'graph_score', 'final_score', 'status', 'decision'
    ]]
    st.dataframe(
        recent.style.applymap(
            lambda v: 'color: #E24B4A; font-weight: 500' if v == 'FRAUD'
                 else 'color: #EF9F27; font-weight: 500' if v == 'REVIEW'
                 else '',
            subset=['status']
        ),
        use_container_width=True,
        height=400
    )

#7: main render loop 
with placeholder.container():
    render_dashboard()

time.sleep(refresh_rate)
st.rerun()

``` 
