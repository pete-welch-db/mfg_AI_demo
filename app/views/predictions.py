"""Predictions: late-delivery risk from ml_delivery_risk_predictions."""
import streamlit as st
from app.data.query import get_delivery_risk_predictions

def render():
    st.title("Delivery risk predictions")
    st.caption("What should I act on first?")
    limit = st.slider("Max rows", 10, 200, 100, key="pred_limit")
    df = get_delivery_risk_predictions(limit=limit)
    if df is None or df.empty:
        st.warning("No predictions. Run ML training notebook to populate ml_delivery_risk_predictions.")
        return
    st.dataframe(df, use_container_width=True)
    if "probability" in df.columns:
        high_risk = (df["probability"] >= 0.5).sum()
        st.metric("High-risk shipments (p≥0.5)", int(high_risk))
