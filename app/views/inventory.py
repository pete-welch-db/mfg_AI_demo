"""Inventory & lead time: inventory turns and lead-time reliability by plant/supplier."""
import streamlit as st
from app.data.query import get_inventory_lead_time

def render():
    st.title("Inventory & lead time")
    st.caption("Where are we exposed on material availability?")
    date_start = st.date_input("From", value=None, key="inv_start")
    date_end = st.date_input("To", value=None, key="inv_end")
    df = get_inventory_lead_time(date_start=date_start.isoformat() if date_start else None, date_end=date_end.isoformat() if date_end else None)
    if df is None or df.empty:
        st.warning("No data.")
        return
    st.dataframe(df, use_container_width=True)
    if "avg_lead_time_days" in df.columns:
        st.metric("Avg lead time (days)", f"{df['avg_lead_time_days'].mean():.1f}")
