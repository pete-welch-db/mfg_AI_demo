"""Margin by program: P&L/margin by plant, product family, customer program."""
import streamlit as st
from app.data.query import get_plant_margin

def render():
    st.title("Margin by program")
    st.caption("Where is margin at risk for electrification programs?")
    date_start = st.date_input("From", value=None, key="margin_start")
    date_end = st.date_input("To", value=None, key="margin_end")
    df = get_plant_margin(date_start=date_start.isoformat() if date_start else None, date_end=date_end.isoformat() if date_end else None)
    if df is None or df.empty:
        st.warning("No data.")
        return
    st.dataframe(df, use_container_width=True)
    if "margin_pct" in df.columns:
        st.metric("Avg margin %", f"{df['margin_pct'].mean():.1f}%")
