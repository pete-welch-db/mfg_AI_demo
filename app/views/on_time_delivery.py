"""On-time delivery % by customer program, plant, product family."""
import streamlit as st
from app.data.query import get_on_time_delivery

def render():
    st.title("On-time delivery")
    st.caption("Are we meeting OEM delivery commitments?")
    date_start = st.date_input("From", value=None, key="otd_start")
    date_end = st.date_input("To", value=None, key="otd_end")
    df = get_on_time_delivery(date_start=date_start.isoformat() if date_start else None, date_end=date_end.isoformat() if date_end else None)
    if df is None or df.empty:
        st.warning("No data.")
        return
    st.dataframe(df, use_container_width=True)
    if "on_time_pct" in df.columns:
        st.metric("Avg on-time %", f"{df['on_time_pct'].mean():.1f}%")
