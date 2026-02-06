"""Executive summary: margin vs target, on-time %, top 3 plants by OEE, critical component lead-time alert."""
import streamlit as st
from app.data.query import get_executive_summary, get_top_plants_oee, get_lead_time_alert
from app.config import GENIE_SPACE_ID

def render():
    st.title("Executive Summary")
    st.caption("Single view: where we stand on margin, delivery, OEE, and material risk.")
    df = get_executive_summary()
    if df is None or df.empty:
        st.warning("No data. Run the data pipeline or enable USE_MOCK_DATA.")
        return
    row = df.iloc[0]
    col1, col2, col3 = st.columns(3)
    with col1:
        margin = float(row.get("avg_margin_pct", 0) or 0)
        st.metric("Avg margin (90d) %", f"{margin:.1f}%", delta="Target 9–10%" if margin < 9 else None)
    with col2:
        ot = float(row.get("on_time_pct", 0) or 0)
        st.metric("On-time delivery (30d) %", f"{ot:.1f}%", delta=None)
    with col3:
        oee = float(row.get("avg_oee_pct", 0) or 0)
        st.metric("Avg OEE (7d) %", f"{oee:.1f}%", delta=None)

    st.subheader("Top 3 plants by OEE (30d)")
    top_oee = get_top_plants_oee(limit=3)
    if top_oee is not None and not top_oee.empty:
        st.dataframe(top_oee, use_container_width=True, hide_index=True)
    else:
        st.caption("No OEE data.")

    st.subheader("Critical component lead-time alert")
    lead_alert = get_lead_time_alert(threshold_days=14, min_availability=0.9)
    if lead_alert is not None and not lead_alert.empty:
        st.warning("Plants/suppliers with lead time > 14 days or availability < 90%.")
        st.dataframe(lead_alert, use_container_width=True, hide_index=True)
    else:
        st.success("No critical lead-time alerts.")

    st.info("Drill into Plant performance, On-time delivery, and Margin by program below.")
    if GENIE_SPACE_ID:
        st.markdown("[Open Genie Space](https://docs.databricks.com/genie) to ask questions in natural language.")
