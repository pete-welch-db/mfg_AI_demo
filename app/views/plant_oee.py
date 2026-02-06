"""Plant performance (OEE): OEE and unplanned downtime by plant and line; filter by program (EV/ADAS/Other)."""
import streamlit as st
from app.data.query import get_plant_oee

def render():
    st.title("Plant performance (OEE)")
    st.caption("Which plants/lines are hurting OEE? Filter by program (EV, ADAS, Other).")
    date_start = st.date_input("From", value=None, key="oee_start")
    date_end = st.date_input("To", value=None, key="oee_end")
    program_type = st.selectbox(
        "Program",
        options=["All", "EV", "ADAS", "Other"],
        index=0,
        key="oee_program",
    )
    program_filter = None if program_type == "All" else program_type
    df = get_plant_oee(
        date_start=date_start.isoformat() if date_start else None,
        date_end=date_end.isoformat() if date_end else None,
        program_type=program_filter,
    )
    if df is None or df.empty:
        st.warning("No data.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)
    numeric_cols = [c for c in df.select_dtypes(include=["number"]).columns.tolist() if c in df.columns and c not in ("product_id",)]
    if numeric_cols and "plant_id" in df.columns:
        try:
            st.bar_chart(df.set_index("plant_id")[numeric_cols[:3]])
        except Exception:
            pass
