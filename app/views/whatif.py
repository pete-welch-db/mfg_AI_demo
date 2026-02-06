"""What-if: score delivery risk via model serving endpoint."""
import streamlit as st

from app.config import SERVING_ENDPOINT_NAME
from app.data.serving_client import invoke_delivery_risk


def render():
    st.title("What-if: delivery risk")
    st.caption("Score late-delivery probability for a scenario using the deployed model.")

    if not SERVING_ENDPOINT_NAME:
        st.info("Set SERVING_ENDPOINT_NAME in app config to use the real-time model endpoint.")
        st.markdown("Example inputs (must match training features): plant_enc, customer_enc, carrier_enc, day_of_week, month, qty, order_backlog, avg_lead_time, avail_pct, utilization_pct.")
        return

    with st.form("whatif_form"):
        st.subheader("Scenario")
        day_of_week = st.slider("Day of week (0=Mon)", 0, 6, 2)
        month = st.number_input("Month", 1, 12, 6)
        qty = st.number_input("Order quantity", 1, 10000, 500)
        order_backlog = st.number_input("Order backlog at plant", 0, 500, 50)
        avg_lead_time = st.number_input("Avg lead time (days)", 1, 30, 7)
        avail_pct = st.slider("Material availability %", 0.0, 1.0, 0.95)
        utilization_pct = st.slider("Capacity utilization %", 0.0, 120.0, 85.0)
        plant_enc = st.number_input("Plant (encoded)", 0, 10, 0)
        customer_enc = st.number_input("Customer (encoded)", 0, 10, 0)
        carrier_enc = st.number_input("Carrier (encoded)", 0, 10, 0)
        submitted = st.form_submit_button("Score")

    if submitted:
        inputs = {
            "plant_enc": plant_enc, "customer_enc": customer_enc, "carrier_enc": carrier_enc,
            "day_of_week": day_of_week, "month": month, "qty": qty, "order_backlog": order_backlog,
            "order_qty_sum": order_backlog * 10, "avg_lead_time": avg_lead_time,
            "avail_pct": avail_pct, "utilization_pct": utilization_pct,
        }
        prob = invoke_delivery_risk(inputs)
        if prob is not None:
            st.metric("P(late delivery)", f"{prob:.1%}", delta="High risk" if prob >= 0.5 else "Lower risk")
        else:
            st.error("Endpoint call failed. Check SERVING_ENDPOINT_NAME and token.")
