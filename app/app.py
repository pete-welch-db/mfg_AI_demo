# Routing only: select page and render the corresponding view.
import streamlit as st

from app.components.header import render_app_header
from app.styles import get_custom_css
from app.views import (
    executive,
    plant_oee,
    on_time_delivery,
    margin,
    inventory,
    predictions,
    genie_chat,
    supervisor,
    whatif,
)

PAGES = {
    "Executive summary": executive,
    "Plant performance (OEE)": plant_oee,
    "On-time delivery": on_time_delivery,
    "Margin by program": margin,
    "Inventory & lead time": inventory,
    "Predictions": predictions,
    "What-if (model serving)": whatif,
    "Ask Genie": genie_chat,
    "Supervisor (Operations + Planning)": supervisor,
}


def main():
    st.set_page_config(
        page_title="DENSO Operations | Databricks",
        page_icon="https://www.databricks.com/favicon.ico",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(get_custom_css(), unsafe_allow_html=True)

    st.sidebar.title("DENSO Operations")
    st.sidebar.caption("Manufacturing & supply chain KPIs")
    choice = st.sidebar.radio("Page", list(PAGES.keys()), key="page_choice")

    render_app_header(title="DENSO North America Operations", subtitle="Manufacturing & supply chain KPIs")
    PAGES[choice].render()


if __name__ == "__main__":
    main()
