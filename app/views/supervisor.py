"""Multi-agent supervisor: one chat input, answers from Operations and Planning Genie spaces combined."""
import streamlit as st

from app.config import GENIE_SPACE_ID, GENIE_PLANNING_SPACE_ID
from app.data.genie_client import chat_operations, chat_planning


def render():
    st.title("Supervisor: Operations + Planning")
    st.caption("One question, two perspectives. Operations (plants, delivery, OEE, margin) and Planning (forecasts, capacity, S&OP).")

    ops_ok = bool(GENIE_SPACE_ID)
    plan_ok = bool(GENIE_PLANNING_SPACE_ID)
    if not ops_ok and not plan_ok:
        st.warning("Set GENIE_SPACE_ID and/or GENIE_PLANNING_SPACE_ID in app config.")
        return

    if "supervisor_ops_conv" not in st.session_state:
        st.session_state.supervisor_ops_conv = None
    if "supervisor_plan_conv" not in st.session_state:
        st.session_state.supervisor_plan_conv = None
    if "supervisor_messages" not in st.session_state:
        st.session_state.supervisor_messages = []

    for m in st.session_state.supervisor_messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
            if m.get("ops"):
                with st.expander("Operations Genie"):
                    st.markdown(m["ops"])
            if m.get("plan"):
                with st.expander("Planning Genie"):
                    st.markdown(m["plan"])

    if prompt := st.chat_input("Ask anything (Operations + Planning will answer)..."):
        st.session_state.supervisor_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            ops_text, plan_text = "", ""
            with st.spinner("Supervisor querying both Genie spaces..."):
                if ops_ok:
                    ops_text, cid_ops = chat_operations(prompt, st.session_state.supervisor_ops_conv)
                    if cid_ops:
                        st.session_state.supervisor_ops_conv = cid_ops
                if plan_ok:
                    plan_text, cid_plan = chat_planning(prompt, st.session_state.supervisor_plan_conv)
                    if cid_plan:
                        st.session_state.supervisor_plan_conv = cid_plan

                combined = []
                if ops_text:
                    combined.append("**Operations:**\n\n" + ops_text)
                if plan_text:
                    combined.append("**Planning:**\n\n" + plan_text)
                display = "\n\n---\n\n".join(combined) if combined else "No response from Genie spaces."
                st.session_state.supervisor_messages.append({
                    "role": "assistant", "content": display, "ops": ops_text or None, "plan": plan_text or None
                })
                st.markdown(display)

    if st.button("Clear supervisor conversation", key="supervisor_clear"):
        st.session_state.supervisor_ops_conv = None
        st.session_state.supervisor_plan_conv = None
        st.session_state.supervisor_messages = []
        st.rerun()
