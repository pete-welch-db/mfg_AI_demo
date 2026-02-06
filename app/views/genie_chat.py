"""Embed Genie conversation in the app: single Operations space chat."""
import streamlit as st

from app.config import GENIE_SPACE_ID
from app.data.genie_client import chat_operations


def render():
    st.title("Ask Genie")
    st.caption("Chat with the Operations Genie space. Ask about margin, on-time delivery, OEE, inventory.")

    if not GENIE_SPACE_ID:
        st.warning("GENIE_SPACE_ID is not set. Deploy the Genie space and set it in app config.")
        return

    if "genie_conversation_id" not in st.session_state:
        st.session_state.genie_conversation_id = None
    if "genie_messages" not in st.session_state:
        st.session_state.genie_messages = []

    for m in st.session_state.genie_messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    if prompt := st.chat_input("Ask about KPIs, plants, or programs..."):
        st.session_state.genie_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Genie is thinking..."):
                response, cid = chat_operations(prompt, st.session_state.genie_conversation_id)
                if cid:
                    st.session_state.genie_conversation_id = cid
                st.session_state.genie_messages.append({"role": "assistant", "content": response})
                st.markdown(response)

    if st.button("Clear conversation", key="genie_clear"):
        st.session_state.genie_conversation_id = None
        st.session_state.genie_messages = []
        st.rerun()
