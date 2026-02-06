"""Genie Conversation API client for chat in the app. Uses databricks-sdk."""
from __future__ import annotations

from app.config import DATABRICKS_HOST, DATABRICKS_TOKEN, GENIE_SPACE_ID, GENIE_PLANNING_SPACE_ID


def get_workspace_client():
    """WorkspaceClient for Genie API. Uses token if set; otherwise default auth (e.g. OAuth in Databricks App)."""
    try:
        from databricks.sdk import WorkspaceClient
        if DATABRICKS_HOST and DATABRICKS_TOKEN:
            return WorkspaceClient(host=DATABRICKS_HOST.replace("https://", "https://").rstrip("/"), token=DATABRICKS_TOKEN)
        return WorkspaceClient()
    except Exception:
        return None


def _response_text(message) -> str:
    """Extract display text from Genie message or conversation."""
    if message is None:
        return ""
    if hasattr(message, "messages") and message.messages:
        return _response_text(message.messages[-1])
    if hasattr(message, "content") and message.content:
        return message.content if isinstance(message.content, str) else str(message.content)
    if hasattr(message, "text"):
        return message.text or ""
    if hasattr(message, "answer"):
        return message.answer or ""
    return str(message)


def chat(space_id: str, prompt: str, conversation_id: str | None = None) -> tuple[str, str]:
    """
    Send a message to a Genie space. Returns (response_text, conversation_id).
    If conversation_id is None, starts a new conversation; otherwise continues it.
    """
    w = get_workspace_client()
    if not w or not space_id or not prompt.strip():
        return ("No Genie space configured or no prompt.", conversation_id or "")

    try:
        if not conversation_id:
            conv = w.genie.start_conversation_and_wait(
                space_id=space_id,
                conversation_starter=prompt.strip(),
            )
            cid = getattr(conv, "conversation_id", None) or getattr(conv, "id", None) or ""
            msg = getattr(conv, "message", None) or getattr(conv, "response", None) or conv
            return (_response_text(msg), cid)
        msg = w.genie.create_message_and_wait(
            space_id=space_id,
            conversation_id=conversation_id,
            content=prompt.strip(),
        )
        return (_response_text(msg), conversation_id)
    except Exception as e:
        return (f"Genie error: {e}", conversation_id or "")


def chat_operations(prompt: str, conversation_id: str | None = None) -> tuple[str, str]:
    """Chat with Operations Genie space."""
    return chat(GENIE_SPACE_ID, prompt, conversation_id)


def chat_planning(prompt: str, conversation_id: str | None = None) -> tuple[str, str]:
    """Chat with Planning Genie space."""
    return chat(GENIE_PLANNING_SPACE_ID, prompt, conversation_id)
