"""App header with Databricks logo and title (Databricks-inspired styling)."""
import streamlit as st

# Inline Databricks-style logo (red delta + wordmark) so we don't depend on static file paths
DATABRICKS_LOGO_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 140 28" fill="none" aria-label="Databricks" style="height:28px;vertical-align:middle">
  <path fill="#FF3621" d="M14 2L2 26h8l2.5-5 2.5 5h8L14 2z"/>
  <text x="32" y="18" font-family="system-ui,-apple-system,'Segoe UI',sans-serif" font-size="16" font-weight="600" fill="#1a1a1a">Databricks</text>
</svg>
"""


def render_app_header(title: str = "DENSO North America Operations", subtitle: str = "Manufacturing & supply chain KPIs"):
    """Render the main app header with Databricks logo and app title."""
    st.markdown(
        f"""
        <div style="
            display: flex;
            align-items: center;
            gap: 16px;
            padding: 12px 0 16px 0;
            border-bottom: 1px solid #e0e0e0;
            margin-bottom: 16px;
        ">
            <div style="flex-shrink:0">{DATABRICKS_LOGO_SVG}</div>
            <div>
                <div style="font-size:1.25rem;font-weight:600;color:#1a1a1a;">{title}</div>
                <div style="font-size:0.875rem;color:#5a5a5a;">{subtitle}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
