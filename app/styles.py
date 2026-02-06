"""Databricks-inspired styling for the DENSO Operations app.
   Colors and typography aligned with https://www.databricks.com/ ."""
# Brand colors (Databricks)
RED = "#FF3621"           # Primary accent
DARK = "#1a1a1a"          # Headers, primary text
DARK_SOFT = "#2d2d2d"     # Secondary text / sidebar
LIGHT_BG = "#f5f5f5"      # Page background
WHITE = "#ffffff"
LINK = "#2486EB"          # Links
BORDER = "#e0e0e0"

def get_custom_css() -> str:
    return f"""
    <style>
    /* Base: Databricks-style palette */
    :root {{
        --db-red: {RED};
        --db-dark: {DARK};
        --db-dark-soft: {DARK_SOFT};
        --db-light-bg: {LIGHT_BG};
        --db-white: {WHITE};
        --db-link: {LINK};
        --db-border: {BORDER};
    }}
    /* Main block background */
    .stApp {{
        background-color: {LIGHT_BG};
    }}
    /* Headers */
    h1, h2, h3 {{
        color: {DARK} !important;
        font-weight: 600;
    }}
    /* Sidebar */
    [data-testid="stSidebar"] {{
        background-color: {WHITE};
        border-right: 1px solid {BORDER};
    }}
    [data-testid="stSidebar"] .stMarkdown {{
        color: {DARK_SOFT};
    }}
    /* Primary button: Databricks red */
    .stButton > button[kind="primary"] {{
        background-color: {RED};
        color: {WHITE};
        border: none;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: #e62e1a;
        color: {WHITE};
    }}
    /* Metric cards */
    [data-testid="stMetric"] {{
        background-color: {WHITE};
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid {BORDER};
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }}
    /* Links */
    a {{
        color: {LINK} !important;
    }}
    a:hover {{
        color: {RED} !important;
    }}
    /* DataFrames */
    [data-testid="stDataFrame"] {{
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid {BORDER};
    }}
    </style>
    """
