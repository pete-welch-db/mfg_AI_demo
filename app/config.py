# Only place in the app where environment variables are read.
import os

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "welch")
DATABRICKS_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "denso_ai")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
GENIE_PLANNING_SPACE_ID = os.environ.get("GENIE_PLANNING_SPACE_ID", "")
DASHBOARD_EMBED_URL = os.environ.get("DASHBOARD_EMBED_URL", "")
USE_MOCK_DATA = os.environ.get("USE_MOCK_DATA", "false").lower() in ("true", "1", "yes")
SERVING_ENDPOINT_NAME = os.environ.get("SERVING_ENDPOINT_NAME", "")
