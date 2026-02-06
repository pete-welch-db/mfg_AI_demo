"""Run SQL against Unity Catalog; coerce numerics; on failure return mock when USE_MOCK_DATA."""
import pandas as pd

from app.config import (
    DATABRICKS_HOST,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    USE_MOCK_DATA,
)


def _coerce_numerics(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    for col in df.select_dtypes(include=["object"]).columns:
        try:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().any():
                df[col] = converted
        except Exception:
            pass
    return df


def get_connection():
    """Cached connection via databricks-sql-connector. Uses default auth (e.g. OAuth for Databricks Apps)."""
    if not DATABRICKS_HOST or not DATABRICKS_HTTP_PATH:
        return None
    try:
        from databricks import sql
        return sql.connect(
            server_hostname=DATABRICKS_HOST.replace("https://", "").split("/")[0],
            http_path=DATABRICKS_HTTP_PATH,
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_SCHEMA,
        )
    except Exception:
        return None


def run_query(sql: str, conn=None):
    """Execute SQL and return a DataFrame; coerce numerics. On failure, return None (caller may use mock)."""
    if conn is None:
        conn = get_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            try:
                df = cursor.fetchall_arrow().to_pandas()
            except Exception:
                df = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
        return _coerce_numerics(df)
    except Exception:
        return None


def query_or_mock(sql: str, mock_df: pd.DataFrame):
    """Run SQL; if it fails and USE_MOCK_DATA is true, return mock_df; else return result or empty DataFrame."""
    df = run_query(sql)
    if df is not None:
        return df
    if USE_MOCK_DATA and mock_df is not None:
        return _coerce_numerics(mock_df)
    return pd.DataFrame()


def _full_table(name: str) -> str:
    return f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{name}"


# --- KPI query helpers (used by views) ---

def get_executive_summary():
    """Key KPIs from UC metric views (MEASURE)."""
    sql = f"""
    SELECT
      (SELECT AVG(MEASURE(`Margin %`)) FROM {_full_table('mv_plant_margin')} WHERE date >= current_date() - INTERVAL 90 DAYS) AS avg_margin_pct,
      (SELECT AVG(MEASURE(`On-time %`)) FROM {_full_table('mv_on_time_delivery')} WHERE date >= current_date() - INTERVAL 30 DAYS) AS on_time_pct,
      (SELECT AVG(MEASURE(`OEE %`)) FROM {_full_table('mv_plant_oee')} WHERE date >= current_date() - INTERVAL 7 DAYS) AS avg_oee_pct
    """
    mock = pd.DataFrame({"avg_margin_pct": [8.5], "on_time_pct": [94.0], "avg_oee_pct": [85.0]})
    return query_or_mock(sql, mock)


def get_top_plants_oee(limit=3):
    """Top N plants by average OEE (recent 30d) from UC metric view. Join to dim_plant for name."""
    sql = f"""
    SELECT p.plant_id, p.plant_name, AVG(MEASURE(`OEE %`)) AS avg_oee_pct
    FROM {_full_table('mv_plant_oee')} g
    JOIN {_full_table('silver_dim_plant')} p ON g.plant_id = p.plant_id
    WHERE g.date >= current_date() - INTERVAL 30 DAYS
    GROUP BY p.plant_id, p.plant_name
    ORDER BY avg_oee_pct DESC
    LIMIT {int(limit)}
    """
    mock = pd.DataFrame({
        "plant_id": ["P001", "P002", "P003"],
        "plant_name": ["Plant Michigan", "Plant Ohio", "Plant Tennessee"],
        "avg_oee_pct": [88.0, 85.0, 82.0],
    })
    return query_or_mock(sql, mock)


def get_lead_time_alert(threshold_days=14, min_availability=0.9):
    """Critical component lead-time alert: suppliers/plants with high lead time or low availability (recent 30d)."""
    sql = f"""
    SELECT g.plant_id, g.supplier_id, AVG(g.avg_lead_time_days) AS avg_lead_days, AVG(g.availability_pct) AS avail_pct
    FROM {_full_table('gold_inventory_lead_time')} g
    WHERE g.date >= current_date() - INTERVAL 30 DAYS
    GROUP BY g.plant_id, g.supplier_id
    HAVING AVG(g.avg_lead_time_days) > {int(threshold_days)} OR AVG(g.availability_pct) < {min_availability}
    ORDER BY avg_lead_days DESC
    LIMIT 10
    """
    mock = pd.DataFrame({
        "plant_id": ["P001"], "supplier_id": ["SUP02"],
        "avg_lead_days": [18.0], "avail_pct": [0.85],
    })
    return query_or_mock(sql, mock)


def get_plant_oee(date_start=None, date_end=None, program_type=None):
    """OEE by plant/work_center/product from UC metric view. If program_type set, join to dim_product and filter."""
    prefix = "g." if program_type else ""
    where_parts = []
    if date_start:
        where_parts.append(f"{prefix}date >= '{date_start}'")
    if date_end:
        where_parts.append(f"{prefix}date <= '{date_end}'")
    if program_type:
        where_parts.append(f"p.program_type = '{program_type}'")
    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
    if program_type:
        sql = f"""
        SELECT g.plant_id, g.work_center_id, g.product_id, p.program_type, g.date,
               MEASURE(`OEE %`) AS oee_pct, MEASURE(`Availability %`) AS availability_pct,
               MEASURE(`Downtime min`) AS downtime_min, MEASURE(`Throughput qty`) AS throughput_qty
        FROM {_full_table('mv_plant_oee')} g
        JOIN {_full_table('silver_dim_product')} p ON g.product_id = p.product_id
        WHERE {where_clause}
        """
    else:
        sql = f"""
        SELECT g.plant_id, g.work_center_id, g.product_id, g.date,
               MEASURE(`OEE %`) AS oee_pct, MEASURE(`Availability %`) AS availability_pct,
               MEASURE(`Downtime min`) AS downtime_min, MEASURE(`Throughput qty`) AS throughput_qty
        FROM {_full_table('mv_plant_oee')} g
        WHERE {where_clause}
        """
    mock = pd.DataFrame({
        "plant_id": ["P001", "P002"], "work_center_id": ["WC001", "WC003"], "product_id": ["PRD001", "PRD003"],
        "date": pd.to_datetime(["2025-01-01", "2025-01-01"]), "oee_pct": [85.0, 78.0],
        "availability_pct": [88.0, 80.0], "downtime_min": [45, 120], "throughput_qty": [1200, 900],
    })
    return query_or_mock(sql, mock)


def get_on_time_delivery(date_start=None, date_end=None):
    """On-time delivery from UC metric view (MEASURE)."""
    where = "1=1"
    if date_start:
        where += f" AND date >= '{date_start}'"
    if date_end:
        where += f" AND date <= '{date_end}'"
    sql = f"""
    SELECT plant_id, customer_id, product_family, date,
           MEASURE(`Total shipments`) AS total_shipments,
           MEASURE(`On-time count`) AS on_time_count,
           MEASURE(`On-time %`) AS on_time_pct
    FROM {_full_table('mv_on_time_delivery')}
    WHERE {where}
    """
    mock = pd.DataFrame({
        "plant_id": ["P001"], "customer_id": ["C001"], "product_family": ["Electrification"],
        "date": pd.to_datetime(["2025-01-01"]), "total_shipments": [50], "on_time_count": [47], "on_time_pct": [94.0],
    })
    return query_or_mock(sql, mock)


def get_plant_margin(date_start=None, date_end=None):
    """Plant margin from UC metric view (MEASURE)."""
    sql = f"""
    SELECT plant_id, product_id, customer_id, date,
           MEASURE(Revenue) AS revenue, MEASURE(Cost) AS cost, MEASURE(`Margin %`) AS margin_pct
    FROM {_full_table('mv_plant_margin')}
    WHERE 1=1
    """ + (f" AND date >= '{date_start}'" if date_start else "") + (f" AND date <= '{date_end}'" if date_end else "")
    mock = pd.DataFrame({
        "plant_id": ["P001"], "product_id": ["PRD001"], "customer_id": ["C001"],
        "date": pd.to_datetime(["2025-01-01"]), "revenue": [1e6], "cost": [920000], "margin_pct": [8.0],
    })
    return query_or_mock(sql, mock)


def get_inventory_lead_time(date_start=None, date_end=None):
    sql = f"""
    SELECT plant_id, supplier_id, date, asn_qty, received_qty, avg_lead_time_days, availability_pct
    FROM {_full_table('gold_inventory_lead_time')}
    WHERE 1=1
    """ + (f" AND date >= '{date_start}'" if date_start else "") + (f" AND date <= '{date_end}'" if date_end else "")
    mock = pd.DataFrame({
        "plant_id": ["P001"], "supplier_id": ["SUP01"], "date": pd.to_datetime(["2025-01-01"]),
        "asn_qty": [5000], "received_qty": [4800], "avg_lead_time_days": [7], "availability_pct": [0.95],
    })
    return query_or_mock(sql, mock)


def get_delivery_risk_predictions(limit=100):
    sql = f"""
    SELECT shipment_id, plant_id, customer_id, date, prediction, probability, model_version, scored_at
    FROM {_full_table('ml_delivery_risk_predictions')}
    ORDER BY probability DESC
    LIMIT {int(limit)}
    """
    mock = pd.DataFrame({
        "shipment_id": ["SHIP001"], "plant_id": ["P001"], "customer_id": ["C001"],
        "date": pd.to_datetime(["2025-01-15"]), "prediction": [1], "probability": [0.72],
        "model_version": ["1"], "scored_at": [pd.Timestamp.utcnow()],
    })
    return query_or_mock(sql, mock)
