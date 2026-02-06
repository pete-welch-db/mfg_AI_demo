# Databricks notebook source
# MAGIC %md
# MAGIC # UC metric views (AI/BI semantic layer)
# MAGIC Creates Unity Catalog metric views over gold/ml tables for use in AI/BI dashboards (MEASURE() in SQL).
# MAGIC Run after DLT or 04_gold_metrics so gold tables exist.

# COMMAND ----------

# MAGIC %run ./00_common_setup

# COMMAND ----------

from pyspark.sql import functions as F

FQ = lambda name: f"`{catalog}`.`{schema}`.`{name}`"


def _exists(name):
    try:
        return spark.catalog.tableExists(f"{catalog}.{schema}.{name}")
    except Exception:
        return False


def _drop_view_if_exists(fq):
    try:
        spark.sql(f"DROP VIEW IF EXISTS {fq}")
    except Exception:
        pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Column comments on DLT-created tables (for Genie / AI)
# MAGIC DLT sets table-level comments; we add column comments here so source tables have full metadata.

# COMMAND ----------

_dlt_table_column_comments = {
    "silver_machine_events": {"event_id": "Unique event identifier", "plant_id": "Plant", "work_center_id": "Work center", "product_id": "Product", "date": "Event date", "event_ts": "Event timestamp", "state": "running or down", "duration_min": "Duration minutes", "throughput_qty": "Units produced"},
    "silver_production_orders": {"order_id": "Order identifier", "plant_id": "Plant", "product_id": "Product", "customer_id": "Customer", "date": "Order date", "order_qty": "Order quantity", "planned_qty": "Planned quantity", "status": "open, released, or closed"},
    "silver_shipments": {"shipment_id": "Shipment identifier", "plant_id": "Plant", "customer_id": "Customer", "carrier_id": "Carrier", "supplier_id": "Supplier", "product_id": "Product", "date": "Ship date", "promised_date": "Promised delivery", "delivered_date": "Actual delivery", "qty": "Quantity", "transit_days": "Days in transit", "on_time": "1 if on time, 0 if late"},
    "silver_material_availability": {"record_id": "Record identifier", "plant_id": "Plant", "supplier_id": "Supplier", "product_id": "Product", "date": "Date", "asn_qty": "ASN quantity", "received_qty": "Received quantity", "lead_time_days": "Lead time days", "available": "Availability flag"},
    "silver_planner_forecasts": {"forecast_id": "Forecast identifier", "plant_id": "Plant", "product_id": "Product", "customer_id": "Customer", "date": "Forecast date", "demand_qty": "Demand quantity", "capacity_hrs": "Capacity hours", "planned_hrs": "Planned hours"},
    "silver_plant_pnl": {"record_id": "Record identifier", "plant_id": "Plant", "product_id": "Product", "customer_id": "Customer", "date": "P&L date", "revenue": "Revenue", "cost": "Cost", "margin_pct": "Margin %"},
    "gold_plant_oee": {"plant_id": "Plant", "work_center_id": "Work center", "product_id": "Product", "date": "Date", "total_min": "Total minutes", "downtime_min": "Downtime minutes", "throughput_qty": "Throughput", "availability_pct": "Availability %", "oee_pct": "OEE %", "week": "ISO week"},
    "gold_on_time_delivery": {"plant_id": "Plant", "customer_id": "Customer", "product_family": "Product family", "date": "Date", "total_shipments": "Total shipments", "on_time_count": "On-time count", "on_time_pct": "On-time %"},
    "gold_plant_margin": {"plant_id": "Plant", "product_id": "Product", "customer_id": "Customer", "date": "Date", "revenue": "Revenue", "cost": "Cost", "margin_pct": "Margin %"},
    "gold_inventory_lead_time": {"plant_id": "Plant", "supplier_id": "Supplier", "date": "Date", "asn_qty": "ASN qty", "received_qty": "Received qty", "avg_lead_time_days": "Avg lead time days", "availability_pct": "Availability %"},
    "gold_capacity_utilization": {"plant_id": "Plant", "product_id": "Product", "date": "Date", "demand_qty": "Demand qty", "capacity_hrs": "Capacity hrs", "planned_hrs": "Planned hrs", "utilization_pct": "Utilization %"},
}
for tbl, col_comments in _dlt_table_column_comments.items():
    if _exists(tbl):
        apply_table_metadata(catalog, schema, tbl, None, col_comments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OEE metric view (gold_plant_oee)

# COMMAND ----------

if not _exists("gold_plant_oee"):
    print("SKIP mv_plant_oee: gold_plant_oee not found. Run DLT or 04_gold_metrics.")
else:
    _drop_view_if_exists(FQ("mv_plant_oee"))
    spark.sql(f"""
CREATE OR REPLACE VIEW {FQ("mv_plant_oee")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "OEE and downtime by plant, work center, product (for program filter)."
source: {catalog}.{schema}.gold_plant_oee
dimensions:
  - name: plant_id
    expr: plant_id
  - name: work_center_id
    expr: work_center_id
  - name: product_id
    expr: product_id
  - name: date
    expr: date
  - name: week
    expr: week
measures:
  - name: "OEE %"
    expr: AVG(oee_pct)
  - name: "Availability %"
    expr: AVG(availability_pct)
  - name: "Downtime min"
    expr: SUM(downtime_min)
  - name: "Throughput qty"
    expr: SUM(throughput_qty)
$$
""")
    print("Created mv_plant_oee")

# COMMAND ----------

# MAGIC %md
# MAGIC ## On-time delivery metric view (gold_on_time_delivery)

# COMMAND ----------

if not _exists("gold_on_time_delivery"):
    print("SKIP mv_on_time_delivery: gold_on_time_delivery not found.")
else:
    _drop_view_if_exists(FQ("mv_on_time_delivery"))
    spark.sql(f"""
CREATE OR REPLACE VIEW {FQ("mv_on_time_delivery")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "On-time delivery % by plant, customer, product family."
source: {catalog}.{schema}.gold_on_time_delivery
dimensions:
  - name: plant_id
    expr: plant_id
  - name: customer_id
    expr: customer_id
  - name: product_family
    expr: product_family
  - name: date
    expr: date
measures:
  - name: "On-time %"
    expr: AVG(on_time_pct)
  - name: "Total shipments"
    expr: SUM(total_shipments)
  - name: "On-time count"
    expr: SUM(on_time_count)
$$
""")
    print("Created mv_on_time_delivery")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plant margin metric view (gold_plant_margin)

# COMMAND ----------

if not _exists("gold_plant_margin"):
    print("SKIP mv_plant_margin: gold_plant_margin not found.")
else:
    _drop_view_if_exists(FQ("mv_plant_margin"))
    spark.sql(f"""
CREATE OR REPLACE VIEW {FQ("mv_plant_margin")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Operating margin by plant, product, customer."
source: {catalog}.{schema}.gold_plant_margin
dimensions:
  - name: plant_id
    expr: plant_id
  - name: product_id
    expr: product_id
  - name: customer_id
    expr: customer_id
  - name: date
    expr: date
measures:
  - name: "Margin %"
    expr: AVG(margin_pct)
  - name: Revenue
    expr: SUM(revenue)
  - name: Cost
    expr: SUM(cost)
$$
""")
    print("Created mv_plant_margin")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delivery risk metric view (ml_delivery_risk_predictions)

# COMMAND ----------

if not _exists("ml_delivery_risk_predictions"):
    print("SKIP mv_delivery_risk: ml_delivery_risk_predictions not found. Run 03_ml_training.")
else:
    _drop_view_if_exists(FQ("mv_delivery_risk"))
    spark.sql(f"""
CREATE OR REPLACE VIEW {FQ("mv_delivery_risk")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Late delivery risk from ML model (probability, high-risk count)."
source: {catalog}.{schema}.ml_delivery_risk_predictions
dimensions:
  - name: plant_id
    expr: plant_id
  - name: customer_id
    expr: customer_id
  - name: date
    expr: date
measures:
  - name: "Avg late probability"
    expr: AVG(probability)
  - name: "High-risk count"
    expr: SUM(CASE WHEN probability >= 0.5 THEN 1 ELSE 0 END)
  - name: "Shipments scored"
    expr: COUNT(1)
$$
""")
    print("Created mv_delivery_risk")

# COMMAND ----------

display(spark.sql(f"SHOW VIEWS IN `{catalog}`.`{schema}`"))
