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
