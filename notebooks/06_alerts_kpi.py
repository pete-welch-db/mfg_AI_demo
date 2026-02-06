# Databricks notebook source
# MAGIC %md
# MAGIC # KPI alerts: margin &lt; 8%, on-time &lt; 95%, OEE &lt; 75%
# MAGIC Run as a workflow job; outputs breaches with links to the app and Genie for follow-up.

# COMMAND ----------

# MAGIC %run ./00_common_setup

# COMMAND ----------

import os

# Thresholds and links (set via job params or env)
APP_URL = os.environ.get("DEMO_APP_URL", "https://your-workspace.databricks.com/apps/...")
GENIE_URL = os.environ.get("DEMO_GENIE_URL", "https://your-workspace.databricks.com/genie")
MARGIN_THRESHOLD = 8.0
ON_TIME_THRESHOLD = 95.0
OEE_THRESHOLD = 75.0

# COMMAND ----------

alerts = []

# Margin < 8% (from UC metric view)
margin_df = spark.sql(f"""
  SELECT plant_id, product_id, customer_id, AVG(MEASURE(`Margin %`)) AS margin_pct
  FROM {catalog}.{schema}.mv_plant_margin
  WHERE date >= current_date() - INTERVAL 30 DAYS
  GROUP BY plant_id, product_id, customer_id
  HAVING AVG(MEASURE(`Margin %`)) < {MARGIN_THRESHOLD}
""")
if margin_df.count() > 0:
    alerts.append({"kpi": "margin", "threshold": MARGIN_THRESHOLD, "breaches": margin_df.count(), "link": f"{APP_URL} (Margin by program)"})

# On-time < 95% (from UC metric view)
ot_df = spark.sql(f"""
  SELECT plant_id, customer_id, AVG(MEASURE(`On-time %`)) AS on_time_pct
  FROM {catalog}.{schema}.mv_on_time_delivery
  WHERE date >= current_date() - INTERVAL 30 DAYS
  GROUP BY plant_id, customer_id
  HAVING AVG(MEASURE(`On-time %`)) < {ON_TIME_THRESHOLD}
""")
if ot_df.count() > 0:
    alerts.append({"kpi": "on_time_delivery", "threshold": ON_TIME_THRESHOLD, "breaches": ot_df.count(), "link": f"{APP_URL} (On-time delivery)"})

# OEE < 75% (from UC metric view)
oee_df = spark.sql(f"""
  SELECT plant_id, work_center_id, AVG(MEASURE(`OEE %`)) AS oee_pct
  FROM {catalog}.{schema}.mv_plant_oee
  WHERE date >= current_date() - INTERVAL 30 DAYS
  GROUP BY plant_id, work_center_id
  HAVING AVG(MEASURE(`OEE %`)) < {OEE_THRESHOLD}
""")
if oee_df.count() > 0:
    alerts.append({"kpi": "oee", "threshold": OEE_THRESHOLD, "breaches": oee_df.count(), "link": f"{APP_URL} (Plant performance) | {GENIE_URL}"})

# COMMAND ----------

# Write alerts to a table for downstream (e.g. email job, Slack)
from pyspark.sql import Row
if alerts:
    rows = [Row(kpi=a["kpi"], threshold=a["threshold"], breach_count=a["breaches"], action_link=a["link"]) for a in alerts]
    spark.createDataFrame(rows).write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.alerts_kpi_breaches")
    print("ALERTS:", alerts)
else:
    print("No KPI breaches.")
