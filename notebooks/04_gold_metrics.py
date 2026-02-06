# Databricks notebook source
# MAGIC %md
# MAGIC # Gold metrics
# MAGIC Build gold KPI tables from silver (same logic as DLT pipeline). Use when running notebook-only without DLT.

# COMMAND ----------

# MAGIC %run ./00_common_setup

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# gold_plant_oee (by plant, work_center, product_id for program filter)
events = spark.table(f"{catalog}.{schema}.silver_machine_events")
total = events.groupBy("plant_id", "work_center_id", "product_id", "date").agg(
    F.sum("duration_min").alias("total_min"),
    F.sum(F.when(F.col("state") == "down", F.col("duration_min")).otherwise(0)).alias("downtime_min"),
    F.sum("throughput_qty").alias("throughput_qty"),
)
total = total.withColumn("availability_pct", F.least(100.0, F.greatest(0.0, 100.0 - 100.0 * F.col("downtime_min") / F.greatest(F.col("total_min"), 1))))
total = total.withColumn("oee_pct", F.col("availability_pct")).withColumn("week", F.weekofyear(F.col("date")))
total.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_plant_oee")

# COMMAND ----------

# gold_on_time_delivery (by plant, customer, product_family, date)
dim_product = spark.table(f"{catalog}.{schema}.silver_dim_product")
ship = spark.table(f"{catalog}.{schema}.silver_shipments")
ship_with_family = ship.join(dim_product, ship.product_id == dim_product.product_id, "left").select(
    ship.plant_id, ship.customer_id, ship.date,
    F.coalesce(dim_product.product_family, F.lit("Unknown")).alias("product_family"),
    ship.on_time,
)
agg = ship_with_family.groupBy("plant_id", "customer_id", "product_family", "date").agg(
    F.count("*").alias("total_shipments"),
    F.sum("on_time").alias("on_time_count"),
)
agg = agg.withColumn("on_time_pct", F.when(F.col("total_shipments") > 0, 100.0 * F.col("on_time_count") / F.col("total_shipments")).otherwise(0.0))
agg.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_on_time_delivery")

# COMMAND ----------

# gold_plant_margin
pnl = spark.table(f"{catalog}.{schema}.silver_plant_pnl")
margin = pnl.groupBy("plant_id", "product_id", "customer_id", "date").agg(
    F.sum("revenue").alias("revenue"),
    F.sum("cost").alias("cost"),
    F.avg("margin_pct").alias("margin_pct"),
)
margin.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_plant_margin")

# COMMAND ----------

# gold_inventory_lead_time
mat = spark.table(f"{catalog}.{schema}.silver_material_availability")
inv = mat.groupBy("plant_id", "supplier_id", "date").agg(
    F.sum("asn_qty").alias("asn_qty"),
    F.sum("received_qty").alias("received_qty"),
    F.avg("lead_time_days").alias("avg_lead_time_days"),
    F.avg("available").alias("availability_pct"),
)
inv.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_inventory_lead_time")

# COMMAND ----------

# gold_capacity_utilization
fc = spark.table(f"{catalog}.{schema}.silver_planner_forecasts")
cap = fc.groupBy("plant_id", "product_id", "date").agg(
    F.sum("demand_qty").alias("demand_qty"),
    F.sum("capacity_hrs").alias("capacity_hrs"),
    F.sum("planned_hrs").alias("planned_hrs"),
)
cap = cap.withColumn("utilization_pct", F.when(F.col("capacity_hrs") > 0, 100.0 * F.col("planned_hrs") / F.col("capacity_hrs")).otherwise(0.0))
cap.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_capacity_utilization")

# COMMAND ----------

print(f"Gold tables written to {catalog}.{schema}")
