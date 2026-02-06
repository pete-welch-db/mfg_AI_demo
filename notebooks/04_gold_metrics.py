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
    F.sum(F.when(F.col("state") == "down", F.col("duration_min")).otherwise(F.lit(0))).alias("downtime_min"),
    F.sum("throughput_qty").alias("throughput_qty"),
)
total = total.withColumn("availability_pct", F.least(F.lit(100.0), F.greatest(F.lit(0.0), F.lit(100.0) - F.lit(100.0) * F.col("downtime_min") / F.greatest(F.col("total_min"), F.lit(1)))))
total = total.withColumn("oee_pct", F.col("availability_pct")).withColumn("week", F.weekofyear(F.col("date")))
total.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_plant_oee")
apply_table_metadata(catalog, schema, "gold_plant_oee", "OEE and unplanned downtime by plant, work center, product, date; product enables program filter.",
    {"plant_id": "Plant", "work_center_id": "Work center", "product_id": "Product", "date": "Date", "total_min": "Total minutes", "downtime_min": "Unplanned downtime minutes", "throughput_qty": "Units produced", "availability_pct": "Availability %", "oee_pct": "OEE %", "week": "ISO week number"})

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
agg = agg.withColumn("on_time_pct", F.when(F.col("total_shipments") > F.lit(0), F.lit(100.0) * F.col("on_time_count") / F.col("total_shipments")).otherwise(F.lit(0.0)))
agg.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_on_time_delivery")
apply_table_metadata(catalog, schema, "gold_on_time_delivery", "On-time delivery % by plant, customer, product family, date.",
    {"plant_id": "Plant", "customer_id": "Customer", "product_family": "Product family", "date": "Date", "total_shipments": "Total shipment count", "on_time_count": "On-time shipment count", "on_time_pct": "On-time delivery percentage"})

# COMMAND ----------

# gold_plant_margin
pnl = spark.table(f"{catalog}.{schema}.silver_plant_pnl")
margin = pnl.groupBy("plant_id", "product_id", "customer_id", "date").agg(
    F.sum("revenue").alias("revenue"),
    F.sum("cost").alias("cost"),
    F.avg("margin_pct").alias("margin_pct"),
)
margin.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_plant_margin")
apply_table_metadata(catalog, schema, "gold_plant_margin", "Operating margin by plant, product, customer, date.",
    {"plant_id": "Plant", "product_id": "Product", "customer_id": "Customer", "date": "Date", "revenue": "Total revenue", "cost": "Total cost", "margin_pct": "Average margin %"})

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
apply_table_metadata(catalog, schema, "gold_inventory_lead_time", "Inventory and lead-time reliability by plant, supplier, date.",
    {"plant_id": "Plant", "supplier_id": "Supplier", "date": "Date", "asn_qty": "Total ASN quantity", "received_qty": "Total received quantity", "avg_lead_time_days": "Average lead time in days", "availability_pct": "Availability proportion"})

# COMMAND ----------

# gold_capacity_utilization
fc = spark.table(f"{catalog}.{schema}.silver_planner_forecasts")
cap = fc.groupBy("plant_id", "product_id", "date").agg(
    F.sum("demand_qty").alias("demand_qty"),
    F.sum("capacity_hrs").alias("capacity_hrs"),
    F.sum("planned_hrs").alias("planned_hrs"),
)
cap = cap.withColumn("utilization_pct", F.when(F.col("capacity_hrs") > F.lit(0), F.lit(100.0) * F.col("planned_hrs") / F.col("capacity_hrs")).otherwise(F.lit(0.0)))
cap.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_capacity_utilization")
apply_table_metadata(catalog, schema, "gold_capacity_utilization", "Capacity vs plan by plant, product, date; utilization %.",
    {"plant_id": "Plant", "product_id": "Product", "date": "Date", "demand_qty": "Demand quantity", "capacity_hrs": "Capacity hours", "planned_hrs": "Planned hours", "utilization_pct": "Utilization percentage"})

# COMMAND ----------

print(f"Gold tables written to {catalog}.{schema}")
