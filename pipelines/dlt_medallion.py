# Delta Live Tables medallion pipeline.
# Reads bronze tables (populated by 01_generate_bronze), builds silver (dedupe, type, expectations), then gold KPIs.
# Catalog and schema from pipeline configuration (spark.conf or env); never hard-coded.

import os

# Catalog/schema: pipeline config or env (set by DAB pipeline resource)
def _catalog():
    try:
        return spark.conf.get("pipeline.catalog", "welch")
    except Exception:
        return os.environ.get("catalog", "welch")

def _schema():
    try:
        return spark.conf.get("pipeline.schema", "denso_ai")
    except Exception:
        return os.environ.get("schema", "denso_ai")

# Use dlt for compatibility with Databricks DLT runtime
import dlt
from pyspark.sql import functions as F

_cat = _catalog()
_sch = _schema()
_prefix = f"{_cat}.{_sch}"

# ---------------------------------------------------------------------------
# Silver: fact tables from bronze (dedupe, type, expectations)
# Dimensions are already in silver from 01_generate_bronze; DLT only refines facts.
# ---------------------------------------------------------------------------

@dlt.table(name="silver_machine_events", comment="Cleaned machine events from MES/IoT")
@dlt.expect("valid_plant_id", "plant_id IS NOT NULL")
@dlt.expect("valid_event_ts", "event_ts IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
def silver_machine_events():
    df = spark.read.table(f"{_prefix}.bronze_machine_events")
    df = df.withColumn("duration_min", F.col("duration_min").cast("int"))
    df = df.withColumn("throughput_qty", F.col("throughput_qty").cast("int"))
    return df.dropDuplicates(["event_id"])

@dlt.table(name="silver_production_orders", comment="Cleaned production orders from ERP")
@dlt.expect("valid_plant_id", "plant_id IS NOT NULL")
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect("order_qty_non_neg", "order_qty >= 0")
def silver_production_orders():
    df = spark.read.table(f"{_prefix}.bronze_production_orders")
    df = df.withColumn("order_qty", F.col("order_qty").cast("int")).withColumn("planned_qty", F.col("planned_qty").cast("int"))
    return df.dropDuplicates(["order_id"])

@dlt.table(name="silver_shipments", comment="Cleaned shipments from logistics")
@dlt.expect("valid_plant_id", "plant_id IS NOT NULL")
@dlt.expect("valid_shipment_id", "shipment_id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect("qty_non_neg", "qty >= 0")
def silver_shipments():
    df = spark.read.table(f"{_prefix}.bronze_shipments")
    df = df.withColumn("qty", F.col("qty").cast("int"))
    df = df.withColumn("transit_days", F.col("transit_days").cast("int"))
    df = df.withColumn("on_time", F.col("on_time").cast("int"))
    return df.dropDuplicates(["shipment_id"])

@dlt.table(name="silver_material_availability", comment="Cleaned material availability from ASN/EDI")
@dlt.expect("valid_plant_id", "plant_id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect("lead_time_non_neg", "lead_time_days >= 0")
def silver_material_availability():
    df = spark.read.table(f"{_prefix}.bronze_material_availability")
    df = df.withColumn("asn_qty", F.col("asn_qty").cast("int")).withColumn("received_qty", F.col("received_qty").cast("int"))
    df = df.withColumn("lead_time_days", F.col("lead_time_days").cast("int")).withColumn("available", F.col("available").cast("int"))
    return df.dropDuplicates(["record_id"])

@dlt.table(name="silver_planner_forecasts", comment="Cleaned demand/capacity forecasts from S&OP")
@dlt.expect("valid_plant_id", "plant_id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect("demand_non_neg", "demand_qty >= 0")
def silver_planner_forecasts():
    df = spark.read.table(f"{_prefix}.bronze_planner_forecasts")
    df = df.withColumn("demand_qty", F.col("demand_qty").cast("int"))
    df = df.withColumn("capacity_hrs", F.col("capacity_hrs").cast("double")).withColumn("planned_hrs", F.col("planned_hrs").cast("double"))
    return df.dropDuplicates(["forecast_id"])

@dlt.table(name="silver_plant_pnl", comment="Cleaned P&L by plant/product/customer")
@dlt.expect("valid_plant_id", "plant_id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect("margin_reasonable", "margin_pct >= -100 AND margin_pct <= 100")
def silver_plant_pnl():
    df = spark.read.table(f"{_prefix}.bronze_plant_pnl")
    df = df.withColumn("revenue", F.col("revenue").cast("double")).withColumn("cost", F.col("cost").cast("double"))
    df = df.withColumn("margin_pct", F.col("margin_pct").cast("double"))
    return df.dropDuplicates(["record_id"])

# ---------------------------------------------------------------------------
# Gold: KPIs for app and Genie
# ---------------------------------------------------------------------------

@dlt.table(name="gold_plant_oee", comment="OEE and unplanned downtime by plant, work_center, product, period (product enables program filter)")
@dlt.expect("oee_in_range", "oee_pct >= 0 AND oee_pct <= 100")
def gold_plant_oee():
    events = dlt.read("silver_machine_events")
    total = events.groupBy("plant_id", "work_center_id", "product_id", "date").agg(
        F.sum("duration_min").alias("total_min"),
        F.sum(F.when(F.col("state") == "down", 1).otherwise(0)).alias("downtime_events"),
        F.sum(F.when(F.col("state") == "down", F.col("duration_min")).otherwise(0)).alias("downtime_min"),
        F.sum("throughput_qty").alias("throughput_qty"),
        F.count("*").alias("event_count"),
    )
    total = total.withColumn("availability_pct", F.least(100.0, F.greatest(0.0, 100.0 - 100.0 * F.col("downtime_min") / F.greatest(F.col("total_min"), 1))))
    total = total.withColumn("oee_pct", F.col("availability_pct"))
    return total.withColumn("week", F.weekofyear(F.col("date")))

@dlt.table(name="gold_on_time_delivery", comment="On-time delivery % by plant, customer, product family, period")
def gold_on_time_delivery():
    ship = dlt.read("silver_shipments")
    dim_product = spark.read.table(f"{_prefix}.silver_dim_product")
    ship_with_family = ship.join(F.broadcast(dim_product), ship.product_id == dim_product.product_id, "left").select(
        ship["plant_id"], ship["customer_id"], ship["date"],
        F.coalesce(dim_product["product_family"], F.lit("Unknown")).alias("product_family"),
        ship["on_time"],
    )
    agg = ship_with_family.groupBy("plant_id", "customer_id", "product_family", "date").agg(
        F.count("*").alias("total_shipments"),
        F.sum("on_time").alias("on_time_count"),
    )
    return agg.withColumn("on_time_pct", F.when(F.col("total_shipments") > 0, 100.0 * F.col("on_time_count") / F.col("total_shipments")).otherwise(0.0))

@dlt.table(name="gold_plant_margin", comment="Operating margin by plant, product, customer, period")
def gold_plant_margin():
    pnl = dlt.read("silver_plant_pnl")
    return pnl.groupBy("plant_id", "product_id", "customer_id", "date").agg(
        F.sum("revenue").alias("revenue"),
        F.sum("cost").alias("cost"),
        F.avg("margin_pct").alias("margin_pct"),
    )

@dlt.table(name="gold_inventory_lead_time", comment="Inventory turns and lead-time reliability by plant/supplier")
def gold_inventory_lead_time():
    mat = dlt.read("silver_material_availability")
    return mat.groupBy("plant_id", "supplier_id", "date").agg(
        F.sum("asn_qty").alias("asn_qty"),
        F.sum("received_qty").alias("received_qty"),
        F.avg("lead_time_days").alias("avg_lead_time_days"),
        F.avg("available").alias("availability_pct"),
    )

@dlt.table(name="gold_capacity_utilization", comment="Capacity vs plan by plant/product")
def gold_capacity_utilization():
    fc = dlt.read("silver_planner_forecasts")
    return fc.groupBy("plant_id", "product_id", "date").agg(
        F.sum("demand_qty").alias("demand_qty"),
        F.sum("capacity_hrs").alias("capacity_hrs"),
        F.sum("planned_hrs").alias("planned_hrs"),
    ).withColumn("utilization_pct", F.when(F.col("capacity_hrs") > 0, 100.0 * F.col("planned_hrs") / F.col("capacity_hrs")).otherwise(0.0))
