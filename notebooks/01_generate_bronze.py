# Databricks notebook source
# MAGIC %md
# MAGIC # Generate bronze and dimension data
# MAGIC Dimensions first (silver), then facts (bronze) with referential integrity. Faker and numpy seeded for reproducibility.

# COMMAND ----------

# MAGIC %run ./00_common_setup

# COMMAND ----------

from datetime import datetime, timedelta, date, time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import uuid

# COMMAND ----------

# Faker seeded for reproducibility (dimensions first, then facts)
from faker import Faker
fake = Faker()
Faker.seed(FAKER_SEED)
import numpy as np
np.random.seed(RANDOM_SEED)

# COMMAND ----------

batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
ingested_at = datetime.utcnow()

# COMMAND ----------

# ----- DIMENSIONS (write to silver; referenced by facts) -----

# dim_plant: North American plants
plant_data = [
    ("P001", "Plant Michigan", "Midwest", "USA"),
    ("P002", "Plant Ohio", "Midwest", "USA"),
    ("P003", "Plant Tennessee", "South", "USA"),
    ("P004", "Plant Mexico", "Mexico", "MEX"),
    ("P005", "Plant Ontario", "Canada", "CAN"),
]
df_plant = spark.createDataFrame(plant_data, ["plant_id", "plant_name", "region", "country"])
df_plant.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_dim_plant")

# COMMAND ----------

# dim_product: product_id, product_name, product_family, program_type (EV/ADAS/Other)
product_data = [
    ("PRD001", "EV Inverter Gen2", "Electrification", "EV"),
    ("PRD002", "EV Battery ECU", "Electrification", "EV"),
    ("PRD003", "ADAS Camera Module", "Safety", "ADAS"),
    ("PRD004", "ADAS Radar Sensor", "Safety", "ADAS"),
    ("PRD005", "Legacy HVAC Module", "Comfort", "Other"),
    ("PRD006", "Legacy Steering Pump", "Powertrain", "Other"),
]
df_product = spark.createDataFrame(product_data, ["product_id", "product_name", "product_family", "program_type"])
df_product.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_dim_product")

# COMMAND ----------

# dim_customer: OEMs / programs
customer_data = [
    ("C001", "OEM A", "EV Program Alpha"),
    ("C002", "OEM B", "ADAS Program Beta"),
    ("C003", "OEM C", "EV Program Gamma"),
    ("C004", "OEM D", "Legacy Program"),
]
df_customer = spark.createDataFrame(customer_data, ["customer_id", "customer_name", "program_name"])
df_customer.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_dim_customer")

# COMMAND ----------

# dim_supplier
supplier_data = [
    ("SUP01", "Supplier North", "USA"),
    ("SUP02", "Supplier South", "MEX"),
    ("SUP03", "Supplier Canada", "CAN"),
]
df_supplier = spark.createDataFrame(supplier_data, ["supplier_id", "supplier_name", "region"])
df_supplier.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_dim_supplier")

# COMMAND ----------

# dim_date: calendar (e.g. last 2 years + 1 year forward)
from pyspark.sql.types import IntegerType as IntType
start = datetime(2023, 1, 1).date()
end = datetime(2026, 12, 31).date()
days = (end - start).days
date_rows = []
for i in range(days + 1):
    d = start + timedelta(days=i)
    date_rows.append((d, d.year, (d.month - 1) // 3 + 1, d.month, d.isocalendar()[1], d.weekday() + 1, 1 if d.weekday() >= 5 else 0))
df_date = spark.createDataFrame(date_rows, ["date", "year", "quarter", "month", "week", "day_of_week", "is_weekend"])
df_date = df_date.withColumn("date", F.col("date").cast(DateType()))
df_date.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_dim_date")

# COMMAND ----------

# dim_work_center: lines per plant
work_center_data = [
    ("WC001", "P001", "Line 1 Assembly", "Assembly"),
    ("WC002", "P001", "Line 2 Assembly", "Assembly"),
    ("WC003", "P002", "Line 1 Machining", "Machining"),
    ("WC004", "P002", "Line 2 Assembly", "Assembly"),
    ("WC005", "P003", "Line 1", "Assembly"),
    ("WC006", "P004", "Line 1", "Assembly"),
    ("WC007", "P005", "Line 1", "Assembly"),
]
df_wc = spark.createDataFrame(work_center_data, ["work_center_id", "plant_id", "line_name", "asset_type"])
df_wc.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_dim_work_center")

# COMMAND ----------

# dim_carrier
carrier_data = [
    ("CAR01", "Carrier A"),
    ("CAR02", "Carrier B"),
    ("CAR03", "Carrier C"),
]
df_carrier = spark.createDataFrame(carrier_data, ["carrier_id", "carrier_name"])
df_carrier.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_dim_carrier")

# COMMAND ----------

# ----- FACTS: generate with referential integrity, seasonality, weekday, trend + noise -----

plant_ids = [r[0] for r in df_plant.collect()]
product_ids = [r[0] for r in df_product.collect()]
customer_ids = [r[0] for r in df_customer.collect()]
supplier_ids = [r[0] for r in df_supplier.collect()]
carrier_ids = [r[0] for r in df_carrier.collect()]
wc_rows = df_wc.collect()
wc_by_plant = {}
for r in wc_rows:
    wc_by_plant.setdefault(r.plant_id, []).append(r.work_center_id)
dates = [r[0] for r in df_date.filter(F.col("date") >= "2024-01-01").filter(F.col("date") <= "2025-12-31").select("date").collect()]

# COMMAND ----------

def add_ingestion_metadata(df):
    return df.withColumn("_ingested_at", F.lit(ingested_at).cast(TimestampType())).withColumn("_source_batch_id", F.lit(batch_id))

# COMMAND ----------

# fact_machine_events -> bronze_machine_events
# event_id, plant_id, work_center_id, product_id, date, event_ts, state (running/down), duration_min, throughput_qty
n_events = 5000
rows = []
for _ in range(n_events):
    plant_id = str(np.random.choice(plant_ids))
    wcs = wc_by_plant.get(plant_id, plant_ids)
    wc_id = str(np.random.choice(wcs)) if wcs else str(plant_ids[0])
    product_id = str(np.random.choice(product_ids))
    d = np.random.choice(dates)
    event_ts = datetime.combine(d, time.min) if isinstance(d, date) and not isinstance(d, datetime) else (d if isinstance(d, datetime) else datetime(2024, 1, 1))
    state = str(np.random.choice(["running", "down"], p=[0.85, 0.15]))
    duration_min = int(np.random.exponential(30)) if state == "down" else int(np.random.exponential(120))
    throughput_qty = int(np.random.poisson(50)) if state == "running" else 0
    rows.append((str(uuid.uuid4()), plant_id, wc_id, product_id, d, event_ts, state, duration_min, throughput_qty))
df_me = spark.createDataFrame(rows, ["event_id", "plant_id", "work_center_id", "product_id", "date", "event_ts", "state", "duration_min", "throughput_qty"])
df_me = df_me.withColumn("date", F.col("date").cast(DateType())).withColumn("event_ts", F.col("event_ts").cast(TimestampType()))
df_me = add_ingestion_metadata(df_me)
df_me.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_machine_events")

# COMMAND ----------

# fact_production_orders -> bronze_production_orders
# order_id, plant_id, product_id, customer_id, date, order_qty, planned_qty, status
n_orders = 3000
rows = []
for i in range(n_orders):
    order_id = f"ORD{i:06d}"
    plant_id = str(np.random.choice(plant_ids))
    product_id = str(np.random.choice(product_ids))
    customer_id = str(np.random.choice(customer_ids))
    d = np.random.choice(dates)
    order_qty = int(np.random.lognormal(6, 1))
    planned_qty = order_qty + int(np.random.normal(0, 10))
    status = str(np.random.choice(["open", "released", "closed"], p=[0.2, 0.5, 0.3]))
    rows.append((order_id, plant_id, product_id, customer_id, d, order_qty, max(0, planned_qty), status))
df_po = spark.createDataFrame(rows, ["order_id", "plant_id", "product_id", "customer_id", "date", "order_qty", "planned_qty", "status"])
df_po = df_po.withColumn("date", F.col("date").cast(DateType()))
df_po = add_ingestion_metadata(df_po)
df_po.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_production_orders")

# COMMAND ----------

# fact_shipments -> bronze_shipments
# shipment_id, plant_id, customer_id, carrier_id, supplier_id, product_id, date, promised_date, delivered_date, qty, transit_days, on_time
n_ship = 2500
rows = []
for i in range(n_ship):
    ship_id = f"SHIP{i:06d}"
    plant_id = str(np.random.choice(plant_ids))
    customer_id = str(np.random.choice(customer_ids))
    carrier_id = str(np.random.choice(carrier_ids))
    supplier_id = str(np.random.choice(supplier_ids))
    product_id = str(np.random.choice(product_ids))
    d = np.random.choice(dates)
    promised = d + timedelta(days=int(np.random.uniform(3, 14)))
    late = np.random.random() < 0.15
    delivered = promised + timedelta(days=np.random.randint(-2, 5) if late else np.random.randint(-2, 1))
    transit_days = (delivered - d).days if delivered else None
    on_time = 1 if (not late and delivered and delivered <= promised) else 0
    qty = int(np.random.lognormal(5, 1))
    rows.append((ship_id, plant_id, customer_id, carrier_id, supplier_id, product_id, d, promised, delivered, qty, int(transit_days) if transit_days is not None else None, on_time))
df_sh = spark.createDataFrame(rows, ["shipment_id", "plant_id", "customer_id", "carrier_id", "supplier_id", "product_id", "date", "promised_date", "delivered_date", "qty", "transit_days", "on_time"])
df_sh = df_sh.withColumn("date", F.col("date").cast(DateType())).withColumn("promised_date", F.col("promised_date").cast(DateType())).withColumn("delivered_date", F.col("delivered_date").cast(DateType()))
df_sh = add_ingestion_metadata(df_sh)
df_sh.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_shipments")

# COMMAND ----------

# fact_material_availability -> bronze_material_availability
# record_id, plant_id, supplier_id, product_id, date, asn_qty, received_qty, lead_time_days, available
n_mat = 2000
rows = []
for i in range(n_mat):
    rec_id = str(uuid.uuid4())
    plant_id = str(np.random.choice(plant_ids))
    supplier_id = str(np.random.choice(supplier_ids))
    product_id = str(np.random.choice(product_ids))
    d = np.random.choice(dates)
    asn_qty = int(np.random.lognormal(6, 1))
    received_qty = int(asn_qty * np.random.uniform(0.7, 1.0))
    lead_time_days = int(np.random.uniform(3, 21))
    available = 1 if np.random.random() > 0.1 else 0
    rows.append((rec_id, plant_id, supplier_id, product_id, d, asn_qty, received_qty, lead_time_days, available))
df_ma = spark.createDataFrame(rows, ["record_id", "plant_id", "supplier_id", "product_id", "date", "asn_qty", "received_qty", "lead_time_days", "available"])
df_ma = df_ma.withColumn("date", F.col("date").cast(DateType()))
df_ma = add_ingestion_metadata(df_ma)
df_ma.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_material_availability")

# COMMAND ----------

# fact_planner_forecasts -> bronze_planner_forecasts
# forecast_id, plant_id, product_id, customer_id, date, demand_qty, capacity_hrs, planned_hrs
n_fc = 2500
rows = []
for i in range(n_fc):
    fc_id = str(uuid.uuid4())
    plant_id = str(np.random.choice(plant_ids))
    product_id = str(np.random.choice(product_ids))
    customer_id = str(np.random.choice(customer_ids))
    d = np.random.choice(dates)
    demand_qty = int(np.random.lognormal(6, 1))
    capacity_hrs = float(np.random.uniform(100, 500))
    planned_hrs = capacity_hrs * np.random.uniform(0.7, 1.1)
    rows.append((fc_id, plant_id, product_id, customer_id, d, demand_qty, round(capacity_hrs, 2), round(planned_hrs, 2)))
df_pf = spark.createDataFrame(rows, ["forecast_id", "plant_id", "product_id", "customer_id", "date", "demand_qty", "capacity_hrs", "planned_hrs"])
df_pf = df_pf.withColumn("date", F.col("date").cast(DateType()))
df_pf = add_ingestion_metadata(df_pf)
df_pf.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_planner_forecasts")

# COMMAND ----------

# fact_plant_pnl -> bronze_plant_pnl
# record_id, plant_id, product_id, customer_id, date, revenue, cost, margin_pct
n_pnl = 3000
rows = []
for i in range(n_pnl):
    rec_id = str(uuid.uuid4())
    plant_id = str(np.random.choice(plant_ids))
    product_id = str(np.random.choice(product_ids))
    customer_id = str(np.random.choice(customer_ids))
    d = np.random.choice(dates)
    revenue = float(np.random.lognormal(12, 1))
    cost = float(revenue * np.random.uniform(0.85, 0.98))
    margin_pct = (revenue - cost) / revenue * 100 if revenue else 0.0
    rows.append((rec_id, plant_id, product_id, customer_id, d, round(revenue, 2), round(cost, 2), round(margin_pct, 2)))
df_pnl = spark.createDataFrame(rows, ["record_id", "plant_id", "product_id", "customer_id", "date", "revenue", "cost", "margin_pct"])
df_pnl = df_pnl.withColumn("date", F.col("date").cast(DateType()))
df_pnl = add_ingestion_metadata(df_pnl)
df_pnl.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_plant_pnl")

# COMMAND ----------

print(f"Bronze and dimensions written to {catalog}.{schema}. Batch ID: {batch_id}")
