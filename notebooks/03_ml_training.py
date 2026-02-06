# Databricks notebook source
# MAGIC %md
# MAGIC # ML training: delivery risk model
# MAGIC Train a model to predict late delivery risk; register in Unity Catalog; score predictions back to Delta (ml_delivery_risk_predictions).

# COMMAND ----------

# MAGIC %run ./00_common_setup

# COMMAND ----------

# MAGIC %pip install scikit-learn mlflow --quiet

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# Unity Catalog model registry
mlflow.set_registry_uri("databricks-uc")
model_name = f"{catalog}.{schema}.delivery_risk_model"

# COMMAND ----------

# Build training dataset from silver: shipments + features from orders/material/forecasts
ship = spark.table(f"{catalog}.{schema}.silver_shipments").toPandas()
orders = spark.table(f"{catalog}.{schema}.silver_production_orders").toPandas()
mat = spark.table(f"{catalog}.{schema}.silver_material_availability").toPandas()
fc = spark.table(f"{catalog}.{schema}.silver_planner_forecasts").toPandas()

# COMMAND ----------

# Target: late delivery (1 = late, 0 = on time). We predict probability of late.
ship["late"] = (ship["on_time"] == 0).astype(int)
# Features from shipments
ship["date"] = pd.to_datetime(ship["date"])
ship["day_of_week"] = ship["date"].dt.dayofweek
ship["month"] = ship["date"].dt.month
# Aggregate order backlog by plant/date for join
orders["date"] = pd.to_datetime(orders["date"])
order_agg = orders.groupby(["plant_id", "date"]).agg(order_count=("order_id", "count"), total_qty=("order_qty", "sum")).reset_index()
order_agg = order_agg.rename(columns={"order_count": "order_backlog", "total_qty": "order_qty_sum"})
# Material availability by plant/date
mat["date"] = pd.to_datetime(mat["date"])
mat_agg = mat.groupby(["plant_id", "date"]).agg(avg_lead_time=("lead_time_days", "mean"), avail_pct=("available", "mean")).reset_index()
# Capacity utilization by plant/date
fc["date"] = pd.to_datetime(fc["date"])
fc_agg = fc.groupby(["plant_id", "date"]).agg(capacity_hrs=("capacity_hrs", "sum"), planned_hrs=("planned_hrs", "sum")).reset_index()
fc_agg["utilization_pct"] = (fc_agg["planned_hrs"] / fc_agg["capacity_hrs"].replace(0, 1)) * 100

# COMMAND ----------

# Merge features into ship
train_df = ship.merge(order_agg, on=["plant_id", "date"], how="left")
train_df = train_df.merge(mat_agg, on=["plant_id", "date"], how="left")
train_df = train_df.merge(fc_agg, on=["plant_id", "date"], how="left")
train_df = train_df.fillna({"order_backlog": 0, "order_qty_sum": 0, "avg_lead_time": 7, "avail_pct": 1, "utilization_pct": 80})

# COMMAND ----------

# Encode categoricals for sklearn
le_plant = LabelEncoder()
le_cust = LabelEncoder()
le_carrier = LabelEncoder()
train_df["plant_enc"] = le_plant.fit_transform(train_df["plant_id"].astype(str))
train_df["customer_enc"] = le_cust.fit_transform(train_df["customer_id"].astype(str))
train_df["carrier_enc"] = le_carrier.fit_transform(train_df["carrier_id"].astype(str))

feature_cols = ["plant_enc", "customer_enc", "carrier_enc", "day_of_week", "month", "qty", "order_backlog", "order_qty_sum", "avg_lead_time", "avail_pct", "utilization_pct"]
X = train_df[feature_cols]
y = train_df["late"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=RANDOM_SEED, stratify=y)

# COMMAND ----------

with mlflow.start_run():
    clf = RandomForestClassifier(n_estimators=50, max_depth=8, random_state=RANDOM_SEED)
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)
    proba = clf.predict_proba(X_test)[:, 1]
    mlflow.log_param("n_estimators", 50)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("feature_cols", feature_cols)
    mlflow.log_metric("accuracy", accuracy_score(y_test, pred))
    mlflow.log_metric("roc_auc", roc_auc_score(y_test, proba))
    mlflow.sklearn.log_model(clf, "model", registered_model_name=model_name)
    run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# Load model from this run and score full dataset; write to ml_delivery_risk_predictions
model_uri = f"runs:/{run_id}/model"
loaded = mlflow.sklearn.load_model(model_uri)
train_df["probability_late"] = loaded.predict_proba(train_df[feature_cols])[:, 1]
train_df["prediction_late"] = (train_df["probability_late"] >= 0.5).astype(int)

# COMMAND ----------

# Write predictions to Delta
out = train_df[["shipment_id", "plant_id", "customer_id", "date", "prediction_late", "probability_late"]].copy()
out["model_version"] = "1"
out["scored_at"] = pd.Timestamp.utcnow()
out.columns = ["shipment_id", "plant_id", "customer_id", "date", "prediction", "probability", "model_version", "scored_at"]
spark_out = spark.createDataFrame(out)
spark_out.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.ml_delivery_risk_predictions")
apply_table_metadata(catalog, schema, "ml_delivery_risk_predictions", "Delivery risk model predictions; probability and binary prediction of late delivery per shipment.",
    {"shipment_id": "Shipment identifier", "plant_id": "Plant", "customer_id": "Customer", "date": "Ship date", "prediction": "1 if predicted late, 0 otherwise", "probability": "Probability of late delivery", "model_version": "Model version used", "scored_at": "Scoring timestamp"})

# COMMAND ----------

print(f"Model registered as {model_name}; predictions in {catalog}.{schema}.ml_delivery_risk_predictions")
