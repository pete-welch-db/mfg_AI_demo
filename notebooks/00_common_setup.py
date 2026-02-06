# Databricks notebook source
# MAGIC %md
# MAGIC # Common setup
# MAGIC Shared configuration and seeds for DENSO manufacturing demo. Catalog and schema are configurable (widgets or env).

# COMMAND ----------

# MAGIC %pip install faker numpy --quiet

# COMMAND ----------

import os
import random

# COMMAND ----------

# Catalog and schema: from widget (job param) or env; never hard-coded
def get_catalog():
    try:
        return dbutils.widgets.get("catalog")
    except Exception:
        return os.environ.get("DEMO_CATALOG", "welch")

def get_schema():
    try:
        return dbutils.widgets.get("schema")
    except Exception:
        return os.environ.get("DEMO_SCHEMA", "denso_ai")

# COMMAND ----------

catalog = get_catalog()
schema = get_schema()
print(f"Using catalog={catalog}, schema={schema}")

# COMMAND ----------

# Reproducible seeds for Faker and numpy (used by 01_generate_bronze and downstream)
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# COMMAND ----------

import numpy as np
np.random.seed(RANDOM_SEED)

# COMMAND ----------

# Faker seed is set when Faker() is instantiated in 01_generate_bronze using this seed
FAKER_SEED = RANDOM_SEED

# COMMAND ----------

# Ensure catalog and schema exist (Unity Catalog)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# Return config for use in other notebooks (when run as library or %run)
def get_demo_config():
    return {
        "catalog": catalog,
        "schema": schema,
        "random_seed": RANDOM_SEED,
        "faker_seed": FAKER_SEED,
    }
