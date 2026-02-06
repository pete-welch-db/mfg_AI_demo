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


# ---------------------------------------------------------------------------
# Table and column comments for Genie and AI (Unity Catalog)
# Run after saveAsTable to set table description and per-column comments.
# ---------------------------------------------------------------------------
def _escape_sql_comment(s):
    if s is None:
        return ""
    return str(s).replace("'", "''")

def apply_table_metadata(cat, sch, table_name, table_comment, column_comments):
    """Set table comment and column comments for a Delta/UC table. Helps Genie and AI use source metadata."""
    full_name = f"{cat}.{sch}.{table_name}"
    if table_comment:
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{_escape_sql_comment(table_comment)}'")
    for col_name, comment in (column_comments or {}).items():
        if comment:
            spark.sql(f"COMMENT ON COLUMN {full_name}.{col_name} IS '{_escape_sql_comment(comment)}'")
