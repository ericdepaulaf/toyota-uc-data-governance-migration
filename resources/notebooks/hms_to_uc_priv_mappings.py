# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Toyota Mortors North America - Map HMS Privileges to UC Privileges
# MAGIC
# MAGIC ##### Description:
# MAGIC - This notebook will convert the exported HMS governance privileges to UC.
# MAGIC

# COMMAND ----------
import re
import logging
import datetime

from pyspark.sql import functions as F, Column

# COMMAND ----------
start_time = datetime.datetime.now()

# COMMAND ----------
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# COMMAND ----------
dbutils.widgets.text("src_table", "wsilveira.wsilveira.tbdp_prod_grants_phase_1", "HMS privileges table")
hms_privileges_tbl = dbutils.widgets.get("src_table")

pattern = re.compile(r"^[a-zA-Z0-9_-`]+\.[a-zA-Z0-9_-`]+\.[a-zA-Z0-9_-`]+$")
if not pattern.match(hms_privileges_tbl):
    raise ValueError("Invalid source table name. Please use the format: catalog.database.table_name")

# COMMAND ----------
HMS_UC_PRIVILEGES_MAPPING = {
    "READ_METADATA": ["USE_CATALOG", "USE_SCHEMA", "BROWSE"],
    "SELECT": ["USE_CATALOG", "USE_SCHEMA","SELECT"],
    "CREATE": ["USE_CATALOG", 
               "USE_SCHEMA",
               "APPLY_TAG",
               "BROWSE",
               "EXECUTE",
               "MODIFY",
               "READ_VOLUME",
               "REFRESH",
               "SELECT",
               "WRITE_VOLUME",
               "CREATE_FUNCTION",
               "CREATE_MATERIZALIZED_VIEW",
               "CREATE_MODEL",
               "CREATE_TABLE",
               "CREATE_VOLUME"],
    "MODIFY": ["USE_CATALOG",
               "USE_SCHEMA",
               "APPLY_TAG",
               "BROWSE",
               "EXECUTE",
               "MODIFY",
               "READ_VOLUME",
               "REFRESH",
               "SELECT"],
            #    "WRITE_VOLUME",
            #    "CREATE_FUNCTION",
            #    "CREATE_MATERIZALIZED_VIEW",
            #    "CREATE_MODEL",
            #    "CREATE_TABLE",
            #    "CREATE_VOLUME"],
}

# COMMAND ----------
df = spark.table(hms_privileges_tbl)

when_cond = None
for hms_priv, uc_privs in HMS_UC_PRIVILEGES_MAPPING.items():
    if isinstance(when_cond, Column):
        when_cond = when_cond.when(F.upper(F.col("action_type")) == F.lit(hms_priv), F.lit(uc_privs))
    else:
        when_cond = F.when(F.upper(F.col("action_type")) == F.lit(hms_priv), F.lit(uc_privs))

when_cond = when_cond.otherwise(F.lit([]))

logger.info(f"Mapping conditions {when_cond}")

df = (
    df.withColumn("uc_privileges", when_cond)
    .withColumns({
        "object_type": F.when(F.col("table").isNotNull() | F.col("view").isNotNull(), F.lit("TABLE"))
                        .when(F.col("database").isNotNull(), F.lit("SCHEMA"))
                        .when(F.col("catalog").isNotNull(), F.lit("CATALOG"))
                        .otherwise(F.lit("UNKNOWN")),
        "hms_object": F.concat_ws(".", F.col("catalog"), F.col("database"), F.col("table"))
        })
    .groupBy("principal", "hms_object", "object_type")
    .agg(F.array_distinct(F.flatten(F.collect_list("uc_privileges"))).alias("uc_privileges"), F.array_distinct(F.collect_list("action_type")).alias("hms_privileges"))
    .withColumn("hms_object_sanitized", F.lower(F.regexp_replace(F.col("hms_object"), "[^a-zA-Z0-9_]", "_")))
)

# COMMAND ----------
df.display()

# COMMAND ----------
# catalog_datasources_df = (
#   df.filter(F.col("object_type") == F.lit("CATALOG"))
#   .select("hms_object", "hms_object_sanitized")
#   .distinct()
#   .withColumn("output", F.concat(F.lit("data \"databricks_catalog\" \"cat_"),
#                                  F.lower(F.col("hms_object_sanitized")),
#                                  F.lit("\" {\n\tname = \""),
#                                  F.col("hms_object"),
#                                  F.lit("\"\n}")))
#   .select("output"))