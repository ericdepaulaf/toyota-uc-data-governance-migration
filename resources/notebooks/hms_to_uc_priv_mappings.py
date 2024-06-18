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
dbutils.widgets.text("tf_file_dst_volume", "/Volumes/users/wagner_silveira/tf", "Destination Volume for the TF file")
hms_privileges_tbl = dbutils.widgets.get("src_table")
tf_file_dst_volume = dbutils.widgets.get("tf_file_dst_volume")

pattern = re.compile(r"^[a-zA-Z0-9_-`]+\.[a-zA-Z0-9_-`]+\.[a-zA-Z0-9_-`]+$")
if not pattern.match(hms_privileges_tbl):
    raise ValueError("Invalid source table name. Please use the format: catalog.database.table_name")

# COMMAND ----------

HMS_UC_PRIVILEGES_MAPPING = {
    "ALL PRIVILEGES": ["ALL_PRIVILEGES"],
    "READ_METADATA": ["APPLY_TAG", "BROWSE"],
    "USAGE" : ["USE_CATALOG", "USE_SCHEMA"],
    "SELECT": ["SELECT"],
    "CREATE": ["CREATE_TABLE", "CREATE_SCHEMA"],
    "MODIFY": ["MODIFY"],
    "CREATE_NAMED_FUNCTION": ["CREATE_FUNCTION"]
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
      .withColumn("uc_privileges", F.when(F.col("object_type") == "TABLE", F.array_except(F.col("uc_privileges"), F.lit(["BROWSE", "CREATE_CATALOG", "CREATE_SCHEMA", "CREATE_TABLE"])))
                                    .when(F.col("object_type") == "SCHEMA", F.array_except(F.col("uc_privileges"), F.lit(["BROWSE", "CREATE_SCHEMA", "CREATE_CATALOG", "USE_CATALOG"])))
                                    .when(F.col("object_type") == "CATALOG", F.array_except(F.col("uc_privileges"), F.lit(["CREATE_TABLE", "CREATE_CATALOG", "USE_SCHEMA"])))
                                    .otherwise(F.col("uc_privileges")))
      .groupBy("principal", "hms_object", "object_type")
      .agg(F.sort_array(F.array_distinct(F.flatten(F.collect_list("uc_privileges"))), asc=False).alias("uc_privileges"), F.array_distinct(F.collect_list("action_type")).alias("hms_privileges"))
      .groupBy("hms_object")
      .agg(F.collect_list(F.concat(F.lit("[\""), F.regexp_replace(F.concat_ws(",", "uc_privileges"), ",", "\",\""), F.lit("\"]"))).alias("uc_privileges"),
           F.collect_list("hms_privileges").alias("hms_privileges"), 
           F.collect_list("principal").alias("principal"),
           F.mode("object_type").alias("object_type"))
      .withColumn("hms_object_sanitized", F.lower(F.regexp_replace(F.col("hms_object"), "[^a-zA-Z0-9_]", "_")))
      .withColumn("grants",F.concat_ws("\n\n", F.zip_with("principal", "uc_privileges", lambda x, y: F.concat(F.lit("\tgrant {\n\t\tprincipal  = \""), x, F.lit("\""), F.lit("\n\t\tprivileges  = "),y, F.lit("\n\t}")))))
)

# COMMAND ----------

df.display()

# COMMAND ----------

from utils.consts import *

databricks_catalogs_df = (
   df.filter(F.col("object_type") == F.lit("CATALOG"))
   .select("hms_object", "hms_object_sanitized", "principal", "uc_privileges", "grants")
   .distinct()
   .withColumn("output", F.concat(F.lit("resource \"databricks_grants\" \"catalog_"),
                                  F.lower(F.col("hms_object_sanitized")),
                                  F.lit("\" {\n\tcatalog = \""), 
                                  F.col("hms_object"),                                
                                  F.lit("\"\n\n"),
                                  F.col("grants"),
                                  F.lit("\n}"),
                                  F.lit("\n")
                                  )
               )
   .select("output"))


databricks_schemas_df = (
   df.filter(F.col("object_type") == F.lit("SCHEMA"))
   .select("hms_object", "hms_object_sanitized", "principal", "uc_privileges", "grants")
   .distinct()
   .withColumn("output", F.concat(F.lit("resource \"databricks_grants\" \"schema_"),
                                  F.lower(F.col("hms_object_sanitized")),
                                  F.lit("\" {\n\tschema = \""), 
                                  F.col("hms_object"),
                                  F.lit("\"\n\n"),
                                  F.col("grants"),
                                  F.lit("\n}"),
                                  F.lit("\n")
                                  )
               )
   .select("output"))

databricks_tables_df = (
   df.filter(F.col("object_type") == F.lit("TABLE"))
     .select("hms_object", "hms_object_sanitized", "principal", "uc_privileges", "grants")
     .distinct()
     .withColumn("output", F.concat(F.lit("resource \"databricks_grants\" \"table_"),
                                  F.lower(F.col("hms_object_sanitized")),
                                  F.lit("\" {\n\ttable = \""), 
                                  F.col("hms_object"),                                 
                                  F.lit("\"\n\n"),
                                  F.col("grants"),
                                  F.lit("\n}"),
                                  F.lit("\n")
                                  )
               )
     .select("output"))

tf_output_df = (
    spark.createDataFrame([(TF_TEMPLATE_INIT, )], "output string")
    .union(databricks_catalogs_df)
    .union(databricks_schemas_df)
    .union(databricks_tables_df)
)

tf_output_df.display()

# COMMAND ----------

(
    tf_output_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .text(f"{tf_file_dst_volume}/tf_exported_acl_grants_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.tf")
)

# COMMAND ----------

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HCL converter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
