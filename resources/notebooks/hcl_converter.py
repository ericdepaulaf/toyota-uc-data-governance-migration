# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Toyota Mortors North America - Converts exported HMS governance privileges to HCL
# MAGIC
# MAGIC ##### Description:
# MAGIC - This notebook will convert the exported HMS governance privileges to HCL.
# MAGIC

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.25.1 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import datetime
import logging
import re
import sys
import pathlib

from pyspark.sql import functions as F, types as T

# COMMAND ----------

path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path = "/Workspace" + path if not path.startswith("/Workspace") else path

modules_path = pathlib.Path(path).joinpath("../../..").resolve()

# Allow for execution outside of Databricks Repos directory
sys.path.append(str(modules_path))

from utils.consts import *

# COMMAND ----------

start_time = datetime.datetime.now()

# COMMAND ----------

dbutils.widgets.text("src_table", "users.wagner_silveira.hms_governance_update_2", "Source Table Name")
dbutils.widgets.text("tf_file_dst_volume", "/Volumes/users/wagner_silveira/tf", "Destination Volume for the TF file")
src_table = dbutils.widgets.get("src_table")
tf_file_dst_volume = dbutils.widgets.get("tf_file_dst_volume")

pattern = re.compile(r"^[a-zA-Z0-9_-]+.[a-zA-Z0-9_-]+.[a-zA-Z0-9_-]+$")
if not pattern.match(src_table):
    raise ValueError("Invalid source table name. Please use the format: catalog.database.table_name")

tf_file_dst_volume = tf_file_dst_volume.rstrip("/")
pattern = re.compile(r"^/Volumes/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$")
if not pattern.match(tf_file_dst_volume):
    raise ValueError("Invalid volume path. Please use the format: /Volumes/catalog_name/schema_name/volume_name")

# COMMAND ----------

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# COMMAND ----------

logger.info("Fetching External Locations...")

w = WorkspaceClient()

k = ["name", "credential_name", "credential_id", "url"]
ext_loc = []

try:
    api_ext_loc = w.external_locations.list()
    for loc in api_ext_loc:
        loc = loc.as_dict()
        ext_loc.append({key: loc[key] for key in k})
except Exception as e:
    logger.error(f"Failed to list external locations: {e}")
    raise e

schema = T.StructType([
    T.StructField("name", T.StringType(), False, {"metadata": {"description": "The external location name"}}),
    T.StructField("credential_name", T.StringType(), False, {"metadata": {"description": "The credential name"}}),
    T.StructField("credential_id", T.StringType(), False, {"metadata": {"description": "The credential ID"}}),
    T.StructField("url", T.StringType(), False, {"metadata": {"description": "S3 path of the external location"}}),
])

ext_loc_df = (
    spark.createDataFrame(ext_loc, schema)
    .withColumnRenamed("name", "ext_loc_name")
)

logger.info(f"{ext_loc_df.count()} External Locations fetched successfully.")

# COMMAND ----------

ext_loc_df.display()

# COMMAND ----------

logger.info("Fetching HMS Governance data...")

hms_governance_df = (
    spark.table(src_table).filter((F.col("s3_paths_regex").isNotNull()) & (F.col("principal_display_name").isNotNull()))
    .withColumn("s3_paths_regex", F.explode(F.col("s3_paths_regex")))
)

hms_governance_df.display()

logger.info(f"{hms_governance_df.count()} entries for HMS Governance data fetched successfully.")

# COMMAND ----------

logger.info("Joining External Locations with HMS Governance data...")

joined_df = (
    hms_governance_df.crossJoin(ext_loc_df)
    .withColumn("access_granted", F.regexp_like(F.col("url"), F.col("s3_paths_regex")))
    .filter(F.col("access_granted") == F.lit(True))
    .select("instance_profile_arn", "role_name", "principal_display_name", "principal_id", "ext_loc_name", "credential_id", "credential_name", "url")
    .withColumns({
        "sanitized_ext_loc_name": F.lower(F.regexp_replace(F.col("ext_loc_name"), "[^a-zA-Z0-9_]", "_")),
        "sanitized_principal_display_name": F.lower(F.regexp_replace(F.col("principal_display_name"), "[^a-zA-Z0-9_]", "_")),
    })
    .distinct()
)

# COMMAND ----------

joined_df.display()

# COMMAND ----------

logger.info("Generating HCL...")

ext_loc_datasources_df = (
  joined_df.select("ext_loc_name", "sanitized_ext_loc_name")
  .distinct()
  .withColumn("output", F.concat(F.lit("data \"databricks_external_location\" \"ext_loc_"),
                                 F.lower(F.col("sanitized_ext_loc_name")),
                                 F.lit("\" {\n\tname = \""),
                                 F.col("ext_loc_name"),
                                 F.lit("\"\n}")))
  .select("output"))

grant_ext_loc_resources_df = (
    joined_df.select("sanitized_principal_display_name", "sanitized_ext_loc_name", "principal_display_name")
    .distinct()
    .withColumn("output", F.concat(F.lit("resource \"databricks_grant\" \"principal_"),
                                     F.col("sanitized_principal_display_name"),
                                     F.lit("_"),
                                     F.col("sanitized_ext_loc_name"),
                                     F.lit("\" {\n\texternal_location = data.databricks_external_location.ext_loc_"),
                                     F.col("sanitized_ext_loc_name"),
                                     F.lit(".id\n\n\tprincipal = \""),
                                     F.col("principal_display_name"),
                                     F.lit("\"\n\tprivileges = [\"ALL_PRIVILEGES\"]\n}")))
    .select("output"))

tf_output_df = (
    spark.createDataFrame([(TF_TEMPLATE_INIT, )], "output string")
    .union(ext_loc_datasources_df)
    .union(grant_ext_loc_resources_df)
)

# COMMAND ----------

tf_output_df.display()

# COMMAND ----------

(
    tf_output_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .text(f"{tf_file_dst_volume}/tf_exported_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.tf")
)

# COMMAND ----------

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HCL converter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/users/wagner_silveira/tf/tf_exported_20240612_134850.tf/part-00000-tid-923262340002559518-917c562c-9063-4d43-b2fc-4453ec388c2c-711-1-c000.txt")

# COMMAND ----------

dbutils.fs.head("dbfs:/Volumes/users/wagner_silveira/tf/tf_exported_20240612_134850.tf/part-00000-tid-923262340002559518-917c562c-9063-4d43-b2fc-4453ec388c2c-711-1-c000.txt")
