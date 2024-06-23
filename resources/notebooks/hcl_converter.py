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

path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path = "/Workspace" + path if not path.startswith("/Workspace") else path

modules_path = pathlib.Path(path).joinpath("../../..").resolve()

# Allow for execution outside of Databricks Repos directory
sys.path.append(str(modules_path))

from src.crawl_s3_paths import CrawlS3Buckets
from utils.consts import *

# COMMAND ----------

start_time = datetime.datetime.now()

# COMMAND ----------

dbutils.widgets.text("src_table", "users.wagner_silveira.hms_governance_update_2", "Source Table Name")
dbutils.widgets.text("tf_file_dst_volume", "/Volumes/users/wagner_silveira/tf", "Destination Volume for the TF file")
dbutils.widgets.text("aws_secrets_scope", "hms_exporter_aws_secrets", "AWS Secret Scope")
dbutils.widgets.dropdown("create_new_ext_loc", "NO", ["YES", "NO"], "Create New Ext. Locations")
dbutils.widgets.text("credential_name", "field_demos_credential", "Credential Name")

src_table = dbutils.widgets.get("src_table")
tf_file_dst_volume = dbutils.widgets.get("tf_file_dst_volume")
aws_secrets_scope = dbutils.widgets.get("aws_secrets_scope")
create_new_ext_loc = dbutils.widgets.get("create_new_ext_loc")
credential_name = dbutils.widgets.get("credential_name")

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
    .withColumn("created", F.lit(True))
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


if create_new_ext_loc == "YES":

  logger.info("Generete new external locations")

  access_granted_df = (hms_governance_df.crossJoin(ext_loc_df)
                                        .withColumn("access_granted", F.regexp_like(F.col("url"), F.col("s3_paths_regex")))
                                        .filter(F.col("access_granted") == F.lit(True))
                                        .select("s3_paths_regex")
                                        .distinct())

  s3_bucket_regex_df = (hms_governance_df.crossJoin(ext_loc_df)
                                        .withColumn("access_granted", F.regexp_like(F.col("url"), F.col("s3_paths_regex")))
                                        .filter(F.col("access_granted") == F.lit(False))
                                        .join(access_granted_df, ["s3_paths_regex"],"leftanti")
                                        .select("s3_paths_regex")
                                        .distinct()
                                        .withColumn("s3_bucket_regex", F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("s3_paths_regex"), "s3://", ""), "/", ""), r"\.\*\.\*", r"\.\*"))
                                        .select("s3_bucket_regex")
                                        .distinct())

  s3 = CrawlS3Buckets()
  bucket_df = s3.get_s3_buckets(aws_secrets_scope)

  regex_pattern = "|".join([row['s3_bucket_regex'] for row in s3_bucket_regex_df.collect()])

  print(regex_pattern)

  matched_df = bucket_df.withColumn("matched_bucket", F.expr(f"regexp_extract(bucket_name, '{regex_pattern}', 0)"))

  matched_df = (matched_df.filter(F.col("matched_bucket") != "")
                          .select("matched_bucket")
                          .distinct())

  new_ext_loc_df = (matched_df.withColumnRenamed("matched_bucket", "name")
                              .withColumn("credential_name", F.lit(credential_name))
                              .withColumn("credential_id", F.lit(""))
                              .withColumn("url", F.concat(F.lit("s3://"), F.col("name")))
                              .withColumn("created", F.lit(False)))

  ext_loc_df = (ext_loc_df.union(new_ext_loc_df))

else:
  logger.info("Only registered external locations")

# COMMAND ----------


ext_loc_df.display()

# COMMAND ----------

logger.info("Joining External Locations with HMS Governance data...")

joined_df = (
    hms_governance_df.crossJoin(ext_loc_df)
    .withColumn("access_granted", F.regexp_like(F.col("url"), F.col("s3_paths_regex")))
    .filter(F.col("access_granted") == F.lit(True))
    .select("instance_profile_arn", "role_name", "principal_display_name", "principal_id", "ext_loc_name", "credential_id", "credential_name", "url", "email_value", "principalType", "applicationId", "created")
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

if create_new_ext_loc == "YES":
  new_ext_loc_datasources_df = (
    joined_df.select("ext_loc_name", "sanitized_ext_loc_name", "url", "credential_name")
            .distinct()
            .filter("created = false")
            .withColumn("output", F.concat(F.lit("resource \"databricks_external_location\" \"ext_loc_"),
                                            F.lower(F.col("sanitized_ext_loc_name")),
                                            F.lit("\" {\n\tname = \""),
                                            F.col("ext_loc_name"),
                                            F.lit("\""),
                                            F.lit("\n\turl = \""),
                                            F.col("url"),
                                            F.lit("\""),
                                            F.lit("\n\tcredential_name = \""),
                                            F.col("credential_name"),
                                            F.lit("\""),
                                            F.lit("\n\tcomment = \"External location created by terraform\""),
                                            F.lit("\n}"),
                                            F.lit("\n")),
              )
    .select("output"))

ext_loc_datasources_df = (
  joined_df.select("ext_loc_name", "sanitized_ext_loc_name")
           .distinct()
           .filter("created = true")
           .withColumn("output", F.concat(F.lit("data \"databricks_external_location\" \"ext_loc_"),
                                          F.lower(F.col("sanitized_ext_loc_name")),
                                          F.lit("\" {\n\tname = \""),
                                          F.col("ext_loc_name"),
                                          F.lit("\"\n}"),
                                          F.lit("\n")),
             )
  .select("output"))

grant_ext_loc_resources_df = (
    joined_df.select("sanitized_principal_display_name", "sanitized_ext_loc_name", "principal_display_name", "email_value", "applicationId", "principalType")
    .distinct()
    .withColumn("output", F.concat(F.lit("resource \"databricks_grant\" \"principal_"),
                                     F.col("sanitized_principal_display_name"),
                                     F.lit("_"),
                                     F.col("sanitized_ext_loc_name"),
                                     F.lit("\" {\n\texternal_location = data.databricks_external_location.ext_loc_"),
                                     F.col("sanitized_ext_loc_name"),
                                     F.lit(".id\n\n\tprincipal = \""),
                                     F.when((F.col("email_value").isNotNull()) & (F.col("principalType") == "USER"), F.col("email_value"))
                                      .when((F.col("applicationId").isNotNull()) & (F.col("principalType") == "SERVICE_PRINCIPAL"), F.col("applicationId"))
                                      .otherwise(F.col("principal_display_name")),
                                     F.lit("\"\n\tprivileges = [\"ALL_PRIVILEGES\"]\n}"),
                                     F.lit("\n")))
    .select("output"))

if create_new_ext_loc == "YES":
  tf_output_df = (
      spark.createDataFrame([(TF_TEMPLATE_INIT, )], "output string")
      .union(new_ext_loc_datasources_df)
      .union(ext_loc_datasources_df)
      .union(grant_ext_loc_resources_df)
  )
else:
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
    .text(f"{tf_file_dst_volume}/tf_exported_extloc_grants{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.tf")
)

# COMMAND ----------

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HCL converter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
