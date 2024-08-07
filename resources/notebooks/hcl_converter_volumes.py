# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Toyota Mortors North America - Create Volumes exported to HCL for External Locations
# MAGIC
# MAGIC ##### Description:
# MAGIC - This notebook will create Volumes exported to HCL for External Locations. The Volumes created serve all second-level directories of each External Location.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installing libraries

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.25.1 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

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

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("tf_file_dst_volume", "/Volumes/users/wagner_silveira/tf", "Destination Volume for the TF file")
dbutils.widgets.text("aws_secrets_scope", "hms_exporter_aws_secrets", "AWS Secret Scope")
dbutils.widgets.text("default_catalog_name", "users", "Default Catalog Name")
dbutils.widgets.text("default_schema_name", "wagner_silveira", "Default Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set variables

# COMMAND ----------

tf_file_dst_volume = dbutils.widgets.get("tf_file_dst_volume")
aws_secrets_scope = dbutils.widgets.get("aws_secrets_scope")
default_catalog_name = dbutils.widgets.get("default_catalog_name")
default_schema_name = dbutils.widgets.get("default_schema_name")

tf_file_dst_volume = tf_file_dst_volume.rstrip("/")
pattern = re.compile(r"^/Volumes/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$")
if not pattern.match(tf_file_dst_volume):
    raise ValueError("Invalid volume path. Please use the format: /Volumes/catalog_name/schema_name/volume_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Dataframe that maps s3 bucket to catalog/schema for creating Volumes. If the s3 bucket does not have a catalog/schema mapped in this Dataframe, the Volume will be created in the default catalog/schema informed in the widgets

# COMMAND ----------

schema = T.StructType([T.StructField("s3_bucket_url", T.StringType(), True),
                     T.StructField("catalog_name", T.StringType(), True),
                     T.StructField("schema_name", T.StringType(), True),
                    ])

                    

data = [
        ("s3://databricks-dkushari",
        "catalog1", 
        "schema1"),
        ("s3://one-env-uc-external-location",
        "catalog2", 
        "schema2"),
        ("s3://oetrta",
        "catalog3", 
        "schema3"),
    ]

df_check_volumes = spark.createDataFrame(data, schema)
display(df_check_volumes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code execution

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
    .withColumn("s3_bucket_url", F.regexp_extract(F.col("url"), r'(s3a?://[^/]+)/?', 1))
    .withColumn("s3_bucket_name", F.regexp_extract(F.col("url"), r"^s3://(.*?)/", 1))
)

logger.info(f"{ext_loc_df.count()} External Locations fetched successfully.")

# COMMAND ----------

ext_loc_df.display()

# COMMAND ----------

buckets_df = (ext_loc_df.select("s3_bucket_name").distinct())
buckets_list = [row['s3_bucket_name'] for row in buckets_df.collect()]

s3 = CrawlS3Buckets()
volumes_df = s3.get_sub_folders_s3_buckets(aws_secrets_scope, buckets_list)

display(volumes_df)

# COMMAND ----------

joined_df = (ext_loc_df.join(volumes_df, ["s3_bucket_name"], "inner")
                       .withColumn("sanitized_ext_loc_name", F.lower(F.regexp_replace(F.col("ext_loc_name"), "[^a-zA-Z0-9_]", "_")))
                       .withColumn("volume", F.explode("folders"))
                       .withColumn("sanitized_volume", F.lower(F.regexp_replace(F.col("volume"), "[^a-zA-Z0-9_]", "_")))
                       .withColumn("sanitized_volume", F.col("sanitized_volume").substr(F.lit(0), F.length(F.col("sanitized_volume")) - 1))
                       .join(df_check_volumes, ["s3_bucket_url"], "left")
                       .withColumn("catalog_name", F.when(F.col("catalog_name").isNull(), F.lit(default_catalog_name)).otherwise(F.col("catalog_name")))
                       .withColumn("schema_name", F.when(F.col("schema_name").isNull(), F.lit(default_schema_name)).otherwise(F.col("schema_name")))
                       .select("sanitized_ext_loc_name", "sanitized_volume", "ext_loc_name", "volume", "catalog_name", "schema_name", "s3_bucket_url")
                       .filter("volume is not null")
                       .dropDuplicates()
            )
display(joined_df)

# COMMAND ----------

logger.info("Generating HCL...")

volumes_resources_df = (
    joined_df.select("sanitized_ext_loc_name", "sanitized_volume", "volume", "catalog_name", "s3_bucket_url", "schema_name")
    .distinct()
    .withColumn("output", F.concat(F.lit("resource \"databricks_volume\" \"volume_"),
                                     F.col("sanitized_ext_loc_name"),
                                     F.lit("_"),
                                     F.col("sanitized_volume"),
                                     F.lit("\" {\n\tname = \""),
                                     F.col("sanitized_volume"),
                                     F.lit("\"\n"),
                                     F.lit("\tcatalog_name = \""),
                                     F.col("catalog_name"),
                                     F.lit("\"\n"),
                                     F.lit("\tschema_name = \""),
                                     F.col("schema_name"),
                                     F.lit("\"\n"),
                                     F.lit("\tvolume_type = \"EXTERNAL\"\n"),
                                     F.lit("\tstorage_location = \""),
                                     F.col("s3_bucket_url"),
                                     F.lit("/"),
                                     F.col("volume"),
                                     F.lit("\"\n"),
                                     F.lit("\tcomment = \"Volume created by terraform\""),
                                     F.lit("\n}"),
                                     F.lit("\n")
                                    ))
    .select("output"))


tf_output_df = (
    spark.createDataFrame([(TF_TEMPLATE_INIT, )], "output string")
    .union(volumes_resources_df)
)

# COMMAND ----------

tf_output_df.display()

# COMMAND ----------

(
    tf_output_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .text(f"{tf_file_dst_volume}/tf_exported_volumes{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.tf")
)

# COMMAND ----------

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HCL converter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
