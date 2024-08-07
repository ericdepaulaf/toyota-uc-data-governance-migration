# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Toyota Mortors North America - HMS Data Governance Exporter
# MAGIC
# MAGIC ##### Description:
# MAGIC - This notebook will crawl all interactive and job clusters \
# MAGIC   and export the permissions granted to each instance profile.
# MAGIC - The permissions are then joined with the S3 paths \
# MAGIC   that the instance profile has access to.
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

import logging
import pathlib
import sys
import re
import datetime

from pyspark.sql import functions as F

path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path = "/Workspace" + path if not path.startswith("/Workspace") else path

modules_path = pathlib.Path(path).joinpath("../../..").resolve()

# Allow for execution outside of Databricks Repos directory
sys.path.append(str(modules_path))

# from src.cluster_permissions import ClusterPermissions
from src.principal_permissions import PrincipalPermissions, PrincipalType
from src.crawl_s3_paths import CrawlS3Paths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("dest_table", "users.wagner_silveira.hms_governance_update", "Destination Table Name")
dbutils.widgets.text("aws_secrets_scope", "hms_exporter_aws_secrets", "AWS Secret Scope")
dbutils.widgets.dropdown("principal_type", "ALL", ["ALL", "SERVICE_PRINCIPAL", "USER", "GROUP"], "Principal Type")
dbutils.widgets.text("naming_convention_filter", "", "Naming Convention Filter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set variables

# COMMAND ----------

dest_table = dbutils.widgets.get("dest_table").strip()
aws_secrets_scope = dbutils.widgets.get("aws_secrets_scope").strip()
principal_type = dbutils.widgets.get("principal_type")
naming_convention_filter = dbutils.widgets.get("naming_convention_filter")

pattern = re.compile(r"^[a-zA-Z0-9_-]+.[a-zA-Z0-9_-]+.[a-zA-Z0-9_-]+$")
if not pattern.match(dest_table):
    raise ValueError("Invalid destination table name. Please use the format: catalog.database.table_name")

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

principal = PrincipalType.ALL

if principal_type == "SERVICE_PRINCIPAL":
    principal = PrincipalType.SERVICE_PRINCIPAL
elif principal_type == "USER":
    principal = PrincipalType.USER
elif principal_type == "GROUP":
    principal = PrincipalType.GROUP

p = PrincipalPermissions()
permissions_df = p.get_principal_permissions(principal, naming_convention_filter)

# COMMAND ----------

permissions_df = permissions_df.withColumn("email_value", F.col("emails").getItem(0).getItem("value"))
display(permissions_df)

# COMMAND ----------

permissions_df = permissions_df.withColumn("instance_profile_arn", F.explode(F.col("instance_profile_arn")))
instance_profiles_rows = permissions_df.select("instance_profile_arn").na.drop().distinct().collect()
instance_profiles = [row.instance_profile_arn for row in instance_profiles_rows]

s3 = CrawlS3Paths()(instance_profiles, aws_secrets_scope)
hms_governance_df = permissions_df.join(s3, ["instance_profile_arn"], how="left")

# COMMAND ----------

hms_governance_df.display()

# COMMAND ----------

hms_governance_df.count()

# COMMAND ----------

hms_governance_df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(dest_table)

# COMMAND ----------

display(spark.sql(f"select * from {dest_table} "))

# COMMAND ----------

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HMS governance exporter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
