# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Toyota Mortors North America - Store AWS secrets into Databricks Secrets
# MAGIC
# MAGIC ##### Description:
# MAGIC - This notebook will create or update Databricks Secrets \
# MAGIC   with the provided secrets from AWS.
# MAGIC

# COMMAND ----------
%pip install databricks-cli --upgrade

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os

# COMMAND ----------
dbutils.widgets.text("aws_access_key_id", "", "AWS Access Key ID")
dbutils.widgets.text("aws_secret_access_key", "", "AWS Secret Access Key")
dbutils.widgets.text("aws_session_token", "", "AWS Session Token")
dbutils.widgets.text("databricks_secret_scope", "hms_exporter_aws_secrets", "Databricks Secret Scope")

aws_secrets = {
    "aws_access_key_id": dbutils.widgets.get("aws_access_key_id").strip(),
    "aws_secret_access_key": dbutils.widgets.get("aws_secret_access_key").strip(),
    "aws_session_token": dbutils.widgets.get("aws_session_token").strip()

}
databricks_secret_scope = dbutils.widgets.get("databricks_secret_scope").strip()

# COMMAND ----------
os.environ["DATABRICKS_HOST"] = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------
ret = os.system(f"databricks secrets create-scope --scope \"{databricks_secret_scope}\"")

if ret == 0:
    print(f"Secret scope \"{databricks_secret_scope}\" created successfully.")
elif "RESOURCE_ALREADY_EXISTS" in ret:
    print(f"Secret scope \"{databricks_secret_scope}\" already exists.")
else:
    print(f"Error creating secret scope \"{databricks_secret_scope}\". Error: {ret}")
    raise Exception(f"Error creating secret scope \"{databricks_secret_scope}\". Error: {ret}")

# COMMAND ----------
for k, v in aws_secrets.items():
    ret = os.system(f"databricks secrets put --scope \"{databricks_secret_scope}\" --key \"{k}\" --string-value \"{v}\"")

    if ret == 0:
        print(f"Secret \"{k}\" created or updated successfully.")
    else:
        print(f"Error creating secret \"{k}\". Error: {ret}")
        raise Exception(f"Error creating secret \"{k}\". Error: {ret}")