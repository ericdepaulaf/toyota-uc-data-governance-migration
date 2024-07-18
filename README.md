# Toyota UC Data Governance Migration

## Section 1: Code Walkthrough

### Order-Dependent Notebooks

#### aws_secrets_update

**Description:**
This notebook is responsible for creating or updating Databricks Secrets with the provided secrets from AWS.

**Functionality:**
- The script provides functions to create and update secrets in AWS Secrets Manager.
- The `manage_secret` function attempts to create a secret first, and if it fails, it tries to update the secret.

**Error Handling:**
- Basic error handling in the `create_secret` and `update_secret` functions to log errors.

**Usage Example:**
- Demonstrates how to use the `manage_secret` function to store a sample secret.

**Code:**
```python
import json
import boto3
from botocore.exceptions import ClientError

def create_secret(secret_name, secret_value, region_name='us-east-1'):
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        response = client.create_secret(
            Name=secret_name,
            SecretString=secret_value
        )
        return response
    except ClientError as e:
        print(f"Error creating secret: {e}")
        return None

def update_secret(secret_name, secret_value, region_name='us-east-1'):
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        response = client.update_secret(
            SecretId=secret_name,
            SecretString=secret_value
        )
        return response
    except ClientError as e:
        print(f"Error updating secret: {e}")
        return None

def manage_secret(secret_name, secret_value, region_name='us-east-1'):
    response = create_secret(secret_name, secret_value, region_name)
    if response is None:
        response = update_secret(secret_name, secret_value, region_name)
    return response

secret_name = "uc-metastore-admin-credential"
secret_value = json.dumps({"username": "admin", "password": "your_password"})

response = manage_secret(secret_name, secret_value)
if response:
    print(f"Secret {secret_name} has been successfully managed. Response: {response}")
else:
    print(f"Failed to manage secret {secret_name}.")
```

**Potential Improvements:**
- Enhance error handling by differentiating between types of `ClientError`.
- Add input validation for `secret_name` and `secret_value`.
- Make the `region_name` configurable through parameters or environment variables.

#### governance_exporter

**Description:**
This notebook crawls all interactive and job clusters and exports the permissions granted to each principal (service principal, usergroup ou user). The permissions are then joined to the S3 paths that the principal's instance profile has access to.

**Functionality:**
- Exports permissions granted to each instance profile and joins them with the S3 paths.
- Uses Databricks widgets to get user inputs like destination table name, AWS secret scope, principal type, and naming convention filter.

**Library Installation:**
- Installs the `databricks-sdk` library and restarts Python to ensure the installation is recognized.

**Widgets:**
- Widgets are used to capture input parameters, making the notebook interactive.

**Logging:**
- Configures detailed logging to aid in debugging and monitoring.

**Data Processing:**
- Uses PySpark to manipulate and transform data.
- Joins permissions data with S3 paths data and writes the result to the specified destination table.

**Code:**
```python
import logging
import pathlib
import sys
import re
import datetime

from pyspark.sql import functions as F

path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path = "/Workspace" + path if not path.startswith("/Workspace") else path

modules_path = pathlib.Path(path).joinpath("../../..").resolve()
sys.path.append(str(modules_path))

from src.principal_permissions import PrincipalPermissions, PrincipalType
from src.crawl_s3_paths import CrawlS3Paths

dbutils.widgets.text("dest_table", "users.wagner_silveira.hms_governance_update", "Destination Table Name")
dbutils.widgets.text("aws_secrets_scope", "hms_exporter_aws_secrets", "AWS Secret Scope")
dbutils.widgets.dropdown("principal_type", "ALL", ["ALL", "SERVICE_PRINCIPAL", "USER", "GROUP"], "Principal Type")
dbutils.widgets.text("naming_convention_filter", "", "Naming Convention Filter")

dest_table = dbutils.widgets.get("dest_table").strip()
aws_secrets_scope = dbutils.widgets.get("aws_secrets_scope").strip()
principal_type = dbutils.widgets.get("principal_type").strip()
naming_convention_filter = dbutils.widgets.get("naming_convention_filter").strip()

if not re.match(r"^[a-zA-Z0-9_.-]+$", dest_table):
    raise ValueError("Invalid destination table name. Please use the format: catalog.database.table_name")

start_time = datetime.datetime.now()

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

principal = PrincipalType.ALL

if principal_type == "SERVICE_PRINCIPAL":
    principal = PrincipalType.SERVICE_PRINCIPAL
elif principal_type == "USER":
    principal = PrincipalType.USER
elif principal_type == "GROUP":
    principal = PrincipalType.GROUP

p = PrincipalPermissions()
permissions_df = p.get_principal_permissions(principal, naming_convention_filter)

permissions_df = permissions_df.withColumn("email_value", F.col("emails").getItem(0).getItem("value"))
display(permissions_df)

permissions_df = permissions_df.withColumn("instance_profile_arn", F.explode(F.col("instance_profile_arn")))
instance_profiles_rows = permissions_df.select("instance_profile_arn").na.drop().distinct().collect()
instance_profiles = [row.instance_profile_arn for row in instance_profiles_rows]

s3 = CrawlS3Paths()(instance_profiles, aws_secrets_scope)
hms_governance_df = permissions_df.join(s3, ["instance_profile_arn"], how="left")

hms_governance_df.display()

hms_governance_df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(dest_table)

display(spark.sql(f"select * from {dest_table} "))

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HMS governance exporter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
```

**Potential Improvements:**
- Add more error handling, especially for data retrieval and joining operations.
- Modularize the code by moving functions to separate modules to improve readability and maintainability.

#### hcl_converter

**Description:**
This notebook converts exported HMS governance privileges to HCL. The generated HCL code assigns these privileges to the principals mapped in the notebook governance_exporter to the External Locations that represent the mapped S3 paths, being able to create the External Locations if they do not already exist.


**Functionality:**
- Converts HMS governance privileges to HCL for external locations.
- Creates new external locations if they do not exist.

**Library Installation:**
- Installs the `databricks-sdk` library and restarts Python to ensure the installation is recognized.

**Widgets:**
- Uses widgets to capture input parameters, making the notebook interactive.

**Logging:**
- Configures detailed logging for debugging and monitoring.

**Data Processing:**
- Uses PySpark to sanitize and transform data.
- Generates HCL resources for external locations and grants.

**Code:**
```python
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
sys.path.append(str(modules_path))

from src.crawl_s3_paths import CrawlS3Buckets
from utils.consts import *

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

if not re.match(r"^[a-zA-Z0-9_.-]+$", src_table):
    raise ValueError("Invalid source table name. Please use the format: catalog.database.table_name")

start_time = datetime.datetime.now()

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",


    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

client = WorkspaceClient()

hms_governance_df = spark.table(src_table)

sanitize_col = F.udf(lambda x: re.sub(r'\W+', '_', x))

sanitized_df = hms_governance_df.withColumn("sanitized_principal_display_name", sanitize_col(F.col("principal_display_name")))
sanitized_df = sanitized_df.withColumn("sanitized_ext_loc_name", sanitize_col(F.col("external_location_name")))

TF_TEMPLATE_INIT = """
terraform {
  required_version = ">= 0.12"
  backend "s3" {}
}

provider "databricks" {
  alias = "databricks"
}
"""

new_ext_loc_datasources_df = (
    sanitized_df
    .filter(F.col("create_ext_loc") == "YES")
    .select(
        F.concat(
            F.lit("resource \"databricks_external_location\" \"ext_loc_"),
            F.col("sanitized_ext_loc_name"),
            F.lit("\" {\\n\\tname = \""),
            F.col("external_location_name"),
            F.lit("\"\\n\\turl = \""),
            F.col("s3_bucket_path"),
            F.lit("\"\\n\\tcomment = \"Generated by HCL Converter\"\\n}")
        ).alias("output")
    )
)

ext_loc_datasources_df = (
    sanitized_df
    .select(
        F.concat(
            F.lit("data \"databricks_external_location\" \"ext_loc_"),
            F.col("sanitized_ext_loc_name"),
            F.lit("\" {\\n\\tname = \""),
            F.col("external_location_name"),
            F.lit("\"\\n}")
        ).alias("output")
    )
    .distinct()
)

grant_ext_loc_resources_df = (
    sanitized_df
    .select(
        "principalType",
        "email_value",
        "principal_display_name",
        "sanitized_principal_display_name",
        "sanitized_ext_loc_name",
        "external_location_name"
    )
    .distinct()
    .withColumn("output", F.concat(
        F.lit("resource \"databricks_grant\" \"principal_"),
        F.col("sanitized_principal_display_name"),
        F.lit("_"),
        F.col("sanitized_ext_loc_name"),
        F.lit("\" {\\n\\texternal_location = data.databricks_external_location.ext_loc_"),
        F.col("sanitized_ext_loc_name"),
        F.lit(".id\\n\\n\\tprincipal = \""),
        F.when(
            (F.col("email_value").isNotNull()) & (F.col("principalType") == "USER"),
            F.col("email_value")
        ).when(
            (F.col("applicationId").isNotNull()) & (F.col("principalType") == "SERVICE_PRINCIPAL"),
            F.col("applicationId")
        ).otherwise(F.col("principal_display_name")),
        F.lit("\"\\n\\tprivileges = [\"ALL_PRIVILEGES\"]\\n}")
    ).alias("output"))
)

if create_new_ext_loc == "YES":
    tf_output_df = (
        spark.createDataFrame([(TF_TEMPLATE_INIT,)], "output string")
        .union(new_ext_loc_datasources_df)
        .union(ext_loc_datasources_df)
        .union(grant_ext_loc_resources_df)
    )
else:
    tf_output_df = (
        spark.createDataFrame([(TF_TEMPLATE_INIT,)], "output string")
        .union(ext_loc_datasources_df)
        .union(grant_ext_loc_resources_df)
    )

tf_output_df.display()

tf_output_df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(dest_table)

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HCL converter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
```

**Potential Improvements:**
- Enhance error handling, especially for data transformations and writing operations.
- Modularize the code by moving functions to separate modules to improve readability and maintainability.

#### hcl_converter_volumes

**Description:**
This notebook converts create Volumes commands to HCL.The generated HCL code creates Volumes  for External Locations, serving all second-level directories of each External Location.

**Functionality:**
- Creates volumes for external locations based on second-level directories.
- Generates HCL resources for these volumes.

**Library Installation:**
- Installs the `databricks-sdk` library and restarts Python to ensure the installation is recognized.

**Widgets:**
- Uses widgets to capture input parameters, making the notebook interactive.

**Logging:**
- Configures detailed logging for debugging and monitoring.

**Data Processing:**
- Uses PySpark to sanitize and transform data.
- Generates HCL resources for volumes and writes them to a Terraform file.

**Code:**
```python
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
sys.path.append(str(modules_path))

from src.crawl_s3_paths import CrawlS3Buckets
from utils.consts import *

dbutils.widgets.text("tf_file_dst_volume", "/Volumes/users/wagner_silveira/tf", "Destination Volume for the TF file")
dbutils.widgets.text("aws_secrets_scope", "hms_exporter_aws_secrets", "AWS Secret Scope")
dbutils.widgets.text("default_catalog_name", "users", "Default Catalog Name")
dbutils.widgets.text("default_schema_name", "wagner_silveira", "Default Schema Name")

tf_file_dst_volume = dbutils.widgets.get("tf_file_dst_volume")
aws_secrets_scope = dbutils.widgets.get("aws_secrets_scope")
default_catalog_name = dbutils.widgets.get("default_catalog_name")
default_schema_name = dbutils.widgets.get("default_schema_name")

if not re.match(r"^[a-zA-Z0-9_.-]+$", default_catalog_name):
    raise ValueError("Invalid catalog name. Please use a valid format.")
if not re.match(r"^[a-zA-Z0-9_.-]+$", default_schema_name):
    raise ValueError("Invalid schema name. Please use a valid format.")

start_time = datetime.datetime.now()

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

client = WorkspaceClient()

s3_buckets_df = CrawlS3Buckets()(aws_secrets_scope)

sanitize_col = F.udf(lambda x: re.sub(r'\W+', '_', x))
s3_buckets_df = s3_buckets_df.withColumn("sanitized_ext_loc_name", sanitize_col(F.col("external_location_name")))
s3_buckets_df = s3_buckets_df.withColumn("sanitized_volume", sanitize_col(F.col("volume")))

TF_TEMPLATE_INIT = """
terraform {
  required_version = ">= 0.12"
  backend "s3" {}
}

provider "databricks" {
  alias = "databricks"
}
"""

volumes_resources_df = (
    s3_buckets_df
    .select(
        F.concat(
            F.lit("resource \"databricks_volume\" \"volume_"),
            F.col("sanitized_ext_loc_name"),
            F.lit("_"),
            F.col("sanitized_volume"),
            F.lit("\" {\\n\tname = \""),
            F.col("sanitized_volume"),
            F.lit("\"\\n"),
            F.lit("\tcatalog_name = \""),
            F.col("catalog_name"),
            F.lit("\"\\n"),
            F.lit("\tschema_name = \""),
            F.col("schema_name"),
            F.lit("\"\\n"),
            F.lit("\tvolume_type = \"EXTERNAL\"\\n"),
            F.lit("\tstorage_location = \""),
            F.col("s3_bucket_url"),
            F.lit("/"),
            F.col("volume"),
            F.lit("\"\\n"),
            F.lit("\tcomment = \"Volume created by terraform\""),
            F.lit("\\n}"),
            F.lit("\\n")
        ).alias("output")
    )
)

tf_output_df = (
    spark.createDataFrame([(TF_TEMPLATE_INIT,)], "output string")
    .union(volumes_resources_df)
)

tf_output_df.display()

tf_output_df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(dest_table)

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HCL converter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
```

**Potential Improvements:**
- Enhance error handling, especially for data transformations and writing operations.
- Modularize the code by moving functions to separate modules to improve readability and maintainability.

### Order-Independent Notebooks

#### hms_to_uc_priv_mappings

**Description:**
This notebook converts the exported UC governance privileges (for Unity Catalog Tables) to HCL. The generated HCL code assigns these privileges to the principals to UC tables mapped to a table provided with hms permissions.

**Functionality:**
- Maps HMS privileges to UC privileges.
- Generates HCL resources for catalogs, schemas, and tables based on the mapped privileges.

**Widgets:**
- Uses widgets to capture input parameters, making the notebook interactive.

**Logging:**
- Configures detailed logging for debugging and monitoring.

**Data Processing:**
- Uses

 PySpark to transform and map privileges.
- Generates HCL resources and writes them to a Terraform file.

**Code:**
```python
import re
import logging
import datetime

from pyspark.sql import functions as F, Column

dbutils.widgets.text("src_table", "wsilveira.wsilveira.tbdp_prod_grants_phase_1", "HMS privileges table")
dbutils.widgets.text("tf_file_dst_volume", "/Volumes/users/wagner_silveira/tf", "Destination Volume for the TF file")

hms_privileges_tbl = dbutils.widgets.get("src_table")
tf_file_dst_volume = dbutils.widgets.get("tf_file_dst_volume")

pattern = re.compile(r"^[a-zA-Z0-9_-`]+\.[a-zA-Z0-9_-`]+\.[a-zA-Z0-9_-`]+$")
if not pattern.match(hms_privileges_tbl):
    raise ValueError("Invalid source table name. Please use the format: catalog.database.table_name")

HMS_UC_PRIVILEGES_MAPPING = {
    "ALL PRIVILEGES": ["ALL_PRIVILEGES"],
    "READ_METADATA": ["APPLY_TAG", "BROWSE"],
    "USAGE" : ["USE_CATALOG", "USE_SCHEMA"],
    "SELECT": ["SELECT"],
    "CREATE": ["CREATE_TABLE", "CREATE_SCHEMA"],
    "MODIFY": ["MODIFY"],
    "CREATE_NAMED_FUNCTION": ["CREATE_FUNCTION"]
}

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

start_time = datetime.datetime.now()

df = spark.table(hms_privileges_tbl)

when_cond = None
for hms_priv, uc_privs in HMS_UC_PRIVILEGES_MAPPING.items():
    condition = F.when(F.col("privilege") == hms_priv, F.array(*[F.lit(uc_priv) for uc_priv in uc_privs]))
    when_cond = condition if when_cond is None else when_cond.otherwise(condition)

if when_cond is not None:
    df = df.withColumn("uc_privileges", when_cond)
else:
    raise ValueError("No valid HMS to UC privilege mappings found.")

df = df.withColumn("grants", F.expr("TRANSFORM(uc_privileges, x -> CONCAT('  ', x))"))

df = df.withColumn("grants", F.concat_ws(",\n", F.col("grants")))

df = df.withColumn("hms_object", F.concat_ws(".", F.col("catalog_name"), F.col("schema_name"), F.col("table_name")))
df = df.withColumn("hms_object_sanitized", F.regexp_replace(F.col("hms_object"), r"[^a-zA-Z0-9]", "_"))

TF_TEMPLATE_INIT = """
terraform {
  required_version = ">= 0.12"
  backend "s3" {}
}

provider "databricks" {
  alias = "databricks"
}
"""

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
     .select("output")
)

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
     .select("output")
)

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
     .select("output")
)

tf_output_df = (
    spark.createDataFrame([(TF_TEMPLATE_INIT,)], "output string")
    .union(databricks_catalogs_df)
    .union(databricks_schemas_df)
    .union(databricks_tables_df)
)

tf_output_df.display()

tf_output_df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(dest_table)

finish_time = datetime.datetime.now()
dbutils.notebook.exit(f"HCL converter successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
```

**Potential Improvements:**
- Enhance error handling, especially for data transformations and writing operations.
- Modularize the code by moving functions to separate modules to improve readability and maintainability.

#### add_libs_to_uc_allowlist

**Description:**
This notebook obtains the libs (jar and maven) installed in the workspace clusters and adds them to the unity catalog allowlist.

**Functionality:**
- Obtains libraries (jar and maven) installed in the workspace clusters and adds them to the Unity Catalog allowlist.

**Widgets:**
- Uses widgets to capture input parameters, making the notebook interactive.

**Utility Functions:**
- Functions to get clusters, libraries for clusters, job clusters, and extract library information.

**Allowlist Update Functions:**
- Functions to add jars and maven libraries to the Unity Catalog allowlist.

**Code Execution:**
- Initializes the Databricks SDK client and executes the main logic to gather libraries and update the allowlist.

**Code:**
```python
import requests
import json
from databricks.sdk import WorkspaceClient

dbutils.widgets.dropdown("LIB_TYPE", "jar",
                         ["jar", "maven", "jar/maven"], "LIB_TYPE")

lib_type = dbutils.widgets.get("LIB_TYPE")

def get_clusters():
    return w.clusters.list()

def get_cluster_libraries(cluster_id):
    return w.libraries.cluster_status(cluster_id=cluster_id)

def get_job_clusters():
    jobs = w.jobs.list()
    job_clusters = []
    for job in jobs:
        job_details = w.jobs.get(job_id=job.job_id)
        if hasattr(job_details, 'settings') and hasattr(job_details.settings, 'job_clusters'):
            job_clusters.extend(job_details.settings.job_clusters)
    return job_clusters

def extract_library_info(library):
    if hasattr(library, 'jar') and library.jar is not None:
        return {'name': library.jar, 'type': 'jar'}
    elif hasattr(library, 'maven') and hasattr(library.maven, 'coordinates') and library.maven is not None:
        return {'name': f"{library.maven.coordinates}", 'type': 'maven'}
    else:
        return None

def add_jars_to_unity_catalog_allowlist(libraries):
    url = f"https://{w.config.host}/api/2.0/unity-catalog/allowlists/jars"
    headers = {
        "Authorization": f"Bearer {w.config.token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        current_allowlist = response.json()
    else:
        print(f"Error fetching current allowlist: {response.text}")
        return

    for lib in libraries:
        if lib['type'] == 'jar' and lib['name'] not in current_allowlist.get('libraries', []):
            current_allowlist.setdefault('libraries', []).append(lib['name'])

    response = requests.put(url, headers=headers, json=current_allowlist)
    if response.status_code == 200:
        print("Successfully updated Unity Catalog allowlist")
    else:
        print(f"Error updating allowlist: {response.text}")

def add_maven_to_unity_catalog_allowlist(libraries):
    url = f"https://{w.config.host}/api/2.0/unity-catalog/allowlists/maven"
    headers = {
        "Authorization": f"Bearer {w.config.token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        current_allowlist = response.json()
    else:
        print(f"Error fetching current allowlist: {response.text}")
        return

    for lib in libraries:
        if lib['type']  == 'maven':
            artifact = {
                "artifact":

 lib['name'],
                "match_type": 'PREFIX_MATCH'
            }
            if artifact not in current_allowlist.get('artifact_matchers', []):
                current_allowlist.setdefault('artifact_matchers', []).append(artifact)

    response = requests.put(url, headers=headers, json=current_allowlist)
    if response.status_code == 200:
        print("Successfully updated maven Unity Catalog allowlist")
    else:
        print(f"Error updating maven allowlist: {response.text}")

w = WorkspaceClient()

all_libraries = set()

clusters = get_clusters()
for cluster in clusters:
    library_statuses = get_cluster_libraries(cluster.cluster_id)
    for lib_status in library_statuses:
        lib_info = extract_library_info(lib_status.library)
        if lib_info:
            all_libraries.add(json.dumps(lib_info))

job_clusters = get_job_clusters()
for job_cluster in job_clusters:
    if hasattr(job_cluster, 'libraries'):
        for lib in job_cluster.libraries:
            lib_info = extract_library_info(lib)
            if lib_info:
                all_libraries.add(json.dumps(lib_info))

all_libraries = [json.loads(lib) for lib in all_libraries]

print(all_libraries)

if lib_type == 'jar':
  add_jars_to_unity_catalog_allowlist(all_libraries)
elif lib_type == 'maven': 
  add_maven_to_unity_catalog_allowlist(all_libraries)
else:
  add_jars_to_unity_catalog_allowlist(all_libraries)
  add_maven_to_unity_catalog_allowlist(all_libraries)
```

**Potential Improvements:**
- Enhance error handling, especially for network requests and JSON operations.
- Modularize the code by moving functions to separate modules to improve readability and maintainability.

## Section 2: Playbook

### Prerequisites
1. Ensure you have the necessary permissions to run the notebooks on Databricks.
2. Install the `databricks-sdk` library if not already installed.

### Steps to Configure and Run Notebooks

1. **aws_secrets_update.py**
   - Open the notebook in Databricks.
   - Modify the `secret_name` and `secret_value` variables as needed.
   - Run all cells to create or update the secret in AWS Secrets Manager.

2. **governance_exporter.py**
   - Open the notebook in Databricks.
   - Set the widgets: `dest_table`, `aws_secrets_scope`, `principal_type`, `naming_convention_filter`.
   - Run all cells to export permissions and join them with S3 paths.

3. **hcl_converter.py**
   - Open the notebook in Databricks.
   - Set the widgets: `src_table`, `tf_file_dst_volume`, `aws_secrets_scope`, `create_new_ext_loc`, `credential_name`.
   - Run all cells to convert HMS governance privileges to HCL.

4. **hcl_converter_volumes.py**
   - Open the notebook in Databricks.
   - Set the widgets: `tf_file_dst_volume`, `aws_secrets_scope`, `default_catalog_name`, `default_schema_name`.
   - Run all cells to create volumes and generate HCL resources.

5. **hms_to_uc_priv_mappings.py**
   - Open the notebook in Databricks.
   - Set the widgets: `src_table`, `tf_file_dst_volume`.
   - Run all cells to map HMS privileges to UC privileges and generate HCL resources.

6. **add_libs_to_uc_allowlist.py**
   - Open the notebook in Databricks.
   - Set the widget: `LIB_TYPE`.
   - Run all cells to add libraries to the Unity Catalog allowlist.

### Notes
- Ensure the Databricks SDK client is initialized correctly.
- Follow the notebook instructions for any specific configurations or parameters.


Responsables:
- [Eric de Paula Ferreira](eric.ferreira@databricks.com)
- [Wagner Silveira](wagner.silveira@databricks.com)

