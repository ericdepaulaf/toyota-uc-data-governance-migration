# Databricks notebook source
import boto3

aws_secrets_scope = "hms_exporter_aws_secrets"

# Create an IAM client
iam_client = boto3.client('sts',
                          aws_access_key_id=dbutils.secrets.get(scope=aws_secrets_scope, key='aws_access_key_id'),
                          aws_secret_access_key=dbutils.secrets.get(scope=aws_secrets_scope, key='aws_secret_access_key'),
                          aws_session_token=dbutils.secrets.get(scope=aws_secrets_scope, key='aws_session_token'))

iam_client.get_caller_identity()


# COMMAND ----------

import boto3

aws_secrets_scope = "hms_exporter_aws_secrets"

# Create an IAM client
iam_client = boto3.client('sts')

iam_client.get_caller_identity()

# COMMAND ----------

import requests

# Define your variables
access_token = 'dapi198473a9b6749a966e9400e90d3f5bfb'
databricks_instance = 'e2-demo-field-eng.cloud.databricks.com'
principal_id = 'Adriana'

# Step 1: Retrieve the User ID
url = f'https://{databricks_instance}/api/2.0/preview/scim/v2/Users?filter=displayName eq "{principal_id}"'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/scim+json'
}

response = requests.get(url, headers=headers)

user_data = response.json()

print(user_data)
user_id = user_data['Resources'][0]['id']

# Step 2: Get User Details
url = f'https://{databricks_instance}/api/2.0/preview/scim/v2/Users/{user_id}'
response = requests.get(url, headers=headers)
user_details = response.json()
user_email = user_details['emails'][0]['value']

print(f'User Email: {user_email}')

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json

# Dynamic config
context_json = dbutils.entry_point.getDbutils().notebook().getContext().toJson()

# Parse the JSON string
context = json.loads(context_json)

# Extract the workspace URL
databricks_workspace_url = f"https://{context['tags']['browserHostName']}"

# Get temporary token
databricks_token = context["extraContext"]["api_token"]

w = WorkspaceClient(host=databricks_workspace_url, token=databricks_token)

for i in w.service_principals.list():
  if i.display_name == "dropbox_test":
    print(i)

# COMMAND ----------

import boto3

# Initialize a session using Amazon S3
s3 = boto3.client('s3')

# List all buckets
response = s3.list_buckets()

# Print bucket names
buckets = [bucket['Name'] for bucket in response['Buckets']]
print("Bucket List: %s" % buckets)

# COMMAND ----------

import re

buckets = ['00-ravi-test', '00sumitsaraswatdemo', '01-pramod', '20230306-mlflow-experiment', '20230607-sg-test', '4sq-databricks', 'aaugustyniak', 'abhi-us-east1-root-storage', 'abhim-us-east1-metastore', 'abhisaxena', 'ademianczuk', 'adp-east', 'adrian-databricks', 'af-databricks-e2-dbfs-root-us-east-1-997819012307-bb6cd5c0', 'ah-datadog-test', 'airizarry-west', 'ajmal-databricks-e2-dbfs-root-us-east-1-997819012307-229c00d0', 'ajmal-fe-demo', 'akangsha-test-bucket', 'akash-demo-bucket', 'akash-demo-mumbai-bucket', 'akash-root-eu-west-1', 'akash-s-test', 'akash-scc-dep-root-bucket', 'akash-sink-singapore-bucket', 'akil-west2', 'akil.thomas-databricks', 'akj-test', 'akshayamin-demo-bucket', 'alexnbucket', 'am-e2-cert-s3-us-east-1-997819012307-60d89df0', 'amir-hls', 'amirskiy-1trc', 'andreatardif', 'anglian-water-asset-demo', 'anindita-e2', 'anindita-test-1', 'anru-databricks-e2-dbfs-root-us-east-1-997819012307-b3bbec60', 'anyarum', 'aotttftest', 'apj-llm-cup-bucket', 'apj-partner-workshop', 'archana-s3-test-storage', 'arduino-awsparis1-rootbucket', 'arduino-databricks-public-s3-paris', 'as-databricks-e2-dbfs-root-us-east-1-997819012307-098687b0', 'asher-dbx', 'ashley-demo-resources', 'aslimane-databricks', 'aslimane-databricks-datasets', 'aso-rootbucket-databricks-2', 'aso-tfstate-bucket', 'aso-uc-metastore', 'aw-demo-pg', 'awie-s3-ec-briandatalake-customuploads-01', 'aws-athena-query-results-997819012307-us-west-2-vtuwjcs3', 'aws-cost-report-997819012307-us-west-2', 'aws-databarbarian', 'aws-emr-resources-997819012307-ap-southeast-2', 'aws-emr-resources-997819012307-us-west-2', 'aws-emr-studio-997819012307-us-east-1', 'aws-emr-studio-997819012307-us-west-2', 'aws-glue-assets-997819012307-ap-northeast-1', 'aws-glue-assets-997819012307-us-east-1', 'aws-glue-assets-997819012307-us-east-2', 'aws-glue-assets-997819012307-us-west-2', 'aws-glue-scripts-997819012307-ap-northeast-1', 'aws-glue-scripts-997819012307-us-east-1', 'aws-glue-scripts-997819012307-us-east-2', 'aws-hwong-demo', 'aws-logs-997819012307-ap-southeast-2', 'aws-logs-997819012307-us-east-1', 'aws-logs-997819012307-us-east-2', 'aws-logs-997819012307-us-west-1', 'aws-logs-997819012307-us-west-2', 'aws-sam-cli-managed-default-samclisourcebucket-1141kis9dgid3', 'bb-migration-public', 'bbbdatabricks25march2024']

# Define the regex pattern for the S3 path
pattern = re.compile(r'.*-demo-.*')

# Function to list objects in a bucket and filter by regex
def list_objects_with_regex(bucket_name, pattern):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    
    matched_objects = []
    
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if pattern.search(obj['Key']):
                    matched_objects.append(obj['Key'])
    
    return matched_objects

# Iterate through all buckets and apply the regex filter
for bucket in buckets:
    print(f"Checking bucket: {bucket}")
    matched_objects = list_objects_with_regex(bucket, pattern)
    if matched_objects:
        print(f"Matched objects in {bucket}:")
        for obj in matched_objects:
            print(obj)

# COMMAND ----------


import boto3
from pyspark.sql import SparkSession, types as T

# Initialize a session using Amazon S3
s3 = boto3.client('s3')

newlist = ["aws-glue-scripts-997819012307-us-east-1", "aws-hwong-demo"]

volumes_list = []

for bucket_name in newlist:
  volumes_dict = {}
  folders = []
  paginator = s3.get_paginator('list_objects_v2')
  pages = paginator.paginate(Bucket=bucket_name, Delimiter='/', Prefix='')
  try:
    for page in pages:
        common_prefixes = page.get('CommonPrefixes', [])
        for prefix in common_prefixes:
            folder_path = bucket_name + "/" + prefix.get('Prefix')
            folders.append(folder_path)
  except:
    print("Error to access s3 bucket : " + bucket_name)

  volumes_dict = {'bucket_name': bucket_name, 'folders': folders}
  volumes_list.append(volumes_dict)

# Create a list of rows from the list of dictionaries
rows = [tuple(dict_item.values()) for dict_item in volumes_list]

# Define the schema for the DataFrame
schema = T.StructType([
    T.StructField("bucket_name", T.StringType(), True),
    T.StructField("folders", T.ArrayType(T.StringType()), True)])

# Create the DataFrame
df = spark.createDataFrame(rows, schema)

display(df)

# COMMAND ----------

bucket_name = "tomi-schumacher-s3"
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Delimiter='/', Prefix='')

pages.


# COMMAND ----------

dbutils.fs.ls("s3://oetrta/maggie/")
