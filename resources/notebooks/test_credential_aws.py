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
