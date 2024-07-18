# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Toyota Mortors North America - Add libs to the unity catalog allowlist
# MAGIC
# MAGIC ##### Description:
# MAGIC - This notebook obtains the libs (jar and maven) installed in the workspace clusters (jobcluster and interactive) and adds them to the unity catalog allowlist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import requests
import json
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.dropdown("LIB_TYPE", "jar",
                         ["jar", "maven", "jar/maven"], "LIB_TYPE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set variables

# COMMAND ----------

lib_type = dbutils.widgets.get("LIB_TYPE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility functions

# COMMAND ----------

# Get all clusters in the workspace
def get_clusters():
    return w.clusters.list()

# Get libraries for a specific cluster
def get_cluster_libraries(cluster_id):
    return w.libraries.cluster_status(cluster_id=cluster_id)

# Get all job clusters in the workspace
def get_job_clusters():
    jobs = w.jobs.list()
    job_clusters = []
    for job in jobs:
        job_details = w.jobs.get(job_id=job.job_id)
        if hasattr(job_details, 'settings') and hasattr(job_details.settings, 'job_clusters'):
            job_clusters.extend(job_details.settings.job_clusters)
    return job_clusters

# Extract relevant information from a library
def extract_library_info(library):
    if hasattr(library, 'jar') and library.jar != None:
        return {'name': library.jar, 'type': 'jar'}
    elif hasattr(library, 'maven') and hasattr(library.maven, 'coordinates') and library.maven != None:
        return {'name': f"{library.maven.coordinates}", 'type': 'maven'}
    else:
        return None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions that update the unity catalog allowlist

# COMMAND ----------

# Update the jar allowlist
def add_jars_to_unity_catalog_allowlist(libraries):
    """Add libraries to Unity Catalog allowlist"""
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    url = f"https://{workspace_url}/api/2.1/unity-catalog/artifact-allowlists/LIBRARY_JAR"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Get current allowlist
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error getting current allowlist: {response.text}")
        return

    current_allowlist = response.json()

    # Update allowlist with new libraries
    for lib in libraries:
        if (lib['type']  == 'jar' and (lib['name'].startswith('s3://') or lib['name'].startswith('/Volumes/'))):
            artifact = {
                "artifact": lib['name'],
                "match_type": 'PREFIX_MATCH'
            }
            if artifact not in current_allowlist.get('artifact_matchers', []):
                current_allowlist.setdefault('artifact_matchers', []).append(artifact)

    # Update the allowlist
    response = requests.put(url, headers=headers, json=current_allowlist)
    if response.status_code == 200:
        print("Successfully updated jar Unity Catalog allowlist")
    else:
        print(f"Error updating jar allowlist: {response.text}")

# Update the maven allowlist
def add_maven_to_unity_catalog_allowlist(libraries):
    """Add libraries to Unity Catalog allowlist"""
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    url = f"https://{workspace_url}/api/2.1/unity-catalog/artifact-allowlists/LIBRARY_MAVEN"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Get current allowlist
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error getting current allowlist: {response.text}")
        return

    current_allowlist = response.json()

    # Update allowlist with new libraries
    for lib in libraries:
        if lib['type']  == 'maven':
            artifact = {
                "artifact": lib['name'],
                "match_type": 'PREFIX_MATCH'
            }
            if artifact not in current_allowlist.get('artifact_matchers', []):
                current_allowlist.setdefault('artifact_matchers', []).append(artifact)

    # Update the allowlist
    response = requests.put(url, headers=headers, json=current_allowlist)
    if response.status_code == 200:
        print("Successfully updated maven Unity Catalog allowlist")
    else:
        print(f"Error updating maven allowlist: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code execution

# COMMAND ----------

# Initialize the Databricks SDK client
w = WorkspaceClient()

# Main execution
all_libraries = set()

# Get libraries from interactive clusters
clusters = get_clusters()
for cluster in clusters:
    library_statuses = get_cluster_libraries(cluster.cluster_id)
    for lib_status in library_statuses:
        lib_info = extract_library_info(lib_status.library)
        if lib_info:
            all_libraries.add(json.dumps(lib_info))

# Get libraries from job clusters
job_clusters = get_job_clusters()
for job_cluster in job_clusters:
    if hasattr(job_cluster, 'libraries'):
        for lib in job_cluster.libraries:
            lib_info = extract_library_info(lib)
            if lib_info:
                all_libraries.add(json.dumps(lib_info))

# Convert back to list of dicts
all_libraries = [json.loads(lib) for lib in all_libraries]

print(all_libraries)

# COMMAND ----------

# Add libraries to Unity Catalog allowlist
if lib_type == 'jar':
  add_jars_to_unity_catalog_allowlist(all_libraries)
elif lib_type == 'maven': 
  add_maven_to_unity_catalog_allowlist(all_libraries)
else:
  add_jars_to_unity_catalog_allowlist(all_libraries)
  add_maven_to_unity_catalog_allowlist(all_libraries)
