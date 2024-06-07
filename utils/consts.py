__all__ = ["EXT_LOC_PRIVILEGES", "TF_TEMPLATE_DATASOURCE_EXT_LOC", "TF_TEMPLATE_RESOURCE_GRANT_EXT_LOC", "TF_TEMPLATE_INIT", "TF_TEMPLATE"]

EXT_LOC_PRIVILEGES = ["ALL_PRIVILEGES"]

TF_TEMPLATE_DATASOURCE_EXT_LOC = """\
data "databricks_external_location" "ext_loc_{sanitized_ext_loc_name}" {{
    name = "{ext_loc_name}"
}}\
"""

TF_TEMPLATE_RESOURCE_GRANT_EXT_LOC = """\
resource "databricks_grant" "principal_{sanitized_principal_display_name}_{sanitized_ext_loc_name}" {{
    external_location = databricks_external_location.ext_loc_{sanitized_ext_loc_name}.id

    principal  = "{principal_display_name}"
    privileges = [{privileges}]
}}\
"""

TF_TEMPLATE_INIT = """\
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}\
"""

TF_TEMPLATE = """\
{init_tf}

{ext_loc_datasources}

{grant_ext_loc_resources}\
"""