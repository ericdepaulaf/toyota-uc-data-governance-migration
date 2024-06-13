from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import GroupsAPI, UsersAPI, ServicePrincipalsAPI
from typing import List, Union
from pyspark.sql import types as T, SparkSession, DataFrame
import logging
import json
from enum import Enum


class PrincipalType(Enum):
    ALL = "all"
    GROUP = "group"
    USER = "user"
    SERVICE_PRINCIPAL = "service_principal"


class PrincipalPermissions:
    def __init__(self, log_level=logging.DEBUG):
        """
        Initialize the PrincipalPermissions class.
        """

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(level=logging.DEBUG)  # Prevents changing other log levels

        self._spark = SparkSession.builder.getOrCreate()

        self._ATTRIBUTES = "displayName,id,roles"
        self._PRINCIPAL_READ_PER_CALL = 500

    def _principal_paginated_read(self, client: Union[GroupsAPI]) -> List[dict]:
        """
        Read the principals in a paginated manner.
        """

        not_last_page = True
        index = 1
        principals = []
        previous_principals = []
        len_previous_principals = 0
        while not_last_page:
            try:
                self._logger.info(f"Fetching principals from index: {index}")

                ret = client.list(attributes=self._ATTRIBUTES,
                                         count=self._PRINCIPAL_READ_PER_CALL,
                                         start_index=index)
                # self._logger.info(f"Principals fetched: {list(ret)}")
                ret = list(ret)
                ret_principals = sorted(ret, key=lambda x: x.id)
                len_ret_principals = len(ret_principals)

                if len_ret_principals < self._PRINCIPAL_READ_PER_CALL:
                    not_last_page = False
                    principals.extend(ret_principals)
                elif previous_principals:
                    if len_ret_principals == len_previous_principals:
                        not_last_page = False
                        # If last page, principals would already be present in the final list
                        for l, r in zip(ret_principals, previous_principals):
                            if l.display_name != r.display_name:
                                not_last_page = True
                                break

                index += self._PRINCIPAL_READ_PER_CALL-1
            
            except Exception as e:
                self._logger.error(f"Error while fetching principals: {e}")
                raise Exception(e)
            
            if not_last_page:
                previous_principals = ret_principals
                len_previous_principals = len_ret_principals
                principals.extend(ret_principals)
            else:
                self._logger.info(f"Last page reached. Principals read: {len(principals)}")

        principal_details = []
        principal_detail_keys = self._ATTRIBUTES.split(",")
        for principal in principals:
            principal_dict = principal.as_dict()
            principal_details.append(dict((key,principal_dict.get(key)) for key in principal_detail_keys))

        return principal_details
    
    def _get_ws_principals(self, client: Union[GroupsAPI, UsersAPI, ServicePrincipalsAPI], naming_convention_filter: str = "") -> DataFrame:
        """Get the ws groups from the source table.

        Returns:
            DataFrame: List of ws groups.
        """

        self._logger.info(f"Fetching principals from: {client.__class__.__name__}")

        schema = T.StructType([
            T.StructField("displayName", T.StringType(), True, {"comment": "User display name"}),
            T.StructField("id", T.StringType(), True, {"comment": "User ID"}),
            T.StructField("roles", T.ArrayType(
                T.StructType([
                    T.StructField("$ref", T.StringType(), True, {"comment": "User Ref"}),
                    T.StructField("value", T.StringType(), True, {"comment": "User ID"}),
                    T.StructField("display", T.StringType(), True, {"comment": "User Display Name"}),
                    T.StructField("primary", T.StringType(), True, {"comment": ""}),
                    T.StructField("type", T.StringType(), True, {"comment": ""})])), True, {"comment": "Roles"})
        ])
        
        try:
            ws_principals = self._principal_paginated_read(client)
            ws_principals_df = (
                self._spark.createDataFrame(ws_principals, schema=schema)
                .filter("roles.value IS NOT NULL")
                .dropDuplicates(["displayName", "id"])
                .selectExpr("displayName", "id", "roles.value as instance_profile_arn")
            )

            if naming_convention_filter != "":
                ws_principals_df = ws_principals_df.filter(f"displayName like '%{naming_convention_filter}%' ")

        except Exception as e:
            self._logger.error(f"Error while fetching workspace principals: {e}")
            raise Exception(e)
        
        return ws_principals_df

    def get_principal_permissions(self, principal_type: PrincipalType, naming_convention_filter: str = "") -> DataFrame:
        """
        Get the permissions for a cluster.
        """

        client = WorkspaceClient()

        match principal_type:
            case PrincipalType.ALL:
                ret = (
                    self._get_ws_principals(client.groups, naming_convention_filter)
                    .union(self._get_ws_principals(client.users, naming_convention_filter))
                    .union(self._get_ws_principals(client.service_principals, naming_convention_filter))
                )
            case PrincipalType.GROUP:
                ret = self._get_ws_principals(client.groups, naming_convention_filter)
            case PrincipalType.USER:
                ret = self._get_ws_principals(client.users, naming_convention_filter)
            case PrincipalType.SERVICE_PRINCIPAL:
                ret = self._get_ws_principals(client.service_principals, naming_convention_filter)
            case _:
                raise ValueError("Invalid principal type. Please use one of: group, user, service_principal")
        
        return ret.withColumnsRenamed({"displayName": "principal_display_name", "id": "principal_id"})