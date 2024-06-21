import logging
import boto3
import re
from pyspark.sql import SparkSession, types as T
from pyspark.dbutils import DBUtils
from typing import List, Tuple


class CrawlS3Paths:

    def __init__(self, log_level=logging.DEBUG):
        """
        Initialize the ClusterPermissions class.
        """

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(level=logging.DEBUG)  # Prevents changing other log levels

        self._spark = SparkSession.builder.getOrCreate()
        self._dbutils = DBUtils(self._spark)

    def __call__(self, instance_profile_arn: List[str], aws_secrets_scope: str) -> List:
        return self._get_s3_paths(instance_profile_arn, aws_secrets_scope)
    
    def _get_instance_profile_name(self, instance_profile_arn: str) -> str:
        """
        Get the instance profile name from the ARN.
        """
        match = re.match(r'arn:aws:iam::\d+:instance-profile/(.+)', instance_profile_arn)
        if match:
            return match.group(1)
        else:
            raise ValueError("Invalid instance profile ARN")
        
    def _get_instance_profile_roles(self, instance_profile_arn: str, iam_client) -> List:
        """
        Get the IAM roles for an instance profile.

        :param instance_profile_arn: The instance profile ARN
        :param iam_client: The IAM client

        :return: The IAM roles
        """
        
        try:
            # Get the instance profile ARN
            instance_profile_name = self._get_instance_profile_name(instance_profile_arn)

            # self._logger.debug(f"Getting roles for Instance profile: {instance_profile_name}")

            instance_profile_details = iam_client.get_instance_profile(InstanceProfileName=instance_profile_name)
        except Exception as e:
            self._logger.error(f"Failed to get instance profile details for ARN: {instance_profile_arn}. Error: {e}")
            return []

        # Get the list of IAM roles
        roles = instance_profile_details.get('InstanceProfile', []).get('Roles', [])

        self._logger.debug(f"Roles: {roles}")
        return roles
    

    def _get_role_s3_paths(self, role_name: str, iam_client) -> Tuple[List, List]:
        """
        Get the S3 paths for an IAM role.

        :param role_name: The IAM role name
        :param iam_client: The IAM client

        :return: The S3 paths
        """
        s3_paths = []
        s3_paths_regex = []

        self._logger.debug(f"Getting paths for IAM role: {role_name}")

        # Get the attached policies for the role
        attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)

        # self._logger.debug(f"Attached policies: {attached_policies}")

        for policy in attached_policies['AttachedPolicies']:

            # self._logger.debug(f"Getting paths for policy ARN: {policy['PolicyArn']}")

            # Retrieve the policy's default version details
            policy_details = iam_client.get_policy(PolicyArn=policy['PolicyArn'])
            # self._logger.debug(f"Policy details: {policy_details}")
            policy_version_id = policy_details['Policy']['DefaultVersionId']

            # Get the policy version document
            policy_version = iam_client.get_policy_version(
                PolicyArn=policy['PolicyArn'],
                VersionId=policy_version_id
            )

            # self._logger.debug(f"Policy version: {policy_version}")

            # Get the policy document
            policy_document = policy_version['PolicyVersion']['Document']

            # Check the policy statements
            if 'Statement' in policy_document:
                statements = policy_document['Statement']
                # self._logger.debug(f"Policy statements: {statements}")
                if type(statements) is not list:
                    statements = [statements]

                for statement in statements:
                    if statement['Effect'] == 'Allow' and 'Resource' in statement:
                        resources = statement['Resource']
                        if type(resources) is not list:
                            resources = [resources]  # Ensure resources is a list
                        for resource in resources:
                            if resource.startswith('arn:aws:s3:::'):
                                s3_paths.append(resource.replace('arn:aws:s3:::', 's3://'))
                                s3_paths_regex.append(resource.replace('arn:aws:s3:::', 's3://').replace('*', '.*'))

        return s3_paths, s3_paths_regex
        
    def _get_s3_paths(self, instance_profile_arn: List[str], aws_secrets_scope: str) -> List:
        """
        Get the S3 paths for an instance profile.
        """

        s3_paths = []

        # Initialize an empty list to store the IAM roles and S3 bucket paths
        iam_roles_and_s3_paths = []

        # Create an IAM client
        iam_client = boto3.client('iam',
                                  aws_access_key_id=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_access_key_id'),
                                  aws_secret_access_key=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_secret_access_key'),
                                  aws_session_token=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_session_token'))
        
        iam_client = boto3.client('iam')

        for profile_arn in instance_profile_arn:

            self._logger.debug(f"Getting paths for Instance profile ARN: {profile_arn}")

            # Get the roles for the instance profile
            roles = self._get_instance_profile_roles(profile_arn, iam_client)

            if roles in (None, []):
                continue

            # Add role names here or leave empty to get all roles
            role_filter_list = []

            # Iterate through the IAM roles
            for role in roles:
                # Get the role name
                role_name = role['RoleName']

                # Filter roles, if informed
                if len(role_filter_list) > 0 and role_name not in role_filter_list:
                    continue

                s3_paths, s3_paths_regex = self._get_role_s3_paths(role_name, iam_client)

                # Append role and its paths to the list
                if len(s3_paths) > 5 and len(s3_paths) == len(s3_paths_regex):
                    iam_roles_and_s3_paths.append((profile_arn, role_name, s3_paths, s3_paths_regex))

        schema = T.StructType(
            [
                T.StructField("instance_profile_arn", T.StringType(), True, metadata={"description": "The instance profile ARN"}),
                T.StructField("role_name", T.StringType(), True, metadata={"description": "The role name"}),
                T.StructField("s3_paths", T.ArrayType(T.StringType()), True, metadata={"description": "The S3 paths"}),
                T.StructField("s3_paths_regex", T.ArrayType(T.StringType()), True, metadata={"description": "The S3 paths regex"}),
            ]
        )

        return self._spark.createDataFrame(iam_roles_and_s3_paths, schema)
    
class CrawlS3Buckets:

    def __init__(self, log_level=logging.DEBUG):
        """
        Initialize the ClusterPermissions class.
        """

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(level=logging.DEBUG)  # Prevents changing other log levels

        self._spark = SparkSession.builder.getOrCreate()
        self._dbutils = DBUtils(self._spark)

    def get_s3_buckets(self, aws_secrets_scope: str) -> List:
        """
        Get the S3 buckets.
        """

        # Create an IAM client
        #s3_client = boto3.client('s3',
        #                          aws_access_key_id=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_access_key_id'),
        #                          aws_secret_access_key=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_secret_access_key'),
        #                          aws_session_token=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_session_token'))
        
        s3_client = boto3.client('s3')

        # List all buckets
        response = s3_client.list_buckets()

        # Print bucket names
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        # Crie um DataFrame a partir da lista
        schema = T.StructType([T.StructField("bucket_name", T.StringType(), True)])
        return self._spark.createDataFrame([(b,) for b in buckets], schema)
    
    def get_sub_folders_s3_buckets(self, aws_secrets_scope: str, s3_buckets_list: list) -> List:
        """
        Get the sub-folders of S3 buckets.
        """

        # Create an IAM client
        #s3_client = boto3.client('s3',
        #                          aws_access_key_id=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_access_key_id'),
        #                          aws_secret_access_key=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_secret_access_key'),
        #                          aws_session_token=self._dbutils.secrets.get(scope=aws_secrets_scope, key='aws_session_token'))
        
        s3_client = boto3.client('s3')
        volumes_list = []
        for bucket_name in s3_buckets_list:
            volumes_dict = {}
            folders = []
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Delimiter='/', Prefix='')
            try:
                for page in pages:
                    common_prefixes = page.get('CommonPrefixes', [])
                    for prefix in common_prefixes:
                        folder = prefix.get('Prefix')
                        folders.append(folder)
            except:
                print("Error to access s3 bucket : " + bucket_name)

            volumes_dict = {'s3_bucket_name': bucket_name, 'folders': folders}
            volumes_list.append(volumes_dict)

        # Create a list of rows from the list of dictionaries
        rows = [tuple(dict_item.values()) for dict_item in volumes_list]

        # Define the schema for the DataFrame
        schema = T.StructType([
            T.StructField("s3_bucket_name", T.StringType(), True),
            T.StructField("folders", T.ArrayType(T.StringType()), True)])

        # Create the DataFrame
        return self._spark.createDataFrame(rows, schema)
