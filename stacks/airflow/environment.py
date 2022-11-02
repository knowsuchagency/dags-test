"""
This module contains the stack used to deploy an MWAA Airflow environment.

Each environment will have its own unique S3 bucket for dags.
This is deployed in the same stack as the environment since they're tightly coupled.
"""
import json

from cdktf_cdktf_provider_aws.iam_role import IamRole, IamRoleInlinePolicy
from cdktf_cdktf_provider_aws.mwaa_environment import (
    MwaaEnvironment,
    MwaaEnvironmentNetworkConfiguration,
    MwaaEnvironmentLoggingConfiguration,
)
from cdktf_cdktf_provider_aws.s3_bucket import S3Bucket
from cdktf_cdktf_provider_aws.s3_bucket_public_access_block import (
    S3BucketPublicAccessBlock,
)
from cdktf_cdktf_provider_aws.security_group import (
    SecurityGroup,
    SecurityGroupIngress,
    SecurityGroupEgress,
)
from constructs import Construct

from stacks.literals import *
from stacks.base import BaseStack


class AirflowEnvironment(BaseStack):
    """This stack deploys the MWAA Environment and the bucket it relies on."""

    def __init__(
        self,
        scope: Construct,
        ns: str,
        environment: Environment,
        mwaa_environment_name: str,
        bucket: str,
        tags=None,
        airflow_version: AirflowVersion = "2.2.2",
        environment_class: EnvironmentClass = "mw1.small",
        webserver_access_mode: WebserverAccessMode = "PUBLIC_ONLY",
        max_workers: int = 10,
        min_workers: int = 1,
        schedulers: Schedulers = 2,
        logging_configuration: MwaaEnvironmentLoggingConfiguration = None,
    ):
        super().__init__(scope, ns, environment, tags=tags)

        self.airflow_version = airflow_version
        self.environment_class = environment_class
        self.webserver_access_mode = webserver_access_mode
        self.mwaa_environment_name = mwaa_environment_name
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.schedulers = schedulers
        self.logging_configuration = logging_configuration

        self.bucket_name = bucket

        self.bucket = self.get_s3_bucket()

        self.execution_role = self.get_execution_role()

        self.security_group = self.get_security_group()

        self.mwaa_environment = self.get_mwaa_environment()

    def get_s3_bucket(self):
        bucket = S3Bucket(
            self,
            "airflow-bucket",
            bucket=self.bucket_name,
            tags={
                "Name": self.bucket_name,
            },
        )

        S3BucketPublicAccessBlock(
            self,
            "block-public-access",
            bucket=bucket.bucket,
            block_public_acls=True,
            block_public_policy=True,
            ignore_public_acls=True,
            restrict_public_buckets=True,
        )

        return bucket

    def get_security_group(self):
        return SecurityGroup(
            self,
            "security-group",
            name=f"mwaa-{self.mwaa_environment_name}-sg",
            vpc_id=self.config.vpc,
            ingress=[
                SecurityGroupIngress(
                    description="allow all inbound traffic within self",
                    from_port=0,
                    to_port=0,
                    protocol="-1",
                    self_attribute=True,
                )
            ],
            egress=[
                SecurityGroupEgress(
                    description="allow all outbound traffic",
                    from_port=0,
                    to_port=0,
                    protocol="-1",
                    cidr_blocks=["0.0.0.0/0"],
                    ipv6_cidr_blocks=["::/0"],
                )
            ],
        )

    def get_mwaa_environment(self):

        match self.environment_class:
            case "mw1.small":
                scale_max, scale_min = 5, 5
            case "mw1.medium":
                scale_max, scale_min = 10, 5
            case "mw1.large":
                scale_max, scale_min = 20, 10
            case _:
                raise ValueError(
                    f"could not determine scaling for {self.environment_class = }"
                )

        logging_configuration = self.logging_configuration or (
            MwaaEnvironmentLoggingConfiguration(
                dag_processing_logs={
                    "enabled": True,
                    "log_level": "ERROR",
                },
                task_logs={
                    "enabled": True,
                    "log_level": "INFO",
                },
                scheduler_logs={
                    "enabled": True,
                    "log_level": "WARNING",
                },
                webserver_logs={
                    "enabled": True,
                    "log_level": "ERROR",
                },
                worker_logs={
                    "enabled": True,
                    "log_level": "ERROR",
                },
            )
        )

        return MwaaEnvironment(
            self,
            "mwaa-environment",
            dag_s3_path="dags/",
            execution_role_arn=self.execution_role.arn,
            name=self.mwaa_environment_name,
            network_configuration=MwaaEnvironmentNetworkConfiguration(
                security_group_ids=[self.security_group.id],
                subnet_ids=self.config.subnets,
            ),
            source_bucket_arn=self.bucket.arn,
            airflow_configuration_options={
                "core.execute_tasks_new_python_interpreter": "true",
                "celery.sync_parallelism": "1",
                "celery.worker_autoscale": f"{scale_max},{scale_min}",
                "scheduler.schedule_after_task_execution": "false",
                "core.killed_task_cleanup_time": "604800",
                # save the following optimizations for later if ever needed (they may not be)
                # "core.dag_file_processor_timeout": "150",
                # "core.dagbag_import_timeout": "90",
                # "core.min_serialized_dag_update_interval": "60",
                # "scheduler.dag_dir_list_interval": "300",
                # "scheduler.min_file_process_interval": "300",
                # "scheduler.parsing_processes": "2",
                # "scheduler.processor_poll_interval": "60",
            },
            webserver_access_mode=self.webserver_access_mode,
            environment_class=self.environment_class,
            airflow_version=self.airflow_version,
            min_workers=self.min_workers,
            max_workers=self.max_workers,
            schedulers=self.schedulers,
            logging_configuration=logging_configuration,
        )

    def get_execution_role(self):
        """Return execution role for MWAA."""
        return IamRole(
            self,
            "execution-role",
            name=f"mwaa-{self.environment}-{self.mwaa_environment_name}-execution-role",
            assume_role_policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": [
                                    "airflow-env.amazonaws.com",
                                    "airflow.amazonaws.com",
                                ]
                            },
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
            inline_policy=[
                IamRoleInlinePolicy(
                    name="mwaa-execution-role-inline-policy",
                    policy=json.dumps(
                        {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": "s3:GetAccountPublicAccessBlock",
                                    "Resource": "*",
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": "airflow:PublishMetrics",
                                    "Resource": (
                                        f"arn:aws:airflow:{self.region}:{self.account}:environment/"
                                        f"{self.mwaa_environment_name}"
                                    ),
                                },
                                {
                                    "Effect": "Deny",
                                    "Action": "s3:ListAllMyBuckets",
                                    "Resource": [
                                        f"arn:aws:s3:::{self.bucket_name}",
                                        f"arn:aws:s3:::{self.bucket_name}/*",
                                    ],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "s3:GetObject*",
                                        "s3:GetBucket*",
                                        "s3:List*",
                                    ],
                                    "Resource": [
                                        f"arn:aws:s3:::{self.bucket_name}",
                                        f"arn:aws:s3:::{self.bucket_name}/*",
                                    ],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "logs:CreateLogStream",
                                        "logs:CreateLogGroup",
                                        "logs:PutLogEvents",
                                        "logs:GetLogEvents",
                                        "logs:GetLogRecord",
                                        "logs:GetLogGroupFields",
                                        "logs:GetQueryResults",
                                    ],
                                    "Resource": [
                                        (
                                            f"arn:aws:logs:{self.region}:{self.account}:log-group:airflow-"
                                            f"{self.mwaa_environment_name}-*"
                                        )
                                    ],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["logs:DescribeLogGroups"],
                                    "Resource": ["*"],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": "cloudwatch:PutMetricData",
                                    "Resource": "*",
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "sqs:ChangeMessageVisibility",
                                        "sqs:DeleteMessage",
                                        "sqs:GetQueueAttributes",
                                        "sqs:GetQueueUrl",
                                        "sqs:ReceiveMessage",
                                        "sqs:SendMessage",
                                    ],
                                    "Resource": f"arn:aws:sqs:{self.region}:*:airflow-celery-*",
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "kms:Decrypt",
                                        "kms:DescribeKey",
                                        "kms:GenerateDataKey*",
                                        "kms:Encrypt",
                                        "kms:PutKeyPolicy",
                                    ],
                                    "NotResource": f"arn:aws:kms:*:{self.account}:key/*",
                                    "Condition": {
                                        "StringLike": {
                                            "kms:ViaService": [
                                                f"sqs.{self.region}.amazonaws.com"
                                            ]
                                        }
                                    },
                                },
                            ],
                        }
                    ),
                )
            ],
        )
