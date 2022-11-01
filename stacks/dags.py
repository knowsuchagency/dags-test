"""
This module contains the stack for deploying dags to S3.
"""
from pathlib import Path

from cdktf import (
    Fn,
    TerraformIterator,
)
from cdktf_cdktf_provider_aws.s3_object import S3Object
from constructs import Construct

from stacks.literals import *
from .base import BaseStack


class AirflowDags(BaseStack):
    """This stack deploys DAG code to s3."""

    def __init__(
        self,
        scope: Construct,
        ns: str,
        environment: Environment,
        bucket: str,
        tags: dict=None,
    ):

        super().__init__(scope, ns, environment, tags=tags)

        dags_path = Path("dags").resolve()

        S3Object(
            self,
            "dags-deployment",
            for_each=TerraformIterator.from_list(Fn.fileset(f"{dags_path}/", "*.py")),
            bucket=bucket,
            key="dags/${each.value}",
            source=f"{dags_path}/${{each.value}}",
            etag=f'filemd5("{dags_path}/${{each.value}}")',
        )
