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

from aw_data_infra.literals import Environment
from aw_data_infra.stacks.base import BaseStack


class AirflowDags(BaseStack):
    """This stack deploys DAG code to s3."""

    def __init__(
        self,
        scope: Construct,
        ns: str,
        environment: Environment,
        bucket: str,
        tags: dict = None,
        dags_path: Path | str = None,
    ):

        super().__init__(scope, ns, environment, tags=tags)

        self.dags_path = Path(dags_path or "dags").resolve()

        self.deploy_dags(bucket)

    def deploy_dags(self, bucket):
        """Deploys all python modules in the self.deploy_dags directory."""
        S3Object(
            self,
            "dags-deployment",
            for_each=TerraformIterator.from_list(
                Fn.fileset(f"{self.dags_path}/", "*.py")
            ),
            bucket=bucket,
            key="dags/${each.value}",
            source=f"{self.dags_path}/${{each.value}}",
            etag=f'filemd5("{self.dags_path}/${{each.value}}")',
        )
