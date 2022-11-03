"""
This contains the base Terraform Stack.

It ensures consistent use of configuration and AWS resource tagging.
"""
import os

from cdktf import (
    TerraformStack,
    Aspects,
    S3Backend,
    TerraformBackend,
)
from cdktf_cdktf_provider_aws.provider import AwsProvider
from constructs import Construct

from aw_data_infra.aspects import TagsAddingAspect
from aw_data_infra.literals import Environment


class BaseStack(TerraformStack):

    environment: Environment
    account: str
    region: str
    stack_name: str

    def __init__(
        self,
        scope: Construct,
        ns: str,
        environment: Environment,
        tags: dict = None,
        region: str = None,
        backend: TerraformBackend = None,
    ):
        super().__init__(scope, ns)

        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.default_tags = {"cdktf": "true"}

        match environment:
            case "dev":
                self.account = "805321607950"
            case "stage":
                self.account = "645769240473"
            case "prod":
                self.account = "608056288583"
            case _:
                raise ValueError(f"unknown {environment = }")

        self.stack_name = ns
        self.environment = environment

        # TODO: switch to shared remote state backend
        self.backend = backend or S3Backend(
            self,
            bucket=f"allied-world-cdktf-state-{environment}",
            key=f"{ns.lower()}/terraform.tfstate",
        )

        self.aws_provider = AwsProvider(
            self,
            "AWS",
        )

        self.add_tags(tags)

    def add_tags(self, tags):
        """Add tags to every resource that can be tagged in the stack."""
        tags = tags or {}
        Aspects.of(self).add(TagsAddingAspect(self.default_tags | tags))
