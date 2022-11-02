"""
This contains the base Terraform Stack.

It ensures consistent use of configuration and AWS resource tagging.
"""
import logging
import os
from pathlib import Path

import toml
from box import Box
from cdktf import (
    TerraformStack,
    Aspects,
    S3Backend,
    TerraformBackend,
)
from cdktf_cdktf_provider_aws.provider import AwsProvider
from constructs import Construct

from stacks.aspects import TagsAddingAspect
from stacks.literals import *


class BaseStack(TerraformStack):

    environment: Environment
    aws_account: str
    region: str
    stack_name: str

    def __init__(
        self,
        scope: Construct,
        ns: str,
        environment: Environment,
        tags: dict = None,
        region: str = None,
        config_path: Path | str = None,
        config: Box = None,
        backend: TerraformBackend = None,
    ):
        super().__init__(scope, ns)

        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.default_tags = {"cdktf": "true"}

        if config and config_path:
            logging.warning(
                f"config and config_path both passed. only config will be used."
            )

        # default to config.toml in the root directory
        self.config_path = config_path or "config.toml"
        self._config = config

        match environment:
            case "dev":
                self.aws_account = "805321607950"
            case "stage":
                self.aws_account = "645769240473"
            case "prod":
                self.aws_account = "608056288583"
            case _:
                raise ValueError(f"unknown {environment = }")

        self.stack_name = ns
        self.environment = environment

        # TODO: switched to shared remote state backend
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

    @property
    def config(self) -> Box:
        if self._config:
            return self._config
        config = Box(toml.loads(Path(self.config_path).read_text()))
        return config[self.environment]
