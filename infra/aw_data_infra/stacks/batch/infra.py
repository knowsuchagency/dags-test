"""
This module's stack deploys batch job queues and compute environments.
"""

from cdktf_cdktf_provider_aws.batch_compute_environment import (
    BatchComputeEnvironment,
    BatchComputeEnvironmentComputeResources,
)
from cdktf_cdktf_provider_aws.batch_job_queue import BatchJobQueue
from cdktf_cdktf_provider_aws.security_group import (
    SecurityGroup,
    SecurityGroupIngress,
    SecurityGroupEgress,
)

from aw_data_infra.literals import (
    Environment,
)
from aw_data_infra.stacks.base import BaseStack


class Batch(BaseStack):

    security_group: SecurityGroup
    fargate_compute_environment: BatchComputeEnvironment
    fargate_spot_compute_environment: BatchComputeEnvironment
    fargate_job_queue: BatchJobQueue
    fargate_spot_job_queue: BatchJobQueue

    def __init__(
        self,
        scope,
        ns,
        environment: Environment,
        vpc: str,
        subnets: list[str],
        **kwargs,
    ):
        super().__init__(
            scope,
            ns,
            environment=environment,
            **kwargs,
        )

        self.vpc = vpc
        self.subnets = subnets

        self.default_service_role = f"arn:aws:iam::{self.account}:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch"

        self.security_group_name = "batch-sg"

        self.security_group = self.get_security_group()

        self.fargate_compute_environment = self.get_fargate_compute_environment(
            "fargate"
        )

        self.fargate_spot_compute_environment = self.get_fargate_compute_environment(
            "fargate-spot",
            spot=True,
            max_vcpus=2048,
        )

        self.fargate_spot_job_queue = BatchJobQueue(
            self,
            "fargate-spot-queue",
            name="fargate-spot",
            compute_environments=[self.fargate_spot_compute_environment.arn],
            state="ENABLED",
            priority=0,
        )

        self.fargate_job_queue = BatchJobQueue(
            self,
            "fargate-queue",
            name="fargate",
            compute_environments=[self.fargate_compute_environment.arn],
            state="ENABLED",
            priority=0,
        )

    def get_security_group(self):
        return SecurityGroup(
            self,
            "security-group",
            name=self.security_group_name,
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
            vpc_id=self.vpc,
            tags={"Name": self.security_group_name},
        )

    def get_fargate_compute_environment(
        self,
        name,
        spot: bool = False,
        max_vcpus: int = 256,
    ) -> BatchComputeEnvironment:
        return BatchComputeEnvironment(
            self,
            f"{name}-compute-environment",
            compute_environment_name=name,
            compute_resources=BatchComputeEnvironmentComputeResources(
                type="FARGATE_SPOT" if spot else "FARGATE",
                max_vcpus=max_vcpus,
                security_group_ids=[self.security_group.id],
                subnets=self.subnets,
            ),
            service_role=self.default_service_role,
            type="MANAGED",
        )
