"""
This module is for Fargate and EC2 job definitions for Batch.
"""

import json

from cdktf_cdktf_provider_aws.batch_job_definition import BatchJobDefinition
from cdktf_cdktf_provider_aws.iam_role import IamRole, IamRoleInlinePolicy
from cdktf_cdktf_provider_aws.iam_role_policy_attachment import IamRolePolicyAttachment

from aw_data_infra.stacks.base import BaseStack


class FargateJobDefinition(BaseStack):

    execution_role: IamRole
    job_definition: BatchJobDefinition

    def __init__(
        self,
        scope,
        ns,
        name: str,
        image: str,
        command: list[str],
        vcpu: int | float = 1,
        memory: int = 2048,
        environment_variables: dict = None,
        policy_statements: list[dict] = None,
        **kwargs,
    ):
        super().__init__(scope, ns, **kwargs)

        self.name = name
        self.image = image
        self.command = command
        self.vcpu = vcpu
        self.memory = memory
        self.environment_variables = environment_variables or {}
        self.policy_statements = policy_statements

        self.execution_role = self.get_execution_role()

        self.job_definition = BatchJobDefinition(
            self,
            "job-definition",
            name=self.name,
            type="container",
            platform_capabilities=["FARGATE"],
            container_properties=json.dumps(
                {
                    "command": self.command,
                    "environment": [
                        {"name": key, "value": value}
                        for key, value in self.environment_variables.items()
                    ],
                    "image": self.image,
                    "resourceRequirements": [
                        {"type": "VCPU", "value": f"{self.vcpu}"},
                        {"type": "MEMORY", "value": f"{self.memory}"},
                    ],
                    "executionRoleArn": self.execution_role.arn,
                }
            ),
        )

    def get_execution_role(self):

        inline_policy = None

        if self.policy_statements:

            inline_policy = [
                IamRoleInlinePolicy(
                    name=f"{self.name}-batch-execution-role-inline-policy",
                    policy=json.dumps(
                        {
                            "Version": "2012-10-17",
                            "Statement": statement,
                        }
                        for statement in self.policy_statements
                    ),
                )
            ]

        role = IamRole(
            self,
            "execution-role",
            name=f"{self.name}-execution-role",
            assume_role_policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "ecs-tasks.amazonaws.com",
                            },
                        }
                    ],
                }
            ),
            inline_policy=inline_policy,
        )

        IamRolePolicyAttachment(
            self,
            "policy-attachment",
            role=role.name,
            policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
        )

        return role
