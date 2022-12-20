#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import (
    aws_s3_deployment as s3_deployment,
    aws_s3 as s3,
    pipelines,
    aws_codepipeline_actions as pipeline_actions,
)

AWS_ACCOUNT = os.getenv("AWS_ACCOUNT", "805321607950")
BUCKET = os.getenv("BUCKET", "allied-world-airflow-dev")

env = {
    "account": AWS_ACCOUNT,
    "region": "us-east-1",
}

tags = {
    "core": "true",
}


class DeployDags(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket.from_bucket_name(self, "bucket", BUCKET)

        s3_deployment.BucketDeployment(
            self,
            f"{BUCKET}-dags-deploy",
            sources=[s3_deployment.Source.asset("dags/")],
            destination_bucket=bucket,
            prune=False,
            destination_key_prefix="dags/",
            exclude=["__pycache__"],
        )


class Stage(cdk.Stage):
    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)
        DeployDags(
            self,
            "DeployDags",
            env=env,
            tags=tags,
            stack_name="deploy-dags",
        )


class Pipeline(cdk.Stack):
    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.python_version = "3.10.7"

        self.install_commands = [
            "ls $(pyenv root)/versions",
            f"pyenv install {self.python_version}",
            f"pyenv global {self.python_version}",
            'pip install -r requirements-deployment.txt',
            "npm i -g aws-cdk",
        ]

        self.input = pipelines.CodePipelineSource.git_hub(
            # TODO: update the way we source code once we have the necessary tokens/apps configured
            "knowsuchagency/dags-test",
            "deployment-pipeline",
            trigger=pipeline_actions.GitHubTrigger.POLL,
        )

        pipeline = pipelines.CodePipeline(
            self,
            "Pipeline",
            pipeline_name="data-engineering-dags",
            synth=pipelines.ShellStep(
                "Synth",
                input=self.input,
                install_commands=self.install_commands,
                commands=["cdk synth"],
            ),
        )

        pipeline.add_stage(
            Stage(
                self,
                "dags",
                env=env,
            ),
            pre=[pipelines.ManualApprovalStep("DeployDags")],
        )


app = cdk.App()


Pipeline(
    app,
    "pipeline",
    stack_name="data-engineering-dags",
    env=env,
    tags=tags,
)


app.synth()
