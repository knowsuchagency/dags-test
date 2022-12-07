#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import (
    aws_s3_deployment as s3_deployment,
    aws_s3 as s3,
)

AWS_ACCOUNT = os.getenv("AWS_ACCOUNT", "805321607950")
BUCKET = os.getenv("BUCKET", "allied-world-airflow-dev")


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


app = cdk.App()

env = {
    "account": AWS_ACCOUNT,
    "region": "us-east-1",
}

tags = {
    "core": "true",
}

DeployDags(
    app,
    "DeployDags",
    env=env,
    tags=tags,
    stack_name="deploy-dags",
)


app.synth()
