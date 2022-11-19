#!/usr/bin/env python
"""
This module is the infrastructure entrypoint.
"""

from pathlib import Path

from box import Box
from cdktf import App
from ruamel.yaml import YAML

from aw_data_infra.literals import Environment
from aw_data_infra.stacks.airflow.dags import AirflowDags
from aw_data_infra.stacks.airflow.environment import AirflowEnvironment
from aw_data_infra.stacks.batch.infra import Batch
from aw_data_infra.stacks.batch.job_definition import FargateJobDefinition

CONFIG = Box(YAML().load(Path("config.yml")))


app = App()


for environment, config in CONFIG.items():

    environment: Environment

    vpc = config["vpc"]
    backend_bucket = config.get("backend_bucket")

    airflow = config.get("airflow")
    batch = config.get("batch")

    if airflow:

        for airflow_environment in airflow["environments"]:

            bucket = f"allied-world-dags-{environment}-{airflow_environment.lower()}"

            AirflowDags(
                app,
                f"{environment}-airflow-{airflow_environment}-dags",
                environment=environment,
                bucket=bucket,
                backend_bucket=backend_bucket,
            )

            AirflowEnvironment(
                app,
                f"{environment}-airflow-{airflow_environment}-environment",
                environment=environment,
                mwaa_environment_name=airflow_environment,
                bucket=bucket,
                vpc=airflow.vpc,
                subnets=airflow.subnets,
                peer_vpc=vpc,
                backend_bucket=backend_bucket,
            )

    Batch(
        app,
        f"{environment}-batch-infra",
        environment=environment,
        vpc=vpc,
        subnets=batch.subnets,
        backend_bucket=backend_bucket,
    )

    FargateJobDefinition(
        app,
        f"{environment}-fargate-job-definition",
        environment=environment,
        name="hello-world",
        image="alpine",
        command=["echo", "hello-world"],
    )

app.synth()
