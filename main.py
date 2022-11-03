#!/usr/bin/env python
"""
This module is the infrastructure entrypoint.
"""

from pathlib import Path

import toml
from cdktf import App

from aw_data_infra.literals import Environment
from aw_data_infra.stacks.airflow.dags import AirflowDags
from aw_data_infra.stacks.airflow.environment import AirflowEnvironment

CONFIG = toml.loads(
    Path("config.toml").read_text(),
)

app = App()


for environment, config in CONFIG.items():

    environment: Environment

    for airflow_environment in config["airflow_environments"]:

        bucket = f"allied-world-dags-{environment}-{airflow_environment.lower()}"

        AirflowDags(
            app,
            f"airflow-{environment}-{airflow_environment}-dags",
            environment=environment,
            bucket=bucket,
        )

        AirflowEnvironment(
            app,
            f"airflow-{environment}-{airflow_environment}-environment",
            environment=environment,
            mwaa_environment_name=airflow_environment,
            bucket=bucket,
            vpc=config["vpc"],
            subnets=config["subnets"],
            peer_vpc=config.get("peer_vpc"),
        )


app.synth()
