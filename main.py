#!/usr/bin/env python

import toml
from cdktf import App

from stacks.airflow.environment import AirflowEnvironment
from stacks.airflow.dags import AirflowDags
from stacks.literals import Environment

with open("config.toml") as fp:
    config = toml.load(fp)


app = App()


for environment in config:

    environment: Environment

    for airflow_environment in config[environment]["airflow_environments"]:

        bucket = f"allied-world-dags-{environment}-{airflow_environment}"

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
        )


app.synth()
