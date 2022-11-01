#!/usr/bin/env python
from pathlib import Path

import toml
from box import Box
from cdktf import (
    App,
)

from stacks.airflow_environment import AirflowEnvironment
from stacks.dags import AirflowDags

CONFIG = Box(toml.loads(Path("config.toml").read_text()))


app = App()


for environment in CONFIG:

    for airflow_environment in CONFIG[environment].airflow_environments:

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
