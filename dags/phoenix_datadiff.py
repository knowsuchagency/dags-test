import datetime as dt
import json

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from common import set_defaults, run_batch_job

dag = DAG(
    dag_id="phoenix-datadiff",
    description="Comparing source and target Phoenix tables.",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
    catchup=True,
    tags=["phoenix"],
)

var = set_defaults(
    prefix="phoenix_datadiff",
    job_queue="fargate-spot",
    job_definition="datadiff",
    source_glue_connection="phoenixrep-development-ingestion-jdbc",
    target_glue_connection="phoenix-development-optimization",
    source_schema="public",
    target_schema="phoenix_development_ingested_dms",
    bucket="allied-world-dags-dev-data-engineering",
    tables_key="data/phoenix/datadiff/source_tables.json",
)


@task()
def get_tables() -> list:
    """Return list of tables to diff from source schema."""
    run_batch_job(
        job_name="phoenix-upload-source-tables-list",
        job_definition=var.job_definition,
        job_queue=var.job_queue,
        command="upload-tables",
        environment_variables={
            "SOURCE_SCHEMA": var.source_schema,
            "S3_TABLES_PATH": f"s3://{var.bucket}/{var.tables_key}",
        },
    )

    key = S3Hook().read_key(
        key=var.tables_key,
        bucket_name=var.bucket,
    )

    return json.loads(key)


@task
def run_datadiff(tables: list):
    """Execute batch array job to diff each table between source and target schemas."""
    context = get_current_context()

    run_batch_job(
        job_name="phoenix-datadiff",
        job_definition=var.job_definition,
        job_queue=var.job_queue,
        environment_variables={
            "SOURCE_GLUE_CONNECTION": var.source_glue_connection,
            "TARGET_GLUE_CONNECTION": var.target_glue_connection,
            "SOURCE_SCHEMA": var.source_schema,
            "TARGET_SCHEMA": var.target_schema,
            "TABLES": json.dumps(tables),
            "DATE": context["ds"],
        },
        array_size=len(tables),
        retries=1,
    )


with dag:

    tables = get_tables()

    run_datadiff(tables)
