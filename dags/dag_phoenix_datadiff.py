import datetime as dt
import json
import logging
from operator import attrgetter

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from common import set_defaults, run_batch_job, slack_notification, BatchOperator
from models import DataDiffResult

dag = DAG(
    dag_id="phoenix-datadiff",
    description="Comparing source and target Phoenix tables.",
    schedule_interval="@daily",
    start_date=dt.datetime.today() - dt.timedelta(days=1),
    catchup=True,
    tags=["phoenix"],
    default_args={
        "on_failure_callback": slack_notification,
    },
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
    results_table="phoenix_datadiff_results",
    results_schema="dynamodb",
)

DataDiffResult.update_table_name(var.results_table)


@task(retries=1)
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


@task(retries=2)
def run_datadiff(tables: list):
    """Execute batch array job to diff each table between source and target schemas."""
    context = get_current_context()

    run_batch_job(
        job_name="phoenix-datadiff",
        command="diff-data",
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


@task(
    # run whether upstream task succeeds or fails
    trigger_rule="all_done",
)
def handle_exceptions():
    """Handle datadiff exceptions"""
    results = list(
        DataDiffResult.scan(
            filter_condition=DataDiffResult.exception.exists(),
        )
    )

    results.sort(key=attrgetter("exception", "table"))

    for r in results:
        logging.info(f"{r.table} - {r.exception}")


with dag:

    tables = get_tables()

    datadiff = run_datadiff(tables)

    datadiff >> handle_exceptions()

    # this operator isn't strictly necessary. quicksight reads directly from dynamodb
    results_to_redshift = BatchOperator(
        task_id="results-to-redshift",
        job_definition=var.job_definition,
        command=["dynamodb-to-redshift", var.results_table],
        job_name="phoenix-datadiff-results-to-redshift",
        job_queue=var.job_queue,
        environment_variables={
            "GLUE_CONNECTION": var.target_glue_connection,
            "SCHEMA": var.results_schema,
            "S3_PATH": f"s3://{var.bucket}/data/temp/",
        },
        # run whether upstream task succeeds or fails
        trigger_rule="all_done",
    )

    datadiff >> results_to_redshift
