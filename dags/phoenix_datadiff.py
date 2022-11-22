import datetime as dt
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from common import BatchOperator, set_defaults, run_batch_job

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
    s3_bucket="allied-world-dags-dev-data-engineering",
    s3_key="data/phoenix/datadiff/source_tables.json",
)


@task()
def get_tables() -> list:
    s3 = S3Hook()
    key = s3.read_key(key=var.s3_key, bucket_name=var.s3_bucket)
    return json.loads(key)


@task
def datadiff(tables: list):
    run_batch_job(
        job_name="phoenix-datadiff",
        job_definition=var.job_definition,
        job_queue=var.job_queue,
        environment_variables={
            "INGESTION_GLUE_CONNECTION": var.source_glue_connection,
            "OPTIMIZATION_GLUE_CONNECTION": var.target_glue_connection,
            "SOURCE_SCHEMA": var.source_schema,
            "TARGET_SCHEMA": var.target_schema,
            "TABLES": json.dumps(tables),
        },
        array_size=len(tables),
    )


with dag:

    upload_tables = BatchOperator(
        task_id="upload_tables",
        job_name="phoenix-upload-source-tables-list",
        job_definition=var.job_definition,
        job_queue=var.job_queue,
        command=["upload-tables"],
        environment_variables={
            "SOURCE_SCHEMA": var.source_schema,
            "S3_PATH": f"s3://{var.s3_bucket}/{var.s3_key}",
        },
    )

    tables = get_tables()

    upload_tables >> tables

    datadiff(tables)
