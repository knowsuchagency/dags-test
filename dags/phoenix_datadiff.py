import datetime as dt
from airflow import DAG

from common import BatchOperator, set_defaults

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
    source_table="public.countries",
    target_table="phoenix_development_ingested_dms.countries",
)


with dag:

    BatchOperator(
        task_id="fargate-job",
        job_name="fargate-task",
        job_definition=var.job_definition,
        job_queue=var.job_queue,
        environment_variables={
            "INGESTION_GLUE_CONNECTION": var.source_glue_connection,
            "OPTIMIZATION_GLUE_CONNECTION": var.target_glue_connection,
            "SOURCE_TABLE": var.source_table,
            "TARGET_TABLE": var.target_table,
        },
    )
