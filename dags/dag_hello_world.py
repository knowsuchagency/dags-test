from datetime import datetime as dt

from airflow import DAG

from common import BatchOperator, set_defaults

dag = DAG(
    dag_id="hello_world",
    description="A minimal example for orchestrating a job on AWS Batch.",
    schedule_interval=None,
    start_date=dt.today(),
    catchup=False,
    tags=["example"],
)

set_defaults(
    prefix="hello_world",
    job_queue="fargate-spot",
    job_definition="hello-world",
)


with dag:

    BatchOperator(
        task_id="fargate-job",
        job_name="fargate-task",
        job_definition="{{ var.value.job_definition }}",
        job_queue="{{ var.value.job_queue }}",
    )
