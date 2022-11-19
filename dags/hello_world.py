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
    hello_world_job_definition="hello-world",
    hello_world_job_queue="fargate-spot",
)


with dag:

    BatchOperator(
        task_id="fargate-job",
        job_definition="{{ var.value.hello_world_job_definition }}",
        job_name="hello",
        job_queue="{{ var.value.hello_world_job_queue }}",
    )
