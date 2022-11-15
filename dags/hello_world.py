import json
import logging
from datetime import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator

from common import BatchOperator


@dag(
    schedule_interval=None,
    start_date=dt.today(),
    catchup=False,
    tags=["example"],
)
def hello_world_dag():
    @task()
    def extract():
        order_data_dict = json.loads('{"x": 1.3, "y": 3}')
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):
        logging.info(f"Total order value is: {total_order_value:.2f}")

    start = DummyOperator(task_id="start")

    fargate_job = BatchOperator(
        task_id="fargate-job",
        job_definition="hello-world",
        job_name="hello",
    )

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

    start >> [order_data, fargate_job]


hello_world_dag = hello_world_dag()
