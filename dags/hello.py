import json
import logging
from datetime import datetime as dt

from airflow.decorators import dag, task

from common import EXAMPLE_STRING


@dag(
    schedule_interval=None,
    start_date=dt.today(),
    catchup=False,
    tags=["example"],
)
def tutorial_taskflow_api_etl():
    @task()
    def extract():
        logging.info(f"EXAMPLE_STRING: {EXAMPLE_STRING}")
        order_data_dict = json.loads(EXAMPLE_STRING)
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

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


tutorial_etl_dag = tutorial_taskflow_api_etl()
