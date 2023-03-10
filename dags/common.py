"""
Objects in this module are meant to be imported from the modules in which DAGs are defined.
"""
import json
import logging
import os
import re
import shlex
import base64
from textwrap import dedent
from types import SimpleNamespace
from typing import *

import requests
from airflow.models import TaskInstance, BaseOperator, Variable
from airflow.providers.amazon.aws.hooks.batch_client import (
    AwsBatchClientHook,
    AwsBatchProtocol,
)
from jinja2 import Template


def set_defaults(deserialize_json=False, prefix=None, **kwargs) -> SimpleNamespace:
    """Simplifies setting (and retrieving) airflow variables."""
    result = {}
    for var_name, default in kwargs.items():
        result[var_name] = Variable.setdefault(
            f"{prefix}_{var_name}" if prefix else var_name,
            default,
            deserialize_json=deserialize_json,
        )
    return SimpleNamespace(**result)


def run_batch_job(
        job_definition: str,
        command: Optional[Union[list, str]] = None,
        job_name: str = None,
        job_queue: str = "fargate-spot",
        check_success: bool = True,
        tags: dict = None,
        vcpu: Union[int, float] = 1,
        memory: int = 2048,
        timeout: int = None,
        environment_variables: dict = None,
        array_size: int = None,
        retries: int = None,
):
    """
    Run job in AWS Batch.

    Args:
        command: the command to run in the container
        job_name: what to name of the job in AWS Batch
        job_definition: the Batch job definition to execute
        job_queue: the queue on which to submit the job
        check_success: if true only return successfully if the job terminates successfully
        tags: metadata to be given to the job
        vcpu: number of virtual cpu's for the job
        memory: maximum amount of memory the job can use
        timeout: how long until the job times out
        environment_variables: environment variables to pass to the job
        retries: the number of times to retry a failed job

    Returns: job id

    Warnings:
        Timeout terminations are handled on a best-effort basis. You shouldn't expect your
        timeout termination to happen exactly when the job attempt times out (it may take a few
        seconds longer). If your application requires precise timeout execution, you should
        implement this logic within the application. If you have a large number of jobs timing
        out concurrently, the timeout terminations behave as a first in, first out queue,
        where jobs are terminated in batches.
        https://docs.aws.amazon.com/batch/latest/userguide/job_timeouts.html

    """
    assert (
            command or job_name
    ), "you must provide either a name for the job or a command to run"

    if not job_name:
        if isinstance(command, list):
            job_name = command[0]
        elif isinstance(command, str):
            job_name = command.split()[0]
        else:
            raise ValueError("Could not determine job name")

    tags = tags or {}
    environment_variables = environment_variables or {}

    batch_hook = AwsBatchClientHook()

    batch_client: AwsBatchProtocol = batch_hook.client

    container_overrides = {
        "resourceRequirements": [
            {"type": "MEMORY", "value": f"{memory}"},
            {"type": "VCPU", "value": f"{vcpu}"},
        ],
    }

    if command:
        container_overrides["command"] = (
            command if isinstance(command, list) else shlex.split(command)
        )

    if environment_variables:
        container_overrides["environment"] = [
            {"name": key, "value": value}
            for key, value in environment_variables.items()
        ]

    print(
        "container overrides:",
        json.dumps(container_overrides, indent=2),
        sep=os.linesep,
    )

    submit_job_kwargs = {
        "jobName": job_name,
        "jobQueue": job_queue,
        "jobDefinition": job_definition,
        "containerOverrides": container_overrides,
        "tags": tags,
    }

    if timeout:
        submit_job_kwargs.update(
            timeout={
                "attemptDurationSeconds": timeout,
            }
        )

    if retries:
        submit_job_kwargs.update(
            retryStrategy={
                "attempts": retries,
            }
        )

    if array_size:
        submit_job_kwargs.update(
            arrayProperties={
                "size": array_size,
            },
        )

    job = batch_client.submit_job(**submit_job_kwargs)

    job_id = job["jobId"]

    url = f"https://us-east-1.console.aws.amazon.com/batch/home?region=us-east-1#jobs/detail/" \
          f"{job_id}"

    logging.info(url)

    if not check_success:
        return job_id

    batch_hook.wait_for_job(job_id)

    try:

        batch_hook.check_job_success(job_id)

    except Exception as e:

        raise type(e)(url + "\n" + str(e))

    logging.info(url)

    return job_id


class BatchOperator(BaseOperator):
    """
    This operator is for submitting jobs to AWS Batch.
    """

    template_fields = (
        "command",
        "job_name",
        "job_queue",
        "job_definition",
        "vcpu",
        "memory",
        "check_success",
        "batch_retries",
    )

    def __init__(
            self,
            job_definition: str,
            job_queue: str = "fargate-spot",
            command: Optional[Union[list, str]] = None,
            job_name: Optional[str] = None,
            check_success: bool = True,
            tags: Optional[dict] = None,
            vcpu: Union[str, int] = 1,
            memory: Union[str, int] = 2048,
            environment_variables: dict = None,
            timeout: Optional[int] = None,
            transform_environment_variables: bool = True,
            array_size: int = None,
            batch_retries: int = None,
            **kwargs,
    ):
        """
        Run job on AWS Batch.

        Args:
            command: the command to be run in the container
            job_name: the name the job will be given
            job_queue: the queue on which to run the job
            job_definition: the aws batch definition to execute
            check_success: if true only return successfully if the job terminates successfully
            tags: metadata to be given to the job
            vcpu: the minimum number of vcpu's for the job
            memory: the maximum about of memory for the job. converted to GiB if < 1024 else MiB
            environment_variables: environment variables to pass to the job
            timeout: timeout for job in seconds
            transform_environment_variables: allows you to use airflow templates in environment
            variables
            array_size: if the job is an array job, this is the number of parallel jobs
            batch_retries: the number of times AWS Batch will retry a failed job

        Warnings:
            Timeout terminations are handled on a best-effort basis. You shouldn't expect your
            timeout termination to happen exactly when the job attempt times out (it may take a
            few seconds longer). If your application requires precise timeout execution,
            you should implement this logic within the application. If you have a large number of
            jobs timing out concurrently, the timeout terminations behave as a first in,
            first out queue, where jobs are terminated in batches.
            https://docs.aws.amazon.com/batch/latest/userguide/job_timeouts.html
        """
        super().__init__(**kwargs)

        self.command = command
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.job_name = job_name
        self.check_success = check_success
        self.tags = tags
        self.vcpu = vcpu
        self.memory = memory
        self.environment_variables = environment_variables
        self.timeout = timeout
        self.transform_environment_variables = transform_environment_variables
        self.array_size = array_size
        self.batch_retries = batch_retries

    def execute(self, context):

        if self.transform_environment_variables and self.environment_variables:
            environment_variables = {
                k: Template(v).render(context)
                for k, v in self.environment_variables.items()
            }
        else:
            environment_variables = self.environment_variables

        job_id = run_batch_job(
            command=self.command,
            job_definition=self.job_definition,
            job_queue=self.job_queue,
            job_name=self.job_name,
            check_success=self.check_success,
            tags=self.tags,
            vcpu=self.vcpu,
            memory=self.memory,
            environment_variables=environment_variables,
            timeout=self.timeout,
            array_size=self.array_size,
            retries=self.batch_retries,
        )

        task_instance: TaskInstance = context["task_instance"]

        task_instance.xcom_push("job_id", job_id)
        task_instance.xcom_push("array_size", self.array_size)

        return job_id


def slack_notification(context):
    """Send slack alert in event of failure."""

    logging.info("sending slack notification")

    url = Variable.get(
        "slack_alerts_webhook_url",
        default_var="https://hooks.slack.com/services/{{foo}}",
    )

    task_instance = context.get("task_instance")

    task = task_instance.task_id
    dag = task_instance.dag_id
    log_url = task_instance.log_url
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    # we had a naming convention for dags this would take advantage of to determine the color of
    # the slack alert emoji
    m = re.search(r"priority_?(\d)", dag)

    priority = m.group(1) if m is not None else "3"

    alert_emoji = {
        "0": "black_circle",
        "1": "red_circle",
        "2": "large_orange_circle",
        "3": "large_blue_circle",
    }.get(priority, "large_blue_circle")

    data = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":{alert_emoji}: *{dag}.{task}*",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Time*: {execution_date}",
                    }
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"Exception: {exception}"},
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "logs",
                        "emoji": True,
                    },
                    "value": "click_me_123",
                    "url": log_url,
                    "action_id": "button-action",
                },
            },
        ]
    }

    resp = requests.post(url, json=data)

    logging.info("slack response:", resp.content)

    logging.info("slack notification sent")


def create_jira_ticket(context):
    """Create a JIRA ticket in the event of failure."""

    url = Variable.get(
        "jira_api_url",
        default_var="https://alliedworld.atlassian.net/rest/api/3/issue",
    )
    token = Variable.get(
        "jira_username_token",
        default_var="",
    ).strip()

    project_id = Variable.get(
        "jira_project_id",
        default_var="11898",  # DP Project
    )
    parent_ticket_key = Variable.get(
        "jira_parent_ticket_key",
        default_var="DP-400",  # Phoenix Pipeline Errors Epic
    )
    reporter_id = Variable.get(
        "jira_reporter_id",
        default_var="6296b0a81a2bdf0070950a79",  # alison.stanton@awacservices.com should be reporter
    )
    priority_id = Variable.get(
        "jira_priority_id",
        default_var="10104",  # Minor
    )
    story_points_id = Variable.get(
        "jira_story_points_id",
        default_var="14676",  # Story Points = 2
    )

    timestamp = context['ts']
    ds = context['ds']
    task_instance = context['ti']
    task = task_instance.task_id
    dag = task_instance.dag_id
    log_url = task_instance.log_url
    issue_type_id = "11502"  # Issue is a Task

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Basic " + base64.b64encode(token.encode()).decode("utf-8")
    }

    data = {
        "fields": {
            "summary": f"[jira-test] Airflow Pipeline Failure: {dag}-{task}",
            "project": {
                "id": project_id
            },
            "parent": {
                "key": parent_ticket_key
            },
            "issuetype": {
                "id": issue_type_id
            },
            "priority": {
                "id": priority_id
            },
            "customfield_12507": ds,  # Start Date custom field -- has to be a data in format 'YYYY-MM-DD'
            "customfield_12863": {  # Story Points
                "id": story_points_id
            },
            "customfield_12862": "When a pipeline fails, the DP teams wants to capture the failure so they can address it and fix the pipeline.",
            "reporter": {
                "id": reporter_id
            },
            "labels": [
                "Phoenix",
                "Error",
            ],
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {
                                "type": "text",
                                "text": dedent(
                                    f"""
                                    Date: {timestamp}
                                    DAG: {dag}
                                    Task: {task}
                                    Logs: {log_url}
                                    """)
                            }
                        ]
                    }
                ]
            },
        }
    }

    logging.info("creating JIRA ticket")
    resp = requests.post(url, json=data, headers=headers)
    resp.raise_for_status()
    logging.info(f"JIRA ticket: {resp.text}")
