"""
Objects in this module are meant to be imported from the modules in which DAGs are defined.
"""
import json
import logging
import os
import shlex
from types import SimpleNamespace
from typing import *
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

    Returns: job id

    Warnings:
        Timeout terminations are handled on a best-effort basis. You shouldn't expect your timeout termination to happen exactly when the job attempt times out (it may take a few seconds longer). If your application requires precise timeout execution, you should implement this logic within the application. If you have a large number of jobs timing out concurrently, the timeout terminations behave as a first in, first out queue, where jobs are terminated in batches.
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

    if array_size:
        submit_job_kwargs.update(
            arrayProperties={
                "size": array_size,
            },
        )

    job = batch_client.submit_job(**submit_job_kwargs)

    job_id = job["jobId"]

    url = f"https://us-east-1.console.aws.amazon.com/batch/home?region=us-east-1#jobs/detail/{job_id}"

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
            transform_environment_variables: allows you to use airflow templates in environment variables
            array_size: if the job is an array job, this is the number of parallel jobs

        Warnings:
            Timeout terminations are handled on a best-effort basis. You shouldn't expect your timeout termination to happen exactly when the job attempt times out (it may take a few seconds longer). If your application requires precise timeout execution, you should implement this logic within the application. If you have a large number of jobs timing out concurrently, the timeout terminations behave as a first in, first out queue, where jobs are terminated in batches.
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
        )

        task_instance: TaskInstance = context["task_instance"]

        task_instance.xcom_push("job_id", job_id)
        task_instance.xcom_push("array_size", self.array_size)

        return job_id
