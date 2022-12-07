# Orchestration Infrastructure

This repo contains IAC for deploying the following

* Managed Airflow
  * Environments
  * Dags
* AWS Batch
  * Compute Environments
  * Job Queues
  * Job Definitions

It uses the [CDK for Terraform](https://developer.hashicorp.com/terraform/cdktf) to define Terraform constructs in pure Python.

## Structure

### Data Infrastructure

The infrastructure-as-code entrypoint is [main.py](main.py).

The [aw_data_infra](infra/) package contains the classes and utilities for our terraform deployments.

Configuration for each environment can be found in [config.toml](config.toml).

## Usage

### Installation

```bash
# install cdktf cli
npm install --global cdktf-cli@latest
# set up 3.10+ virtualenv
python3.10 -m venv .venv
. .venv/bin/activate
# install requirements
pip install -U pip
pip install -r requirements-deployment.txt
# install the following if you're going to be editing dags in the dags/ directory
# otherwise feel free to ignore
pip install -r requirements-airflow.txt
```

### Deployment

```bash
cdk deploy
```

### Running airflow locally for development

```bash
# make sure you first install airflow/dags requirements
pip install -r requirements-airflow.txt

./scripts/run-airflow.zsh
```

**Note**:
You may occassionally run into a situation where the local airflow database becomes corrupted
in a way you can't fix with `airflow db reset`.

In that case, you can nuke the db by deleting the default airflow metadata directory `rm -rf ~/airflow`.
