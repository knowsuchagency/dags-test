# MWAA Infra

This repo contains IAC for deploying MWAA (managed airflow) to AWS. 
It uses the [CDK for Terraform](https://developer.hashicorp.com/terraform/cdktf) to define Terraform constructs in pure Python.

## Structure

### Data Infrastructure

The infrastructure-as-code entrypoint is [main.py](main.py).

The [aw_data_infra](infra/) package contains the classes and utilities for our terraform deployments.

Configuration for each environment can be found in [config.toml](config.toml).

## Usage

### Installation

```bash
# set up 3.10+ virtualenv
python3.10 -m venv .venv
. .venv/bin/activate
# install requirements
pip install -U pip
pip install -e .infra/
# install the following if you're going to be editing dags in the dags/ directory
# otherwise feel free to ignore
pip install -r requirements-dags.txt
```

### Deployment

```bash
# list available stacks for deployment
cdktf list
# deploy airflow environment
cdktf deploy airflow-dev-data-engineering-environment
# deploy dags
cdktf deploy airflow-dev-data-engineering-dags
```
