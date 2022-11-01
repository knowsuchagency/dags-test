# MWAA Infra

This repo contains IAC for deploying MWAA (managed airflow) to AWS as well as the dags in the [`dags/`](dags) folder.

The infrastructure-as-code entrypoint is [main.py](main.py).

Configuration for each environment can be found in [config.toml](config.toml)

## Usage

### Installation

```bash
# set up 3.10+ virtualenv
python3.10 -m venv .venv
. .venv/bin/activate
# install requirements
pip install -U pip
pip install -r requirements.txt
# we have to separate these, otherwise pip-installation fails (can't resolve)
pip install -r requirements-dags.txt
```

### Deployment

```bash
# list available stacks for deployment
cdktf list
# deploy a stack
cdktf deploy airflow-dev-dags
```
