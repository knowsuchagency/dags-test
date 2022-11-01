# MWAA Infra

This repo contains IAC for deploying MWAA (managed airflow) to AWS as well as the dags in the [`dags/`](dags) folder.

## Usage

```bash
# set up 3.10+ virtualenv
python3.10 -m venv .venv
. .venv/bin/activate
# install requirements
pip install -U pip ; pip install -r requirements.txt
# deploy terraform
cdktf deploy airflow-dev
```
