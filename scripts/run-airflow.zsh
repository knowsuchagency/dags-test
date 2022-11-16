#!/bin/zsh

export AIRFLOW__CORE__DAGS_FOLDER=./dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow standalone
