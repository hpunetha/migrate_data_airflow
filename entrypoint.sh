#!/usr/bin/env bash

echo "Initializing the Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname FIRST_NAME \
    --lastname LAST_NAME \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Starting $1..."
exec airflow "$1"
