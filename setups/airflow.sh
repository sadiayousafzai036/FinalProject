#!/bin/bash

echo "Changing permissions for dbt folder..."
cd ~/FinalProject/ && sudo chmod -R 777 dbt

echo "Building airflow docker images..."
cd ~/FinalProject/airflow
docker-compose build

echo "Running airflow-init..."
docker-compose up airflow-init

echo "Starting up airflow in detached mode..."
docker-compose up -d

echo "Airflow started successfully."
