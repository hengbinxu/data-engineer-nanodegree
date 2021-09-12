#!/bin/bash

# Fill the AIRFLOW__CORE__FERNET_KEY and ensure its consistency 
export AIRFLOW_HOME=$( pwd )
export AIRFLOW__CORE__FERNET_KEY=
# export AIRFLOW__CORE__LOAD_EXAMPLES=False

DB_PATH="$( pwd )/airflow.db"

if [ -f "${DB_PATH}" ]
then
    echo "${DB_PATH} exists"
else
    airflow initdb
fi

echo "Start Ariflow Webserver and Scheduler...."
# Start airflow
airflow scheduler --daemon
airflow webserver --daemon -p 8080

# Wait till airflow web-server is ready
echo "======================================"
echo "| Waiting for Airflow web server... |"
echo "======================================"

while true; do
  _RUNNING=$( ps aux | grep airflow-webserver | grep ready | wc -l )
  if [ $_RUNNING -eq 0 ]; then
    sleep 1
  else
    echo "Airflow web server is ready"
    break;
  fi
done

# Add connection information to Airflow
if [ "$1" == "--add-conn" ]
then
  ADD_CONN_FILE="./add-conn.sh"

  if [ -f "${ADD_CONN_FILE}" ];
  then
    # Load another script functions to add connection information to Airflow
    . "${ADD_CONN_FILE}" --source-only
    add_conn
  else
    "FindFileError: Can't find add_conn.sh"
  fi
fi
