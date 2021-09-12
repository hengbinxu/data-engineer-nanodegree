!/bin/bash

if [ "$1" != '--source-only' ]
then
    echo "ImportError: Invalid args: $@"
    exit 1
fi

add_conn() {
    # Add AWS credentials
    local AWS_ACCESS_KEY_ID=
    local AWS_SECRET_ACCESS_KEY=

    local DB_CONN_ID=
    local DB_TYPE=
    local DB_HOST=
    local DB_PORT=
    local DB_NAME=
    local DB_USER=
    local DB_PASSWORD=

    airflow connections --add --conn_id aws_credentials --conn_type "Amazon Web Services"\
        --conn_login ${AWS_ACCESS_KEY_ID} --conn_password ${AWS_SECRET_ACCESS_KEY}

    airflow connections --add --conn_id ${DB_CONN_ID} --conn_type ${DB_TYPE}\
        --conn_host ${DB_HOST} --conn_port ${DB_PORT} --conn_schema ${DB_NAME}\
        --conn_login ${DB_USER} --conn_password ${DB_PASSWORD}
}

