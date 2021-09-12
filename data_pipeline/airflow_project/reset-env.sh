#!/bin/bash

if [ $1 != "--source-only" ]
then
    echo "ImportError: Invalid args: $@"
    exit 1
fi

reset_env() {
    # Delete logs directory
    LOG_DIR="$( pwd )/logs"
    if [ -d ${LOG_DIR} ];
    then
        rm -rf ${LOG_DIR}
        echo "Remove directory: ${LOG_DIR}\n"
    fi

    delete_files_with_extension() {
        local EXTENSION=$1
        local DELETE_PATH=$2
        DELETE_PATH="${DELETE_PATH:=$( pwd )}"
        # If DELETE_PATH not set or null, set it to $( pwd )

        NUM_FILES=$( find ${DELETE_PATH} -name "*.${EXTENSION}" | wc -l )
        if [ ${NUM_FILES} -ne 0 ]
        then
            # Delete files with the same extension
            find ${DELETE_PATH} -name "*.${EXTENSION}" | xargs rm -v
        else
            echo "${DELETE_PATH} doesn't have file with ${EXTENSION}"
        fi
        echo -e "Totally delete ${NUM_FILES//[[:blank:]]} files\n"
    }

    # Check .pid files whether they're entirely deleted.
    while true
    do
        NUM_PID_FILES=$( find $( pwd ) -name "*.pid" | wc -l )
        if [ ${NUM_PID_FILES} -ne 0 ]
        then
            sleep 0.5
        else
            break
        fi
    done

    # List the extenstion type for deleting
    DELETE_EXTENSTIONS=("err" "log" "out" "cfg" "db")
    # Delete files with specific extenstion.
    for EXTENSION in ${DELETE_EXTENSTIONS[*]}
    do
        delete_files_with_extension ${EXTENSION}
    done
}

# Note:
# Assign default value to shell variable
# https://stackoverflow.com/questions/2013547/assigning-default-values-to-shell-variables-with-a-single-command-in-bash
