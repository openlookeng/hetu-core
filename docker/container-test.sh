#!/usr/bin/env bash

set -euxo pipefail

CONTAINER_ID=

function cleanup {
    if [[ ! -z ${CONTAINER_ID:-} ]]; then
        docker stop "${CONTAINER_ID}"
    fi
}

function test_container {
    { set +x; } 2>/dev/null
    local QUERY_TIMEOUT=150
    local QUERY_PERIOD=15
    local QUERY_RETRIES=$((QUERY_TIMEOUT/QUERY_PERIOD))

    trap cleanup EXIT

    local CONTAINER_NAME=$1
    CONTAINER_ID=$(docker run -d --rm ${CONTAINER_NAME})
    echo "Running container $CONTAINER_ID to test Hetu image..."

    set +e
    I=0
    # check if hetu instance is running
    sleep ${QUERY_PERIOD}
    until RESULT=$(docker exec "${CONTAINER_ID}" openlk --execute "SELECT 'success'"); do
        if [[ $((I++)) -ge ${QUERY_RETRIES} ]]; then
            echo "Too many retries waiting for Hetu to start."
            break
        fi
        sleep ${QUERY_PERIOD}
    done
    set -ex

    # Return proper exit code.
    [[ ${RESULT} == '"success"' ]]
}
