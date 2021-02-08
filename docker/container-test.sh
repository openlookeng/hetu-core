#!/usr/bin/env bash

# Copyright (C) 2020. Huawei Technologies Co., Ltd. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

CONTAINER_ID=

function cleanup {
    if [[ ! -z ${CONTAINER_ID:-} ]]; then
        docker stop "${CONTAINER_ID}"
    fi
}

function test_container {
    local QUERY_TIMEOUT=150
    local QUERY_PERIOD=15
    local QUERY_RETRIES=$((QUERY_TIMEOUT/QUERY_PERIOD))

    trap cleanup EXIT

    local CONTAINER_NAME="$1"
    if [[ ! "${CONTAINER_NAME}" =~ ^[^/]+$ ]];
    then
        echo "unexpected argument: container name"
        exit 1
    fi
    CONTAINER_ID=$(docker run -d --rm ${CONTAINER_NAME})
    if [[ ! "${CONTAINER_ID}" =~ ^[0-9a-zA-Z]+$ ]]
    then
        echo "unexpected value from docker run"
        exit 1
    fi
    echo "Running container $CONTAINER_ID to test Hetu image..."

    set +e
    I=0
    # check if hetu instance is running
    sleep ${QUERY_PERIOD}
    until RESULT=$(docker exec "${CONTAINER_ID}" openlk --execute "SELECT 'success'" | tail -1); do
        if [[ $((I++)) -ge ${QUERY_RETRIES} ]]; then
            echo "Too many retries waiting for Hetu to start."
            break
        fi
        sleep ${QUERY_PERIOD}
    done
    set -e

    # Return proper exit code.
    [[ ${RESULT} == '"success"' ]]
}
