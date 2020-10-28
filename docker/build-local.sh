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

CUSTOM_TAG=
BASE_IMAGE="centos:centos7"
TAG="$(date +%s)"
IS_GIT_FOLDER="$(git rev-parse --is-inside-work-tree)"
if [[ ! "${TAG}" =~ ^[0-9]+$ ]]
then
    echo "unexpected value from date"
    exit 1
fi

if [[ $# != 0 ]]; then
    while [[ $# != 0 && -n "$1" ]]; do
        case "$1" in
            -t | --customTag)
            # might need to edit if more flags added
                CUSTOM_TAG="$2"
                shift
                shift
                ;;
            --base-image)
                BASE_IMAGE="$2"
                shift
                shift
                ;;
            *)
                echo "not supported option $1"
                exit 1
                ;;
        esac
    done
fi

if [[ $IS_GIT_FOLDER = true ]] && [[ -z "$CUSTOM_TAG" ]]; then
    unstagedFiles=$(git ls-files --others --exclude-standard)
    git diff HEAD --quiet &>/dev/null
    if [ $? != 0 ] || [[ -n $unstagedFiles ]] ; then
        echo "Changes not committed, couldn't retrive a unique HASH for tagging"
        exit
    else
        # By default, tag the image using the latest git commit hash, e.g. openlookeng:23555bf9
        TAG=$(git rev-parse HEAD | cut -c 1-8)
        if [[ ! "${TAG}" =~ ^[0-9a-zA-Z]{8}$ ]]
        then
            echo "unexpected value from git"
            exit 1
        fi
    fi
fi

if [[ -n "$CUSTOM_TAG" ]];
then
    if [[  "${CUSTOM_TAG}" =~ ^[^/:\ ]+$ ]];
    then
        TAG="$CUSTOM_TAG"
    else
        echo "unexpected value for custom_tag"
        exit 1
    fi
fi

set -euo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ..
HETU_VERSION=$(mvn --quiet help:evaluate -Dexpression=dep.hetu.version -DforceStdout)
popd

trap '[ -n "${WORK_DIR}" ] && rm -rf "${WORK_DIR}"' EXIT
save_mask=$(umask)
umask 077
WORK_DIR="$(mktemp -d)"

cp ../hetu-server/target/hetu-server-${HETU_VERSION}.tar.gz ${WORK_DIR}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
rm ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/hetu-server-${HETU_VERSION}

cp ../presto-cli/target/hetu-cli-${HETU_VERSION}-executable.jar ${WORK_DIR}/hetu-server-${HETU_VERSION}
cp bin/openlk ${WORK_DIR}
umask "${save_mask}"

docker build ${WORK_DIR} -f Dockerfile --build-arg "OPENLK_VERSION=${HETU_VERSION}" --build-arg "BASE_IMAGE=${BASE_IMAGE}" -t "openlookeng:${TAG}"

# Source common testing functions
. container-test.sh

# one parameter: image name and flag to run hetu as coordinator
test_container "openlookeng:${TAG} -t coordinator"
