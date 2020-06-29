#!/usr/bin/env bash

set -euxo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 HETU_VERSION"
    echo "Missing HETU_VERSION"
    exit 1
fi

HETU_VERSION=$1
HETU_LOCATION="http://10.183.167.145:8089/nexus/content/groups/datacloudFI/${HETU_VERSION}/hetu-server-${HETU_VERSION}.tar.gz"
CLIENT_LOCATION="http://10.183.167.145:8089/nexus/content/groups/datacloudFI/${HETU_VERSION}/hetu-cli-${HETU_VERSION}-executable.jar"

WORK_DIR="$(mktemp -d)"
curl -o ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz ${HETU_LOCATION}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
rm ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/hetu-server-${HETU_VERSION}

curl -o ${WORK_DIR}/hetu-cli-${HETU_VERSION}-executable.jar ${CLIENT_LOCATION}
chmod +x ${WORK_DIR}/hetu-cli-${HETU_VERSION}-executable.jar

docker build ${WORK_DIR} -f Dockerfile -t "hetu:${HETU_VERSION}" --build-arg "HETU_VERSION=${HETU_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container "hetu:${HETU_VERSION}"
