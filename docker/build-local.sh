#!/usr/bin/env bash

set -euxo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ..
HETU_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

WORK_DIR="$(mktemp -d)"
cp ../hetu-server/target/hetu-server-${HETU_VERSION}.tar.gz ${WORK_DIR}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
rm ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/hetu-server-${HETU_VERSION}

cp ../hetu-cli/target/hetu-cli-${HETU_VERSION}-executable.jar ${WORK_DIR}

docker build ${WORK_DIR} -f Dockerfile --build-arg "HETU_VERSION=${HETU_VERSION}" -t "hetu:${HETU_VERSION}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

test_container "hetu:${HETU_VERSION}"
