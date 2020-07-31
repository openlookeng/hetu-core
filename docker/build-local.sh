#!/usr/bin/env bash
CUSTOM_TAG=
BASE_IMAGE="centos:centos7"
TAG=$(date +%s)
IS_GIT_FOLDER=$(git rev-parse --is-inside-work-tree)

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
        TAG=$(git rev-parse HEAD | cut -c 1-8)
    fi
fi

[ -n "$CUSTOM_TAG" ] && TAG="$CUSTOM_TAG"

set -euo pipefail

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ..
#HETU_VERSION=$(mvn --quiet help:evaluate -Dexpression=project.version -DforceStdout)
HETU_VERSION=$(mvn --quiet help:evaluate -Dexpression=dep.hetu.version -DforceStdout)
popd

set -x
WORK_DIR="$(mktemp -d)"
cp ../hetu-server/target/hetu-server-${HETU_VERSION}.tar.gz ${WORK_DIR}
tar -C ${WORK_DIR} -xzf ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
rm ${WORK_DIR}/hetu-server-${HETU_VERSION}.tar.gz
cp -R bin default ${WORK_DIR}/hetu-server-${HETU_VERSION}

cp ../presto-cli/target/hetu-cli-${HETU_VERSION}-executable.jar ${WORK_DIR}

# tag the image using both the Hetu version and the latest git commit hash
# i.e.) hetu:316-23555bf9
docker build ${WORK_DIR} -f Dockerfile --build-arg "OPENLK_VERSION=${HETU_VERSION}" --build-arg "BASE_IMAGE=${BASE_IMAGE}" -t "openlookeng:${TAG}"

rm -r ${WORK_DIR}

# Source common testing functions
. container-test.sh

# one parameter: image name and flag to run hetu as coordinator
test_container "openlookeng:${TAG} -t coordinator"
