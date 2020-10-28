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

ARG BASE_IMAGE=centos:centos7
FROM $BASE_IMAGE
LABEL maintainer="OpenLooKeng Community"

ENV JAVA_HOME /usr/lib/jvm/java-8
RUN \
    set -eu &&\
    yum -y -q update &&\
    yum -y -q install  java-1.8.0-openjdk-devel sudo less &&\
    yum -q clean all &&\
    rm -rf /var/cache/yum /tmp/* /var/tmp/* && \
    groupadd openlkadmin --gid 1000 && \
    useradd openlkadmin --uid 1000 --gid 1000 && \
    gpasswd -a openlkadmin wheel &&\
    echo 'openlkadmin ALL=(ALL) NOPASSWD:ALL' | tee -a /etc/sudoers > /dev/null &&\
    mkdir -p /usr/lib/hetu /var/lib/hetu/data /var/log/hetu /etc/hetu &&\
    chmod 750 /usr/lib/hetu /var/lib/hetu/data /var/log/hetu /etc/hetu &&\
    chown -RL "openlkadmin:openlkadmin" /var/lib/hetu/data /usr/lib/hetu /var/log/hetu /etc/hetu

ARG OPENLK_VERSION
ARG PORT=8080

# Install OpenLooKeng Engine Executable
COPY --chown=openlkadmin:openlkadmin hetu-server-${OPENLK_VERSION} /usr/lib/hetu
# Install CLI
COPY --chown=openlkadmin:openlkadmin openlk /usr/bin/openlk

EXPOSE $PORT
USER openlkadmin:openlkadmin
ENV LANG en_US.UTF-8
HEALTHCHECK CMD curl --fail http://localhost:8080/v1/info/state || exit 1
ENTRYPOINT ["/usr/lib/hetu/bin/run-hetu"]
