/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hetu.core.plugin.singledata.shardingsphere;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.util.Locale;

import static com.google.common.base.Preconditions.checkState;

public class ShardingSphereConfig
{
    private String type;
    private String namespace;
    private String serverLists;
    private String databaseName;
    // zookeeper properties
    private int zkRetryIntervalMilliseconds = 500;
    private int zkMaxRetries = 3;
    private int zkTimeToLiveSeconds = 60;
    private int zkOperationTimeoutMilliseconds = 500;
    private String zkDigest = "";
    // etcd properties
    private long etcdTimeToLiveSeconds = 30L;
    private long etcdConnectionTimeout = 30L;

    @NotNull
    public String getType()
    {
        return type;
    }

    @Config("shardingsphere.type")
    @ConfigDescription("ShardingSphere cluster mode, only support zookeeper or etcd")
    public ShardingSphereConfig setType(String type)
    {
        String modeType = type.toLowerCase(Locale.ENGLISH);
        checkState(modeType.equals("zookeeper") || modeType.equals("etcd"), "Only support zookeeper or etcd cluster mode");
        this.type = modeType;
        return this;
    }

    @NotNull
    public String getNamespace()
    {
        return namespace;
    }

    @Config("shardingsphere.namespace")
    @ConfigDescription("ShardingSphere cluster namespace")
    public ShardingSphereConfig setNamespace(String namespace)
    {
        this.namespace = namespace;
        return this;
    }

    @NotNull
    public String getServerLists()
    {
        return serverLists;
    }

    @Config("shardingsphere.server-lists")
    @ConfigDescription("ShardingSphere cluster server lists")
    public ShardingSphereConfig setServerLists(String serverLists)
    {
        this.serverLists = serverLists;
        return this;
    }

    @NotNull
    public String getDatabaseName()
    {
        return databaseName;
    }

    @Config("shardingsphere.database-name")
    @ConfigDescription("ShardingSphere database name")
    public ShardingSphereConfig setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
        return this;
    }

    public int getZkRetryIntervalMilliseconds()
    {
        return zkRetryIntervalMilliseconds;
    }

    @Config("shardingsphere.zookeeper.retry-interval-milliseconds")
    @ConfigDescription("Retry interval milliseconds when connect with ZooKeeper curator client")
    public ShardingSphereConfig setZkRetryIntervalMilliseconds(int zkRetryIntervalMilliseconds)
    {
        this.zkRetryIntervalMilliseconds = zkRetryIntervalMilliseconds;
        return this;
    }

    public int getZkMaxRetries()
    {
        return zkMaxRetries;
    }

    @Config("shardingsphere.zookeeper.max-retries")
    @ConfigDescription("Max Retry times when connect with ZooKeeper curator client")
    public ShardingSphereConfig setZkMaxRetries(int zkMaxRetries)
    {
        this.zkMaxRetries = zkMaxRetries;
        return this;
    }

    public int getZkTimeToLiveSeconds()
    {
        return zkTimeToLiveSeconds;
    }

    @Config("shardingsphere.zookeeper.time-to-live-seconds")
    @ConfigDescription("ZooKeeper client session timeout value")
    public ShardingSphereConfig setZkTimeToLiveSeconds(int zkTimeToLiveSeconds)
    {
        this.zkTimeToLiveSeconds = zkTimeToLiveSeconds;
        return this;
    }

    public int getZkOperationTimeoutMilliseconds()
    {
        return zkOperationTimeoutMilliseconds;
    }

    @Config("shardingsphere.zookeeper.operation-timeout-milliseconds")
    @ConfigDescription("ZooKeeper client operation timeout value")
    public ShardingSphereConfig setZkOperationTimeoutMilliseconds(int zkOperationTimeoutMilliseconds)
    {
        this.zkOperationTimeoutMilliseconds = zkOperationTimeoutMilliseconds;
        return this;
    }

    public String getZkDigest()
    {
        return zkDigest;
    }

    @Config("shardingsphere.zookeeper.digest")
    @ConfigDescription("ZooKeeper client connection authorization schema name")
    public ShardingSphereConfig setZkDigest(String zkDigest)
    {
        this.zkDigest = zkDigest;
        return this;
    }

    public long getEtcdTimeToLiveSeconds()
    {
        return etcdTimeToLiveSeconds;
    }

    @Config("shardingsphere.etcd.time-to-live-seconds")
    @ConfigDescription("timeToLiveSeconds")
    public ShardingSphereConfig setEtcdTimeToLiveSeconds(long etcdTimeToLiveSeconds)
    {
        this.etcdTimeToLiveSeconds = etcdTimeToLiveSeconds;
        return this;
    }

    public long getEtcdConnectionTimeout()
    {
        return etcdConnectionTimeout;
    }

    @Config("shardingsphere.etcd.connection-timeout")
    @ConfigDescription("connectionTimeout")
    public ShardingSphereConfig setEtcdConnectionTimeout(long etcdConnectionTimeout)
    {
        this.etcdConnectionTimeout = etcdConnectionTimeout;
        return this;
    }
}
