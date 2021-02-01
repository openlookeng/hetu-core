/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hbase.conf;

import io.airlift.configuration.Config;
import io.prestosql.spi.function.Mandatory;

/**
 * hbase.properties
 *
 * @since 2020-03-30
 */
public class HBaseConfig
{
    private int retryNumber = 3; //hbase client retries to connect
    private int pauseTime = 100;
    private int retryCreateSnapshotNumber = 100;
    private boolean isRpcProtectionEnable; // Whether to enable hbase data communication encryption, default is false
    private String zkQuorum;
    private String zkClientPort;
    private String zkZnodeParent = "/hbase";
    private String metastoreType;
    private String defaultValue = "NULL";
    private String hdfsSitePath;
    private String coreSitePath;
    private String jaasConfPath; // java.security.auth.login.config: jaas.conf
    private String hbaseSitePath; // hbase-site.xml file path
    private String krb5ConfPath; // java.security.krb5.conf: krb5.conf
    private String userKeytabPath; // user.keytab file path
    private String principalUsername; // principal username
    private String kerberos;
    private boolean isClientSideEnable; // use client side mode

    public int getRetryNumber()
    {
        return retryNumber;
    }

    @Config("hbase.client.retries.number")
    public void setRetryNumber(int retryNumber)
    {
        this.retryNumber = retryNumber;
    }

    public int getPauseTime()
    {
        return pauseTime;
    }

    @Config("hbase.client.pause.time")
    public void setPauseTime(int pauseTime)
    {
        this.pauseTime = pauseTime;
    }

    public boolean isRpcProtectionEnable()
    {
        return isRpcProtectionEnable;
    }

    @Config("hbase.rpc.protection.enable")
    public void setRpcProtectionEnable(boolean rpcProtectionEnable)
    {
        isRpcProtectionEnable = rpcProtectionEnable;
    }

    public String getJaasConfPath()
    {
        return jaasConfPath;
    }

    @Config("hbase.jaas.conf.path")
    public void setJaasConfPath(String jaasConfPath)
    {
        this.jaasConfPath = jaasConfPath;
    }

    @Config("hbase.hdfs.site.path")
    public void setHdfsSitePath(String hdfsSitePath)
    {
        this.hdfsSitePath = hdfsSitePath;
    }

    public String getHdfsSitePath()
    {
        return hdfsSitePath;
    }

    @Config("hbase.core.site.path")
    public void setCoreSitePath(String coreSitePath)
    {
        this.coreSitePath = coreSitePath;
    }

    public String getCoreSitePath()
    {
        return coreSitePath;
    }

    public String getHbaseSitePath()
    {
        return hbaseSitePath;
    }

    @Config("hbase.hbase.site.path")
    public void setHbaseSitePath(String hbaseSitePath)
    {
        this.hbaseSitePath = hbaseSitePath;
    }

    public String getKrb5ConfPath()
    {
        return krb5ConfPath;
    }

    @Config("hbase.krb5.conf.path")
    public void setKrb5ConfPath(String krb5ConfPath)
    {
        this.krb5ConfPath = krb5ConfPath;
    }

    public String getUserKeytabPath()
    {
        return userKeytabPath;
    }

    @Config("hbase.kerberos.keytab")
    public void setUserKeytabPath(String userKeytabPath)
    {
        this.userKeytabPath = userKeytabPath;
    }

    public String getPrincipalUsername()
    {
        return principalUsername;
    }

    @Config("hbase.kerberos.principal")
    public void setPrincipalUsername(String principalUsername)
    {
        this.principalUsername = principalUsername;
    }

    public String getZkQuorum()
    {
        return zkQuorum;
    }

    @Mandatory(name = "hbase.zookeeper.quorum",
            description = "Zookeeper cluster address",
            defaultValue = "host1,host2",
            required = true)
    @Config("hbase.zookeeper.quorum")
    public void setZkQuorum(String zkQuorum)
    {
        this.zkQuorum = zkQuorum;
    }

    public String getZkClientPort()
    {
        return zkClientPort;
    }

    @Mandatory(name = "hbase.zookeeper.property.clientPort",
            description = "Zookeeper client port",
            defaultValue = "2181",
            required = true)
    @Config("hbase.zookeeper.property.clientPort")
    public void setZkClientPort(String zkClientPort)
    {
        this.zkClientPort = zkClientPort;
    }

    public String getZkZnodeParent()
    {
        return zkZnodeParent;
    }

    @Mandatory(name = "hbase.zookeeper.znode.parent",
            description = "Zookeeper znode parent of hbase",
            defaultValue = "/hbase")
    @Config("hbase.zookeeper.znode.parent")
    public void setZkZnodeParent(String zkZnodeParent)
    {
        this.zkZnodeParent = zkZnodeParent;
    }

    public String getMetastoreType()
    {
        return metastoreType;
    }

    @Mandatory(name = "hbase.metastore.type",
            description = "The storage of hbase metadata",
            defaultValue = "hetuMetastore")
    @Config("hbase.metastore.type")
    public void setMetastoreType(String metastoreType)
    {
        this.metastoreType = metastoreType;
    }

    public String getKerberos()
    {
        return kerberos;
    }

    @Config("hbase.authentication.type")
    public void setKerberos(String kerberos)
    {
        this.kerberos = kerberos;
    }

    public String getDefaultValue()
    {
        return defaultValue;
    }

    @Config("hbase.default.value")
    public void setDefaultValue(String defaultValue)
    {
        this.defaultValue = defaultValue;
    }

    public boolean isClientSideEnable()
    {
        return isClientSideEnable;
    }

    @Config("hbase.client.side.enable")
    public void setClientSideEnable(boolean isClientSideEnable)
    {
        this.isClientSideEnable = isClientSideEnable;
    }

    public int getRetryCreateSnapshotNumber()
    {
        return retryCreateSnapshotNumber;
    }

    @Config("hbase.client.side.snapshot.retry")
    public void setRetryCreateSnapshotNumber(int retryCreateSnapshotNumber)
    {
        this.retryCreateSnapshotNumber = retryCreateSnapshotNumber;
    }
}
