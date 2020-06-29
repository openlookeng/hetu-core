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

/**
 * hbase.properties
 *
 * @since 2020-03-30
 */
public class HBaseConfig
{
    private static final int RETRY_NUMBER = 3;
    private static final int PAUSE_TIME = 100;
    private int retryNumber = RETRY_NUMBER;
    private int pauseTime = PAUSE_TIME;
    private boolean isRpcProtectionEnable; // Whether to enable hbase data communication encryption, default is false
    private String zkQuorum;
    private String zkClientPort;
    private String metastoreType;
    private String metastoreUrl;
    private String defaultValue = "NULL";
    private String jaasConfPath; // java.security.auth.login.config: jaas.conf
    private String coreSitePath; // core-site.xml file path
    private String hdfsSitePath; // hdfs-site.xml file path
    private String hbaseSitePath; // hbase-site.xml file path
    private String krb5ConfPath; // java.security.krb5.conf: krb5.conf
    private String userKeytabPath; // user.keytab file path
    private String principalUsername; // principal username
    private String kerberos;

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

    public String getCoreSitePath()
    {
        return coreSitePath;
    }

    @Config("hbase.core.site.path")
    public void setCoreSitePath(String coreSitePath)
    {
        this.coreSitePath = coreSitePath;
    }

    public String getHdfsSitePath()
    {
        return hdfsSitePath;
    }

    @Config("hbase.hdfs.site.path")
    public void setHdfsSitePath(String hdfsSitePath)
    {
        this.hdfsSitePath = hdfsSitePath;
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

    @Config("hbase.zookeeper.quorum")
    public void setZkQuorum(String zkQuorum)
    {
        this.zkQuorum = zkQuorum;
    }

    public String getZkClientPort()
    {
        return zkClientPort;
    }

    @Config("hbase.zookeeper.property.clientPort")
    public void setZkClientPort(String zkClientPort)
    {
        this.zkClientPort = zkClientPort;
    }

    public String getMetastoreType()
    {
        return metastoreType;
    }

    @Config("hbase.metastore.type")
    public void setMetastoreType(String metastoreType)
    {
        this.metastoreType = metastoreType;
    }

    public String getMetastoreUrl()
    {
        return metastoreUrl;
    }

    @Config("hbase.metastore.uri")
    public void setMetastoreUrl(String metastoreUrl)
    {
        this.metastoreUrl = metastoreUrl;
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
}
