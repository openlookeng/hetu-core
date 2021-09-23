/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseClientConf
 *
 * @since 2020-03-20
 */
public class TestHBaseClientConf
{
    /**
     * testHBaseClientConf
     */
    @Test
    public void testHBaseClientConf()
    {
        HBaseConfig hcc = new HBaseConfig();

        hcc.setZkQuorum("zk1");
        assertEquals("zk1", hcc.getZkQuorum());

        hcc.setPrincipalUsername("root");
        assertEquals("root", hcc.getPrincipalUsername());

        hcc.setZkClientPort("2181");
        assertEquals("2181", hcc.getZkClientPort());

        hcc.setMetastoreType("type");
        assertEquals("type", hcc.getMetastoreType());

        hcc.setKerberos("file");
        assertEquals("file", hcc.getKerberos());

        hcc.setRetryNumber(10);
        assertEquals(10, hcc.getRetryNumber());

        hcc.setPauseTime(10);
        assertEquals(10, hcc.getPauseTime());

        hcc.setRpcProtectionEnable(true);
        assertEquals(true, hcc.isRpcProtectionEnable());

        hcc.setJaasConfPath("/etc/hetu/");
        assertEquals("/etc/hetu/", hcc.getJaasConfPath());

        hcc.setHbaseSitePath("/etc/hetu/");
        assertEquals("/etc/hetu/", hcc.getHbaseSitePath());

        hcc.setKrb5ConfPath("/etc/hetu/");
        assertEquals("/etc/hetu/", hcc.getKrb5ConfPath());

        hcc.setUserKeytabPath("/etc/hetu/");
        assertEquals("/etc/hetu/", hcc.getUserKeytabPath());

        hcc.setCoreSitePath("/etc/hetu/");
        assertEquals("/etc/hetu/", hcc.getCoreSitePath());

        hcc.setHdfsSitePath("/etc/hetu/");
        assertEquals("/etc/hetu/", hcc.getHdfsSitePath());

        hcc.setZkZnodeParent("/hbase");
        assertEquals("/hbase", hcc.getZkZnodeParent());

        hcc.setDefaultValue("default");
        assertEquals("default", hcc.getDefaultValue());

        hcc.setClientSideEnable(true);
        assertEquals(true, hcc.isClientSideEnable());

        hcc.setRetryCreateSnapshotNumber(10);
        assertEquals(10, hcc.getRetryCreateSnapshotNumber());
    }

    /**
     * testHBaseColumnProperties
     */
    @Test
    public void testHBaseColumnProperties()
    {
        HBaseColumnProperties.getColumnProperties();
    }
}
