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
package io.hetu.core.plugin.hbase.security;

import io.hetu.core.plugin.hbase.metadata.TestingHBaseTableUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseKerberosAuthentication
 *
 * @since 2020-03-20
 */
public class TestHBaseKerberosAuthentication
{
    /**
     * testAuthenticateAndReturnUGI
     *
     * @throws IOException
     */
    @Test
    public void testAuthenticateAndReturnUGI()
            throws Exception
    {
        // check input parameters
        // userPrincipal is null
        try {
            HBaseKerberosAuthentication.authenticateAndReturnUGI(null, null, null, null);
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: input userPrincipal is invalid.");
        }

        // userKeytabPath is null
        try {
            HBaseKerberosAuthentication.authenticateAndReturnUGI("root", null, null, null);
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: input userKeytabPath is invalid.");
        }

        // krb5ConfPath is null
        try {
            HBaseKerberosAuthentication.authenticateAndReturnUGI("root", "./", null, null);
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: input krb5ConfPath is invalid.");
        }

        // conf is null
        try {
            HBaseKerberosAuthentication.authenticateAndReturnUGI("root", "./", "./", null);
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: input conf is invalid.");
        }

        // check file exist
        // userKeytabPath is not a file
        try {
            HBaseKerberosAuthentication.authenticateAndReturnUGI("root", "./", "./", new HBaseConfiguration());
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(true, e.toString().contains("java.io.IOException: userKeytabFile"), e.toString());
            assertEquals(true, e.toString().contains("is not a file"), e.toString());
        }

        // krb5ConfPath is not a file
        TestingHBaseTableUtils.createFile("user.keytab.test");
        try {
            HBaseKerberosAuthentication.authenticateAndReturnUGI(
                    "root", "./user.keytab.test", "./", new HBaseConfiguration());
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(true, e.toString().contains("java.io.IOException: krb5ConfFile"), e.toString());
            assertEquals(true, e.toString().contains("is not a file"), e.toString());
        }
        TestingHBaseTableUtils.delFile("user.keytab");
    }

    /**
     * testAuthenticateAndReturnUGINormal
     */
    @Test
    public void testAuthenticateAndReturnUGINormal()
            throws Exception
    {
        // krb5ConfPath is not a file
        TestingHBaseTableUtils.createFile("user.keytab.test");
        TestingHBaseTableUtils.createFile("krb5.conf.test");
        try {
            HBaseKerberosAuthentication.authenticateAndReturnUGI(
                    "root", "./user.keytab.test", "./krb5.conf.test", new HBaseConfiguration());
        }
        catch (IllegalStateException | NullPointerException e) {
            throw new IllegalStateException("testAuthenticateAndReturnUGINormal: failed");
        }
        TestingHBaseTableUtils.delFile("user.keytab.test");
        TestingHBaseTableUtils.delFile("krb5.conf.test");
    }

    /**
     * testSetJaasConf
     *
     * @throws IOException Exception
     */
    @Test
    public void testSetJaasConf()
            throws Exception
    {
        // loginContextName is null
        try {
            HBaseKerberosAuthentication.setJaasConf(null, null, null);
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(e.toString(), "java.io.IOException: inputnull is invalid.");
        }

        // userKeytabFile not exist
        try {
            TestingHBaseTableUtils.delFile("krb5.conf.test");
            TestingHBaseTableUtils.delFile("jaas.conf.test");
            HBaseKerberosAuthentication.setJaasConf("jaas.conf.test", "root", "krb5.conf.test");
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(true, e.toString().contains("java.io.IOException: userKeytabFile"), e.toString());
            assertEquals(true, e.toString().contains("does not exsit"), e.toString());
        }

        // Complete process
        TestingHBaseTableUtils.createFile("krb5.conf.test");
        TestingHBaseTableUtils.createFile("jaas.conf.test");
        try {
            HBaseKerberosAuthentication.setJaasConf("jaas.conf.test", "root", "krb5.conf.test");
            throw new IOException("testAuthenticate : failed");
        }
        catch (IOException e) {
            assertEquals(
                    e.toString(),
                    "java.io.IOException: AppConfigurationEntry named jaas.conf.test does not have value of keyTab.");
        }
        TestingHBaseTableUtils.delFile("krb5.conf.test");
        TestingHBaseTableUtils.delFile("jaas.conf.test");
    }
}
