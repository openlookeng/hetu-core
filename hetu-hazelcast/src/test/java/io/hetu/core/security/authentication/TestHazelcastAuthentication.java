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
package io.hetu.core.security.authentication;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.hetu.core.security.authentication.kerberos.KerberosAuthenticator;
import io.hetu.core.security.authentication.kerberos.KerberosConfig;
import io.hetu.core.security.authentication.kerberos.KerberosTokenCredentials;
import mockit.Mock;
import mockit.MockUp;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class TestHazelcastAuthentication
{
    private static final int PORT1 = 5701;
    private static final int PORT2 = 5702;
    private HazelcastInstance hazelcastInstance1;
    private HazelcastInstance hazelcastInstance2;
    private HazelcastInstance clientInstance;

    public static class KerberosAuthenticatorMockUp
            extends MockUp<KerberosAuthenticator>
    {
        @Mock
        public void $init() {}

        @Mock
        public void login() {}

        @Mock
        public Principal doAuthenticateFilter(KerberosTokenCredentials credentials)
        {
            return new KerberosPrincipal("hetu@kerberos.com");
        }

        @Mock
        public KerberosTokenCredentials generateServiceToken()
        {
            byte[] tokenBytes = new byte[0];
            return new KerberosTokenCredentials(Base64.getEncoder().encode(tokenBytes));
        }

        @Mock
        public String getPrincipalFullName()
        {
            return "hetu@kerberos.com";
        }
    }

    @BeforeSuite
    public void setup()
    {
        new KerberosAuthenticatorMockUp();
        KerberosConfig.setKerberosEnabled(true);
        String clusterName = "cluster-" + UUID.randomUUID();

        Config config1 = new Config();
        config1.setClusterName(clusterName);
        NetworkConfig network1 = config1.getNetworkConfig();
        network1.setPortAutoIncrement(false);
        network1.setPort(PORT1);
        config1.getSecurityConfig().setEnabled(true);
        hazelcastInstance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setClusterName(clusterName);
        NetworkConfig network2 = config2.getNetworkConfig();
        network2.setPortAutoIncrement(false);
        network2.setPort(PORT2);
        config2.getSecurityConfig().setEnabled(true);
        hazelcastInstance2 = Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientInstance = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @AfterSuite
    private void tearDown()
    {
        hazelcastInstance1.shutdown();
        hazelcastInstance2.shutdown();
        clientInstance.shutdown();
    }

    @Test
    public void testHazelcastAuthenticationEnabled()
    {
        String value1 = "aaa";
        String value2 = "bbb";
        String value3 = "ccc";

        Map<Integer, String> clusterMap1 = hazelcastInstance1.getMap("MyMap");
        clusterMap1.put(1, value1);

        Map<Integer, String> clusterMap2 = hazelcastInstance2.getMap("MyMap");
        clusterMap2.put(2, value2);

        Map<Integer, String> clientMap = clientInstance.getMap("MyMap");
        clientMap.put(3, value3);
        assertEquals(clientMap.get(1), value1);
        assertEquals(clientMap.get(2), value2);
        assertEquals(clientMap.get(3), value3);
    }
}
