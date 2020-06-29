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
package io.prestosql.jmx;

import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestJmxAgent
{
    @Test
    public void testJmxAgent()
            throws Exception
    {
        HetuJmxConfig hetuJmxConfig = new HetuJmxConfig();
        HetuJmxAgent hetuJmxAgent = new HetuJmxAgent(hetuJmxConfig);
        assertNotNull(hetuJmxAgent);

        int registryPort = 20000;
        int serverPort = 20001;
        String hostname = "localhost";
        String url = "service:jmx:rmi://localhost:20001/jndi/rmi://localhost:20000/jmxrmi";
        Method testBuildJmxUrl = hetuJmxAgent.getClass().getDeclaredMethod("buildJmxUrl", String.class, int.class, int.class);
        testBuildJmxUrl.setAccessible(true);
        Object buildUrl = testBuildJmxUrl.invoke(hetuJmxAgent, hostname, registryPort, serverPort);
        assertEquals(buildUrl.toString(), url);

        assertNotNull(hetuJmxAgent.getUrl());
    }
}
