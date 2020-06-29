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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestJmxConfig
{
    private HetuJmxConfig hetuJmxConfig;
    private int registryPort = 20000;
    private int serverPort = 20001;
    private String hostname = "localhost";

    @BeforeClass
    public void setup()
    {
        hetuJmxConfig = new HetuJmxConfig();
    }

    @Test
    public void testRegistryPort()
    {
        hetuJmxConfig.setRmiRegistryPort(registryPort);
        assertEquals(hetuJmxConfig.getRmiRegistryPort(), Integer.valueOf(registryPort));
    }

    @Test
    public void testServerPort()
    {
        hetuJmxConfig.setRmiServerPort(serverPort);
        assertEquals(hetuJmxConfig.getRmiServerPort(), Integer.valueOf(serverPort));
    }

    @Test
    public void testHostname()
    {
        hetuJmxConfig.setRmiServerHostname(hostname);
        assertEquals(hetuJmxConfig.getRmiServerHostname(), hostname);
    }
}
