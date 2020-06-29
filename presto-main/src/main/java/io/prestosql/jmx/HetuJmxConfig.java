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

import io.airlift.configuration.Config;

public class HetuJmxConfig
{
    private Integer rmiRegistryPort;
    private Integer rmiServerPort;
    private String rmiServerHostname;

    public Integer getRmiRegistryPort()
    {
        return rmiRegistryPort;
    }

    @Config("jmx.rmiregistry.port")
    public HetuJmxConfig setRmiRegistryPort(Integer rmiRegistryPort)
    {
        this.rmiRegistryPort = rmiRegistryPort;
        return this;
    }

    public Integer getRmiServerPort()
    {
        return rmiServerPort;
    }

    @Config("jmx.rmiserver.port")
    public HetuJmxConfig setRmiServerPort(Integer rmiServerPort)
    {
        this.rmiServerPort = rmiServerPort;

        return this;
    }

    public String getRmiServerHostname()
    {
        return rmiServerHostname;
    }

    @Config("jmx.rmiserver.hostname")
    public HetuJmxConfig setRmiServerHostname(String rmiServerHostname)
    {
        this.rmiServerHostname = rmiServerHostname;
        return this;
    }
}
