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
package io.prestosql.testing.docker;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 *
 * Docker container configuration data object.
 */
public class DockerContainerConfig
{
    private String image;
    private List<Integer> ports;
    private List<Integer> fixedPorts;
    private Map<String, String> environment;
    private String hostname;
    private String networkName;

    public DockerContainerConfig(String image, List<Integer> ports, List<Integer> fixedPorts, Map<String, String> environment,
                                 String hostname, String networkName)
    {
        this.image = requireNonNull(image, "image is null");
        this.ports = ports;
        this.fixedPorts = fixedPorts;
        this.environment = ImmutableMap.copyOf(requireNonNull(environment, "environment is null"));
        this.hostname = hostname;
        this.networkName = requireNonNull(networkName, "network name is null");
    }

    public String getImage()
    {
        return image;
    }

    public List<Integer> getPorts()
    {
        return ports;
    }

    public List<Integer> getFixedPorts()
    {
        return fixedPorts;
    }

    public Map<String, String> getEnvironment()
    {
        return environment;
    }

    public String getHostname()
    {
        return hostname;
    }

    public String getNetworkName()
    {
        return networkName;
    }
}
