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
package io.hetu.core.plugin.vdm;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * hetu vdm plugin
 *
 * @since 2020-02-27
 */
public class VdmPlugin
        implements Plugin
{
    private static final String CONNECTOR_NAME = "vdm";
    private final String name;

    /**
     * vdm plugin
     */
    public VdmPlugin()
    {
        this(CONNECTOR_NAME);
    }

    /**
     * vdm plugin
     *
     * @param name vdm type
     */
    public VdmPlugin(String name)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new VdmConnectorFactory(name, VdmPlugin.class.getClassLoader()));
    }
}
