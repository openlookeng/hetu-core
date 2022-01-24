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
package io.hetu.core.eventlistener;

import io.airlift.configuration.ConfigurationFactory;
import io.hetu.core.eventlistener.listeners.BaseEventListener;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.Map;

public class HetuEventListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "hetu-listener";
    }

    /**
     * create
     *
     * @param properties properties
     * @return event listener
     */
    @Override
    public EventListener create(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        HetuEventListenerConfig config = configurationFactory.build(HetuEventListenerConfig.class);
        return BaseEventListener.create(config);
    }
}
