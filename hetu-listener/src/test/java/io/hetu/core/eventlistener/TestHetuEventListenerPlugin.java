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

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.hetu.core.eventlistener.listeners.BaseEventListener;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertSame;

public class TestHetuEventListenerPlugin
{
    @Test
    public void testCreateConnector1()
    {
        Plugin plugin = new io.hetu.core.eventlistener.HetuEventListenerPlugin();
        EventListenerFactory factory = getOnlyElement(plugin.getEventListenerFactories());
        EventListener listener = factory.create(ImmutableMap.of());
        assertSame(listener, BaseEventListener.SILENT_EVENT_LISTENER, "not silent listener");
    }

    @Test
    public void testCreateConnector2()
    {
        Plugin plugin = new io.hetu.core.eventlistener.HetuEventListenerPlugin();
        EventListenerFactory factory = getOnlyElement(plugin.getEventListenerFactories());
        EventListener listener = factory.create(
                ImmutableMap.of("hetu.event.listener.listen.query.creation", "true"));
        assertSame(listener, BaseEventListener.SILENT_EVENT_LISTENER, "not silent listener");
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void testCreateConnector3()
    {
        Plugin plugin = new io.hetu.core.eventlistener.HetuEventListenerPlugin();
        EventListenerFactory factory = getOnlyElement(plugin.getEventListenerFactories());
        EventListener listener = factory.create(
                ImmutableMap.of("hetu.event.listener.type", "unknown", "hetu.event.listener.listen.query.creation",
                        "true"));
    }

    @Test
    public void testCreateConnector4()
    {
        Plugin plugin = new io.hetu.core.eventlistener.HetuEventListenerPlugin();
        EventListenerFactory factory = getOnlyElement(plugin.getEventListenerFactories());
        EventListener listener = factory.create(
                ImmutableMap.of("hetu.event.listener.type", "LOGGER", "hetu.event.listener.listen.query.creation",
                        "true"));
    }
}
