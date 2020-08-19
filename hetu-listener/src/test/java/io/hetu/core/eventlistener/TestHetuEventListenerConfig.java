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
import io.hetu.core.eventlistener.listeners.BaseEventListener;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHetuEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(io.hetu.core.eventlistener.HetuEventListenerConfig.class).setType(null)
                .setListenQueryCompletion(false)
                .setListenQueryCreation(false)
                .setListenSplitCompletion(false)
                .setLogFile(null)
                .setLogFileLimit(0)
                .setLogFileCount(1)
                .setAuditFile(null)
                .setAuditFileLimit(0)
                .setAuditFileCount(1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("hetu.event.listener.type",
                "LOGGER")
                .put("hetu.event.listener.listen.query.creation", "true")
                .put("hetu.event.listener.listen.query.completion", "true")
                .put("hetu.event.listener.listen.split.completion", "true")
                .put("hetu.event.listener.logger.file", "/var/hetu-events.log")
                .put("hetu.event.listener.logger.count", "10")
                .put("hetu.event.listener.logger.limit", "1024")
                .put("hetu.event.listener.audit.filecount", "10")
                .put("hetu.event.listener.audit.limit", "1024")
                .put("hetu.event.listener.audit.file", "/var/hetu-audit.log")
                .build();

        HetuEventListenerConfig expected = new HetuEventListenerConfig().setType(BaseEventListener.Type.LOGGER)
                .setListenQueryCompletion(true)
                .setListenQueryCreation(true)
                .setListenSplitCompletion(true)
                .setLogFile("/var/hetu-events.log")
                .setLogFileCount(10)
                .setLogFileLimit(1024)
                .setAuditFile("/var/hetu-audit.log")
                .setAuditFileLimit(1024)
                .setAuditFileCount(10);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappings2()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("hetu.event.listener.type",
                "AUDIT")
                .put("hetu.event.listener.listen.query.creation", "true")
                .put("hetu.event.listener.listen.query.completion", "true")
                .put("hetu.event.listener.listen.split.completion", "true")
                .put("hetu.event.listener.logger.file", "/var/hetu-events.log")
                .put("hetu.event.listener.logger.count", "10")
                .put("hetu.event.listener.logger.limit", "1024")
                .put("hetu.event.listener.audit.filecount", "10")
                .put("hetu.event.listener.audit.limit", "1024")
                .put("hetu.event.listener.audit.file", "/var/hetu-audit.log")
                .build();

        HetuEventListenerConfig expected = new HetuEventListenerConfig().setType(BaseEventListener.Type.AUDIT)
                .setListenQueryCompletion(true)
                .setListenQueryCreation(true)
                .setListenSplitCompletion(true)
                .setLogFile("/var/hetu-events.log")
                .setLogFileCount(10)
                .setLogFileLimit(1024)
                .setAuditFile("/var/hetu-audit.log")
                .setAuditFileLimit(1024)
                .setAuditFileCount(10);

        assertFullMapping(properties, expected);
    }
}
