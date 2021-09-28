/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hbase.client;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;

import java.util.Locale;
import java.util.Optional;

/**
 * TestingConnectorSession
 *
 * @since 2020-03-20
 */
public class TestingConnectorSession
        implements ConnectorSession
{
    private final String queryId;
    private final TimeZoneKey timeZoneKey;
    private final ConnectorIdentity identity;
    private final Locale locale;

    /**
     * TestingConnectorSession constructor
     */
    public TestingConnectorSession(String user)
    {
        this.queryId = "test";
        this.timeZoneKey = TimeZoneKey.UTC_KEY;
        this.locale = Locale.ENGLISH;
        this.identity = new ConnectorIdentity(user, Optional.empty(), Optional.empty());
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public Optional<String> getSource()
    {
        return Optional.empty();
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return identity;
    }

    @Override
    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @Override
    public Locale getLocale()
    {
        return locale;
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return Optional.empty();
    }

    @Override
    public long getStartTime()
    {
        return 0;
    }

    @Override
    public <T> T getProperty(String name, Class<T> type)
    {
        T var = null;
        return Optional.ofNullable(var).orElse(var);
    }

    @Override
    public Optional<String> getCatalog()
    {
        return Optional.of("base-hbase");
    }
}
