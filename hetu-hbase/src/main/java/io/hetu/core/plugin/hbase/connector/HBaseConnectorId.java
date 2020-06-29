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
package io.hetu.core.plugin.hbase.connector;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * HBaseConnectorId
 *
 * @since 2020-03-30
 */
public class HBaseConnectorId
{
    private static String connectorId;

    /**
     * constructor
     *
     * @param connectorId connectorId
     */
    public static void setConnectorId(String connectorId)
    {
        HBaseConnectorId.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    public static String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public String toString()
    {
        return connectorId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (!(obj instanceof HBaseConnectorId))) {
            return false;
        }
        HBaseConnectorId other = (HBaseConnectorId) obj;
        return Objects.equals(this.connectorId, other.connectorId);
    }
}
