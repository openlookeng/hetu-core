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

package io.hetu.core.plugin.datacenter;

import io.hetu.core.plugin.datacenter.client.DataCenterClient;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Data center split manager.
 *
 * @since 2020-02-11
 */
public class DataCenterSplitManager
        implements ConnectorSplitManager
{
    private final DataCenterClient client;

    private final GlobalQueryIdGenerator globalQueryIdGenerator;

    /**
     * Constructor of data center split manager.
     *
     * @param config data center config.
     * @param client data center client.
     */
    public DataCenterSplitManager(DataCenterConfig config, DataCenterClient client)
    {
        this.client = client;
        this.globalQueryIdGenerator = new GlobalQueryIdGenerator(Optional.ofNullable(config.getRemoteClusterId()));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorTableHandle connectorTableHandle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        String queryId = globalQueryIdGenerator.createId();
        int splitCount = this.client.getSplits(queryId);
        List<ConnectorSplit> splits = new ArrayList<>(splitCount);
        for (int i = 0; i < splitCount; i++) {
            splits.add(new DataCenterSplit(queryId));
        }

        return new FixedSplitSource(splits);
    }
}
