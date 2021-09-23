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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.hetu.core.plugin.datacenter.client.DataCenterClient;
import io.hetu.core.plugin.datacenter.client.DataCenterStatementClientFactory;
import io.hetu.core.plugin.datacenter.optimization.DataCenterPlanOptimizer;
import io.hetu.core.plugin.datacenter.pagesource.DataCenterPageSourceProvider;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.CachedConnectorMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.TypeManager;
import okhttp3.OkHttpClient;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static io.hetu.core.plugin.datacenter.DataCenterTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

/**
 * Data center connector.
 *
 * @since 2020-02-11
 */
public class DataCenterConnector
        implements Connector
{
    private static final Logger log = Logger.get(DataCenterConnector.class);

    private final LifeCycleManager lifeCycleManager;

    private final ConnectorMetadata metadata;

    private final DataCenterSplitManager splitManager;

    private final DataCenterPageSourceProvider pageSourceProvider;

    private final DataCenterClient dataCenterClient;

    private final OkHttpClient httpClient;

    private final ConnectorPlanOptimizer planOptimizer;

    /**
     * Constructor of data center connector.
     *
     * @param lifeCycleManager data center connector life cycle manager.
     * @param dataCenterConfig data center config.
     * @param typeManager the type manager.
     */
    @Inject
    public DataCenterConnector(
            LifeCycleManager lifeCycleManager,
            DataCenterConfig dataCenterConfig,
            TypeManager typeManager,
            DataCenterPlanOptimizer planOptimizer)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.httpClient = DataCenterStatementClientFactory.newHttpClient(dataCenterConfig);
        this.dataCenterClient = new DataCenterClient(dataCenterConfig, this.httpClient, typeManager);
        this.splitManager = new DataCenterSplitManager(dataCenterConfig, this.dataCenterClient);
        this.pageSourceProvider = new DataCenterPageSourceProvider(dataCenterConfig, this.httpClient, typeManager);
        this.planOptimizer = planOptimizer;
        if (dataCenterConfig.isMetadataCacheEnabled()) {
            this.metadata = new CachedConnectorMetadata(new DataCenterMetadata(dataCenterClient, dataCenterConfig),
                    dataCenterConfig.getMetadataCacheTtl(), dataCenterConfig.getMetadataCacheMaximumSize());
        }
        else {
            this.metadata = new DataCenterMetadata(dataCenterClient, dataCenterConfig);
        }
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return new ConnectorPlanOptimizerProvider()
        {
            @Override
            public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
            {
                return ImmutableSet.of(planOptimizer);
            }

            @Override
            public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
            {
                return ImmutableSet.of();
            }
        };
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean isReadOnly)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public Collection<String> getCatalogs(String user, Map<String, String> extraCredentials)
    {
        return dataCenterClient.getCatalogNames();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
        this.httpClient.dispatcher().executorService().shutdown();
        this.httpClient.connectionPool().evictAll();
    }
}
