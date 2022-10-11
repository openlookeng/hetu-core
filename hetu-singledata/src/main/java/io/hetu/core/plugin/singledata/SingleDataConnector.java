/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata;

import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.hetu.core.plugin.singledata.optimization.SingleDataPlanOptimizerProvider;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.JdbcPageSourceProvider;
import io.prestosql.plugin.jdbc.JdbcTransactionHandle;
import io.prestosql.plugin.jdbc.TransactionScopeCachingJdbcClient;
import io.prestosql.spi.connector.CachedConnectorMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.function.ExternalFunctionHub;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.immutableEnumSet;
import static io.prestosql.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class SingleDataConnector
        implements Connector
{
    private static final Logger LOGGER = Logger.get(SingleDataConnector.class);

    private final JdbcClient jdbcClient;
    private final JdbcPageSourceProvider jdbcPageSourceProvider;
    private final Set<Procedure> procedures;
    private final LifeCycleManager lifeCycleManager;
    private final JdbcMetadataConfig jdbcMetadataConfig;
    private final Optional<ConnectorAccessControl> accessControl;
    private final SingleDataSplitManager splitManager;
    private final SingleDataPlanOptimizerProvider planOptimizerProvider;
    private final ConcurrentMap<ConnectorTransactionHandle, JdbcMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public SingleDataConnector(
            JdbcClient jdbcClient,
            JdbcPageSourceProvider jdbcPageSourceProvider,
            Set<Procedure> procedures,
            LifeCycleManager lifeCycleManager,
            JdbcMetadataConfig jdbcMetadataConfig,
            Optional<ConnectorAccessControl> accessControl,
            SingleDataSplitManager splitManager,
            SingleDataPlanOptimizerProvider planOptimizerProvider)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.jdbcPageSourceProvider = requireNonNull(jdbcPageSourceProvider, "jdbcPageSourceProvider is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.jdbcMetadataConfig = requireNonNull(jdbcMetadataConfig, "config is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.splitManager = requireNonNull(splitManager, "ShardingSphereSplitManager is null");
        this.planOptimizerProvider = requireNonNull(planOptimizerProvider, "SingleDataPlanOptimizerProvider is null");
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return jdbcPageSourceProvider;
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return planOptimizerProvider;
    }

    @Override
    public Optional<ExternalFunctionHub> getExternalFunctionHub()
    {
        return this.jdbcClient.getExternalFunctionHub();
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        JdbcTransactionHandle transaction = new JdbcTransactionHandle();
        transactions.put(transaction,
                new JdbcMetadata(new TransactionScopeCachingJdbcClient(jdbcClient), jdbcMetadataConfig.isAllowDropTable()));
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        JdbcMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        if (jdbcMetadataConfig.isMetadataCacheEnabled()) {
            return new CachedConnectorMetadata(metadata, jdbcMetadataConfig.getMetadataCacheTtl(), jdbcMetadataConfig.getMetadataCacheMaximumSize());
        }
        else {
            return metadata;
        }
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        JdbcMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            LOGGER.error(e, "Error shutting down connector");
        }
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT);
    }
}
