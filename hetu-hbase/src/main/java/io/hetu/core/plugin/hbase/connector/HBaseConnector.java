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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.conf.HBaseColumnProperties;
import io.hetu.core.plugin.hbase.conf.HBaseTableProperties;
import io.hetu.core.plugin.hbase.metadata.HBaseConnectorMetadata;
import io.hetu.core.plugin.hbase.query.HBasePageSinkProvider;
import io.hetu.core.plugin.hbase.query.HBasePageSourceProvider;
import io.hetu.core.plugin.hbase.split.HBaseSplitManager;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;

/**
 * HBaseConnector
 *
 * @since 2020-03-30
 */
public class HBaseConnector
        implements Connector
{
    private static final Logger LOG = Logger.get(HBaseConnector.class);

    private final HBaseConnectorMetadataFactory hbaseConnectorMetadataFactory;
    private final HBaseSplitManager hbaseSplitManager;
    private final HBasePageSinkProvider hbasePageSinkProvider;
    private final HBasePageSourceProvider hBasePageSourceProvider;
    private final Optional<ConnectorAccessControl> accessControl;
    private final HBaseTableProperties hbaseTableProperties;
    private final HBaseColumnProperties hBaseColumnProperties = HBaseColumnProperties.getInstance();

    private final ConcurrentMap<ConnectorTransactionHandle, HBaseConnectorMetadata> transactions =
            new ConcurrentHashMap<>(5);

    /**
     * HBaseConnector constructor
     *
     * @param hbaseConnectorMetadataFactory hbaseConnectorMetadataFactory
     * @param hbaseSplitManager hbaseSplitManager
     * @param hbasePageSinkProvider hbasePageSinkProvider
     * @param hBasePageSourceProvider hBasePageSourceProvider
     * @param accessControl accessControl
     * @param hbaseTableProperties hbaseTableProperties
     */
    @Inject
    public HBaseConnector(
            HBaseConnectorMetadataFactory hbaseConnectorMetadataFactory,
            HBaseSplitManager hbaseSplitManager,
            HBasePageSinkProvider hbasePageSinkProvider,
            HBasePageSourceProvider hBasePageSourceProvider,
            Optional<ConnectorAccessControl> accessControl,
            HBaseTableProperties hbaseTableProperties)
    {
        this.hbaseConnectorMetadataFactory = hbaseConnectorMetadataFactory;
        this.hbaseSplitManager = hbaseSplitManager;
        this.hbasePageSinkProvider = hbasePageSinkProvider;
        this.hBasePageSourceProvider = hBasePageSourceProvider;
        this.accessControl = accessControl;
        this.hbaseTableProperties = hbaseTableProperties;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        HBaseTransactionHandle transaction = new HBaseTransactionHandle();
        transactions.put(transaction, hbaseConnectorMetadataFactory.create());
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        ConnectorMetadata metadata = transactions.get(transactionHandle);
        Preconditions.checkArgument(metadata != null, Constants.ERROR_MESSAGE_TEMPLATE, transactionHandle);
        return metadata;
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        checkArgument(
                transactions.remove(transactionHandle) != null, Constants.ERROR_MESSAGE_TEMPLATE, transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        HBaseConnectorMetadata metadata = transactions.remove(transactionHandle);
        checkArgument(metadata != null, Constants.ERROR_MESSAGE_TEMPLATE, transactionHandle);
        metadata.rollback();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return this.hbaseSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return this.hBasePageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return this.hbasePageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return hbaseTableProperties.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return HBaseColumnProperties.getColumnProperties();
    }
}
