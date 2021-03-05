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

package io.hetu.core.plugin.datacenter.pagesource;

import io.hetu.core.plugin.datacenter.DataCenterConfig;
import io.hetu.core.plugin.datacenter.DataCenterSplit;
import io.hetu.core.plugin.datacenter.DataCenterTableHandle;
import io.hetu.core.plugin.datacenter.client.DataCenterStatementClientFactory;
import io.prestosql.client.DataCenterClientSession;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.type.TypeManager;
import okhttp3.OkHttpClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.stream.Collectors.joining;

/**
 * Data center page source provider.
 *
 * @since 2020-02-11
 */
public class DataCenterPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final int STRING_CAPACITY = 16;
    private static final String ENABLE_CROSS_REGION_DYNAMIC_FILTER = "cross_region_dynamic_filter_enabled";
    private static final String EXCHANGE_COMPRESSION = "exchange_compression";

    private final DataCenterConfig config;

    private final OkHttpClient httpClient;

    private final TypeManager typeManager;

    /**
     * Constructor of data center page source provider.
     *
     * @param config data center config.
     * @param httpClient http client.
     * @param typeManager type manager.
     */
    public DataCenterPageSourceProvider(DataCenterConfig config, OkHttpClient httpClient, TypeManager typeManager)
    {
        this.config = config;
        this.httpClient = httpClient;
        this.typeManager = typeManager;
    }

    private static String buildSql(DataCenterTableHandle tableHandler, List<ColumnHandle> columnHandles,
            OptionalLong limit)
    {
        StringBuilder sql = new StringBuilder(STRING_CAPACITY);

        String catalog = tableHandler.getCatalogName();
        String schema = tableHandler.getSchemaName();
        String table = tableHandler.getTableName();
        String columnNames = columnHandles.stream().map(ColumnHandle::getColumnName).collect(joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columnHandles.isEmpty()) {
            sql.append("null");
        }

        sql.append(" FROM ");

        if (tableHandler.getPushDownSql() == null || "".equals(tableHandler.getPushDownSql())) {
            if (!isNullOrEmpty(catalog)) {
                sql.append(catalog).append('.');
            }
            if (!isNullOrEmpty(schema)) {
                sql.append(schema).append('.');
            }

            sql.append(table);
        }
        else {
            sql.append("(").append(tableHandler.getPushDownSql()).append(") pushdown");
        }

        if (limit.isPresent()) {
            sql.append(" LIMIT ").append(limit.getAsLong());
        }
        return sql.toString();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns)
    {
        return createPageSource(transaction, session, split, table, columns, Optional.empty());
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier)
    {
        // Build the sql
        String query = buildSql((DataCenterTableHandle) table, columns, ((DataCenterTableHandle) table).getLimit());

        Map<String, String> properties = new HashMap<>();

        // Only set the session if there is any dynamic filter for this page source
        if (dynamicFilterSupplier != null && dynamicFilterSupplier.isPresent()) {
            properties.put(ENABLE_CROSS_REGION_DYNAMIC_FILTER, "true");
        }
        if (config.isCompressionEnabled()) {
            properties.put(EXCHANGE_COMPRESSION, "true");
        }
        // Create a new client session
        DataCenterClientSession clientSession = DataCenterStatementClientFactory.createClientSession(this.config,
                this.typeManager, properties);
        return new DataCenterPageSource(this.httpClient, clientSession, query, ((DataCenterSplit) split).getQueryId(),
                columns, dynamicFilterSupplier);
    }
}
