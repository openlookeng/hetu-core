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

package io.hetu.core.plugin.datacenter.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.json.JsonCodec;
import io.hetu.core.plugin.datacenter.DataCenterColumn;
import io.hetu.core.plugin.datacenter.DataCenterColumnHandle;
import io.hetu.core.plugin.datacenter.DataCenterConfig;
import io.hetu.core.plugin.datacenter.DataCenterTable;
import io.hetu.core.plugin.datacenter.GlobalQueryIdGenerator;
import io.prestosql.client.DataCenterClientSession;
import io.prestosql.client.DataCenterStatementClient;
import io.prestosql.client.JsonResponse;
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryStatusInfo;
import io.prestosql.client.StatementClient;
import io.prestosql.client.util.HttpUtil;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoTransportException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.inject.Inject;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.client.util.TypeUtil.parseType;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static io.prestosql.spi.type.DateType.DATE;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

/**
 * Data center client, it is for get help data center plugin to get the metadata and issue query.
 *
 * @since 2020-02-11
 */
public class DataCenterClient
{
    private static final JsonCodec<Integer> INTEGER_JSON_CODEC = jsonCodec(Integer.class);

    private static final String EXCHANGE_COMPRESSION = "exchange_compression";

    private static final String SPLIT_DOT = ".";

    private static final int TYPE_POSITION = 4;

    private static final int DEFAULT_SPLIT_COUNT = 1;

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final HttpUrl serverUri;

    private final DataCenterClientSession clientSession;

    private final GlobalQueryIdGenerator globalQueryIdGenerator;

    private final DataCenterConfig config;

    private final OkHttpClient httpClient;

    private TypeManager typeManager;

    /**
     * Constructor of data center client.
     *
     * @param config data center config.
     * @param httpClient http client.
     * @param typeManager the type manager.
     */
    @Inject
    public DataCenterClient(DataCenterConfig config, OkHttpClient httpClient, TypeManager typeManager)
    {
        Map<String, String> properties = new HashMap<>();
        this.config = requireNonNull(config, "config is null");

        this.serverUri = HttpUrl.get(config.getConnectionUrl());
        if (this.serverUri == null) {
            throw new RuntimeException("Invalid connect-url :" + config.getConnectionUrl().toString());
        }
        if (config.isCompressionEnabled()) {
            properties.put(EXCHANGE_COMPRESSION, "true");
        }
        this.clientSession = DataCenterStatementClientFactory.createClientSession(config, typeManager, properties);
        this.httpClient = httpClient;
        this.globalQueryIdGenerator = new GlobalQueryIdGenerator(Optional.ofNullable(config.getRemoteClusterId()));
        this.typeManager = typeManager;
    }

    private static SQLException resultsException(QueryStatusInfo results)
    {
        QueryError error = requireNonNull(results.getError());
        String message = format("Query failed (#%s): %s", results.getId(), error.getMessage());
        Throwable cause = (error.getFailureInfo() == null) ? null : error.getFailureInfo().toException();
        return new SQLException(message, error.getSqlState(), error.getErrorCode(), cause);
    }

    /**
     * Get catalogs from the remote data center.
     *
     * @return the catalogs
     */
    public Set<String> getCatalogNames()
    {
        String query = "SELECT * FROM SYSTEM.METADATA.CATALOGS";
        try {
            Set<String> catalogNames = new HashSet<>();
            Iterable<List<Object>> data = getResults(clientSession, query);
            for (List<Object> row : data) {
                String catalogName = row.get(0).toString();
                // Skip remote dc catalogs to avoid cyclic dependency
                if (!catalogName.contains(SPLIT_DOT)) {
                    catalogNames.add(catalogName);
                }
            }
            return catalogNames;
        }
        catch (SQLException ex) {
            throw new PrestoTransportException(REMOTE_TASK_ERROR, HostAddress.fromUri(this.serverUri.uri()),
                    "could not connect to the data center");
        }
    }

    /**
     * Get schema names from the given catalog
     *
     * @param catalog catalog name.
     * @return schemas from remote data center's catalog
     */
    public Set<String> getSchemaNames(String catalog)
    {
        requireNonNull(catalog, "catalog is null");
        String query = "SHOW SCHEMAS FROM " + catalog;
        try {
            Iterable<List<Object>> data = getResults(clientSession, query);
            Set<String> schemaNames = new HashSet<>();
            for (List<Object> row : data) {
                String schemaName = row.get(0).toString();
                if (!"information_schema".equals(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames;
        }
        catch (SQLException ex) {
            throw new PrestoException(NOT_FOUND, catalog + " not found, failed to get schema names");
        }
    }

    /**
     * Get table names from the remote data center.
     *
     * @param catalog catalog name.
     * @param schema schema name.
     * @return tables form remote data center's schema
     */
    public Set<String> getTableNames(String catalog, String schema)
    {
        String query = "SHOW TABLES FROM " + catalog + SPLIT_DOT + schema;
        try {
            Iterable<List<Object>> data = getResults(clientSession, query);
            Set<String> tableNames = new HashSet<>();
            for (List<Object> row : data) {
                tableNames.add(row.get(0).toString());
            }
            return tableNames;
        }
        catch (SQLException ex) {
            throw new SchemaNotFoundException(catalog + SPLIT_DOT + schema, "Hetu DC connector failed to get table name");
        }
    }

    /**
     * Get table metadata.
     *
     * @param catalog catalog name.
     * @param schema schema name.
     * @param tableName table name.
     * @return table information from remote data center's catalog.schema.tableName.
     */
    public DataCenterTable getTable(String catalog, String schema, String tableName)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        String query = "SELECT * FROM " + catalog + SPLIT_DOT + schema + SPLIT_DOT + tableName;

        List<DataCenterColumn> columns = getColumns(query);
        if (columns.isEmpty()) {
            return null;
        }
        return new DataCenterTable(tableName, columns);
    }

    /**
     * Get the columns information of this sql.
     *
     * @param sql statement.
     * @return the list of data center column that this sql contains.
     */
    public List<DataCenterColumn> getColumns(String sql)
    {
        String query = "PREPARE subStatement FROM " + sql;
        String describe = "DESCRIBE OUTPUT subStatement";
        try (StatementClient preparedClient = execute(query)) {
            DataCenterClientSession newClientSession = DataCenterStatementClientFactory.createClientSession(
                    preparedClient, this.config, this.typeManager);

            ImmutableList.Builder<DataCenterColumn> builder = new ImmutableList.Builder<>();
            Iterable<List<Object>> data = getResults(newClientSession, describe);
            for (List<Object> row : data) {
                String name = row.get(0).toString();
                String type = row.get(TYPE_POSITION).toString();
                if ("unknown".equalsIgnoreCase(type)) {
                    // Cannot support unknown types
                    return Collections.emptyList();
                }
                try {
                    builder.add(new DataCenterColumn(name, parseType(typeManager, type)));
                }
                catch (IllegalArgumentException ex) {
                    return Collections.emptyList();
                }
            }
            return builder.build();
        }
        catch (SQLException ex) {
            return Collections.emptyList();
        }
    }

    /**
     * Get remote table statistics.
     *
     * @param tableFullName the fully qualified table name
     * @param columnHandles data center column handles
     * @return the table statistics
     */
    public TableStatistics getTableStatistics(String tableFullName, Map<String, ColumnHandle> columnHandles)
    {
        String query = "SHOW STATS FOR " + tableFullName;
        Iterable<List<Object>> data;
        try {
            data = getResults(clientSession, query);
        }
        catch (SQLException ex) {
            throw new PrestoTransportException(REMOTE_TASK_ERROR, HostAddress.fromUri(this.serverUri.uri()),
                    "could not connect to the remote data center");
        }
        TableStatistics.Builder builder = TableStatistics.builder();
        List<Object> lastRow = null;
        for (List<Object> row : data) {
            ColumnStatistics.Builder columnStatisticBuilder = new ColumnStatistics.Builder();
            lastRow = row;
            if (row.get(0) == null) {
                // Only the last row can have the first column (column name) null
                continue;
            }
            // row[0] is column_name
            DataCenterColumnHandle columnHandle = (DataCenterColumnHandle) columnHandles.get(row.get(0).toString());
            if (columnHandle == null) {
                // Unknown column found
                continue;
            }
            // row[1] is data_size
            if (row.get(1) != null) {
                columnStatisticBuilder.setDataSize(Estimate.of(Double.parseDouble(row.get(1).toString())));
            }
            // row[2] is distinct_values_count
            if (row.get(2) != null) {
                columnStatisticBuilder.setDistinctValuesCount(Estimate.of(Double.parseDouble(row.get(2).toString())));
            }
            // row[3] is nulls_fraction
            if (row.get(3) != null) {
                columnStatisticBuilder.setNullsFraction(Estimate.of(Double.parseDouble(row.get(3).toString())));
            }
            // row[4] is row_count. Only the last row has a valid value. Others have null
            // row[5] is low_value and row[6] is high_value
            if (row.get(5) != null && row.get(6) != null) {
                String minStr = row.get(5).toString();
                String maxStr = row.get(6).toString();
                Type columnType = columnHandle.getColumnType();
                if (columnType.equals(DATE)) {
                    LocalDate minDate = LocalDate.parse(minStr, DATE_FORMATTER);
                    LocalDate maxDate = LocalDate.parse(maxStr, DATE_FORMATTER);
                    columnStatisticBuilder.setRange(new DoubleRange(minDate.toEpochDay(), maxDate.toEpochDay()));
                }
                else {
                    columnStatisticBuilder.setRange(new DoubleRange(Double.parseDouble(minStr), Double.parseDouble(maxStr)));
                }
            }
            builder.setColumnStatistics(columnHandle, columnStatisticBuilder.build());
        }
        // Get row_count from the last row
        if (lastRow != null && lastRow.get(4) != null) {
            builder.setRowCount(Estimate.of(Double.parseDouble(lastRow.get(4).toString())));
        }
        return builder.build();
    }

    /**
     * Get the split count form the server. If not possible, always return 1.
     *
     * @param globalQueryId the query id
     * @return number of splits
     */
    public int getSplits(String globalQueryId)
    {
        HttpUrl url = this.serverUri.newBuilder().encodedPath("/v1/dc/split/" + globalQueryId).build();
        Request request = HttpUtil.prepareRequest(url, this.clientSession).build();
        JsonResponse<Integer> response = JsonResponse.execute(INTEGER_JSON_CODEC, httpClient, request);
        if ((response.getStatusCode() == HTTP_OK) && response.hasValue()) {
            Integer splitCount = response.getValue();
            if (splitCount != null) {
                return splitCount;
            }
        }
        return DEFAULT_SPLIT_COUNT;
    }

    /**
     * Execute a query by client session.
     *
     * @param query statement.
     * @return a statement client instance.
     * @throws SQLException when new a statement client failed.
     */
    private StatementClient execute(String query)
            throws SQLException
    {
        try (StatementClient client = DataCenterStatementClient.newStatementClient(this.httpClient, this.clientSession,
                query, this.globalQueryIdGenerator.createId())) {
            while (client.isRunning()) {
                client.advance();
            }
            verify(client.isFinished());
            QueryStatusInfo results = client.finalStatusInfo();
            if (results.getError() != null) {
                throw resultsException(results);
            }
            return client;
        }
        catch (RuntimeException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private Iterable<List<Object>> getResults(DataCenterClientSession session, String query)
            throws SQLException
    {
        try (StatementClient client = DataCenterStatementClient.newStatementClient(this.httpClient, session,
                query, this.globalQueryIdGenerator.createId())) {
            List<Iterable<List<Object>>> list = new LinkedList<>();
            while (client.isRunning()) {
                if (client.currentData() != null) {
                    Iterable<List<Object>> data = client.currentData().getData();
                    if (data != null) {
                        list.add(data);
                    }
                }
                client.advance();
            }
            verify(client.isFinished());
            QueryStatusInfo results = client.finalStatusInfo();
            if (results.getError() != null) {
                throw resultsException(results);
            }
            return Iterables.concat(list);
        }
        catch (RuntimeException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }
}
