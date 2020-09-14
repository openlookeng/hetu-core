/*
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
package io.hetu.core.plugin.carbondata;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableCacheModel;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableReader;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveSplitWrapper;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;

/**
 * Provider Class for Carbondata Page Source class.
 */
public class CarbondataPageSourceProvider
        extends HivePageSourceProvider
{
    private CarbondataTableReader carbonTableReader;
    private String queryId;
    private HdfsEnvironment hdfsEnvironment;

    @Inject
    public CarbondataPageSourceProvider(
            HiveConfig hiveConfig,
            HdfsEnvironment hdfsEnvironment,
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            TypeManager typeManager,
            CarbondataTableReader carbonTableReader)
    {
        super(hiveConfig, hdfsEnvironment, cursorProviders, pageSourceFactories, typeManager, null, null);
        this.carbonTableReader = requireNonNull(carbonTableReader, "carbonTableReader is null");
        this.hdfsEnvironment = hdfsEnvironment;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        HiveSplit carbonSplit =
                Types.checkType(((HiveSplitWrapper) (split)).getSplits().get(0), HiveSplit.class, "split is not class HiveSplit");
        this.queryId = carbonSplit.getSchema().getProperty("queryId");
        if (this.queryId == null) {
            // Fall back to hive pagesource.
            return super.createPageSource(transactionHandle, session, split, table, columns);
        }

        try {
            hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, carbonSplit.getDatabase()), new Path(carbonSplit.getSchema().getProperty("tablePath")));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get file system: " + e.getMessage());
        }
        return hdfsEnvironment.doAs(session.getUser(), () -> {
            Configuration configuration = this.hdfsEnvironment.getConfiguration(
                    new HdfsEnvironment.HdfsContext(session, carbonSplit.getDatabase(), carbonSplit.getTable()),
                    new Path(carbonSplit.getSchema().getProperty("tablePath")));
//            configuration = carbonTableReader.updateS3Properties(configuration);
            CarbonTable carbonTable = getCarbonTable(carbonSplit, configuration);

            /* So that CarbonTLS can access it */
            ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);

            boolean isFullACID = isFullAcidTable(Maps.fromProperties(carbonSplit.getSchema()));
            boolean isDirectVectorFill = (carbonTableReader.config.getPushRowFilter() == null) ||
                    carbonTableReader.config.getPushRowFilter().equalsIgnoreCase("false") ||
                    columns.stream().anyMatch(c ->
                            c.getColumnName().equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID));
            return new CarbondataPageSource(
                    carbonTable, queryId, carbonSplit, columns, table, configuration,
                    isDirectVectorFill, isFullACID,
                    session.getUser(), hdfsEnvironment);
        });
    }

    /**
     * @param carbonSplit
     * @return
     */
    private CarbonTable getCarbonTable(HiveSplit carbonSplit, Configuration configuration)
    {
        CarbondataTableCacheModel tableCacheModel = carbonTableReader
                .getCarbonCache(new SchemaTableName(carbonSplit.getDatabase(), carbonSplit.getTable()),
                        carbonSplit.getSchema().getProperty("tablePath"), configuration);
        requireNonNull(tableCacheModel, "tableCacheModel should not be null");
        requireNonNull(tableCacheModel.getCarbonTable(),
                "tableCacheModel.carbonTable should not be null");
        requireNonNull(tableCacheModel.getCarbonTable().getTableInfo(),
                "tableCacheModel.carbonTable.tableInfo should not be null");
        return tableCacheModel.getCarbonTable();
    }
}
