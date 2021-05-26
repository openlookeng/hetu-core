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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.hetu.core.plugin.carbondata.impl.CarbondataLocalInputSplit;
import io.hetu.core.plugin.carbondata.impl.CarbondataLocalMultiBlockSplit;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableCacheModel;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableConfig;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableReader;
import io.prestosql.plugin.hive.CoercionPolicy;
import io.prestosql.plugin.hive.DirectoryLister;
import io.prestosql.plugin.hive.ForHive;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveSplitManager;
import io.prestosql.plugin.hive.HiveSplitWrapper;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.NamenodeStats;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.type.TypeManager;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil;
import org.apache.carbondata.processing.merger.CompactionType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Build Carbontable splits
 * filtering irrelevant blocks
 */
public class CarbondataSplitManager
        extends HiveSplitManager
{
    private final CarbondataTableReader carbonTableReader;
    private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
    private final HdfsEnvironment hdfsEnvironment;
    private final CarbondataTableConfig carbondataTableConfig;
    private final CarbondataConfig carbondataConfig;
    private static final Logger LOGGER =
            LogServiceFactory.getLogService(CarbondataSplitManager.class.getName());

    @Inject
    public CarbondataSplitManager(
            HiveConfig hiveConfig,
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
            HivePartitionManager partitionManager,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            @ForHive ExecutorService executorService,
            VersionEmbedder versionEmbedder,
            TypeManager typeManager,
            CoercionPolicy coercionPolicy,
            CarbondataTableReader reader,
            CarbondataTableConfig carbondataTableConfig,
            CarbondataConfig carbondataConfig)
    {
        super(hiveConfig, metastoreProvider, partitionManager, namenodeStats, hdfsEnvironment,
                directoryLister, executorService, versionEmbedder, typeManager, coercionPolicy);
        this.carbonTableReader = requireNonNull(reader, "client is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.carbondataTableConfig = requireNonNull(carbondataTableConfig, "carbonTableConfig is null");
        this.carbondataConfig = requireNonNull(carbondataConfig, "carbondataConfig is null");
    }

    private static List<HostAddress> getHostAddresses(String[] hosts)
    {
        return Arrays.stream(hosts).map(HostAddress::fromString).collect(toImmutableList());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session, ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy, Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier,
            Optional<QueryType> queryType, Map<String, Object> queryProperties,
            Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates,
            boolean partOfReuse)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        SchemaTableName schemaTableName = hiveTable.getSchemaTableName();

        // get table metadata
        HiveIdentity identity = new HiveIdentity(session);
        SemiTransactionalHiveMetastore metastore =
                metastoreProvider.apply((HiveTransactionHandle) transactionHandle);
        Table table =
                metastore.getTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (!table.getStorage().getStorageFormat().getInputFormat().contains("carbon")) {
            throw new PrestoException(NOT_SUPPORTED, "Carbondata connector can only read carbondata tables");
        }

        return hdfsEnvironment.doAs(session.getUser(), () -> {
            String location = table.getStorage().getLocation();

            String queryId = System.nanoTime() + "";
            QueryStatistic statistic = new QueryStatistic();
            QueryStatisticsRecorder statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder();
            statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis());
            statisticRecorder.recordStatisticsForDriver(statistic, queryId);
            statistic = new QueryStatistic();

            carbonTableReader.setQueryId(queryId);
            TupleDomain<HiveColumnHandle> predicate =
                    (TupleDomain<HiveColumnHandle>) hiveTable.getCompactEffectivePredicate();
            Configuration configuration = this.hdfsEnvironment.getConfiguration(
                    new HdfsEnvironment.HdfsContext(session, schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()), new Path(location));
            // set the hadoop configuration to thread local, so that FileFactory can use it.
            ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
            CarbondataTableCacheModel cache =
                    carbonTableReader.getCarbonCache(schemaTableName, location, configuration);
            Expression filters = CarbondataHetuFilterUtil.parseFilterExpression(predicate);
            try {
                List<CarbondataLocalMultiBlockSplit> splits =
                        carbonTableReader.getInputSplits(cache, filters, predicate, configuration);

                ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
                long index = 0;
                for (CarbondataLocalMultiBlockSplit split : splits) {
                    index++;
                    Properties properties = new Properties();
                    for (Map.Entry<String, String> entry : table.getStorage().getSerdeParameters().entrySet()) {
                        properties.setProperty(entry.getKey(), entry.getValue());
                    }
                    properties.setProperty("tablePath", cache.getCarbonTable().getTablePath());
                    properties.setProperty("carbonSplit", split.getJsonString());
                    properties.setProperty("queryId", queryId);
                    properties.setProperty("index", String.valueOf(index));
                    cSplits.add(HiveSplitWrapper.wrap(new HiveSplit(schemaTableName.getSchemaName(), schemaTableName.getTableName(),
                            schemaTableName.getTableName(), cache.getCarbonTable().getTablePath(),
                            0, 0, 0, 0,
                            properties, new ArrayList(), getHostAddresses(split.getLocations()),
                            OptionalInt.empty(), false, new HashMap<>(),
                            Optional.empty(), false, Optional.empty(), Optional.empty(), false, ImmutableMap.of())));
                    /* Todo: Make this part aligned with rest of the HiveSlipt loading flow...
                     *   and figure out how to pass valid transaction Ids to CarbonData? */
                }

                statisticRecorder.logStatisticsAsTableDriver();

                statistic
                        .addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION, System.currentTimeMillis());
                statisticRecorder.recordStatisticsForDriver(statistic, queryId);
                statisticRecorder.logStatisticsAsTableDriver();
                if (queryType != null && queryType.isPresent() && queryType.get().equals(QueryType.VACUUM)) {
                    // Get Splits for compaction
                    return getSplitsForCompaction(identity,
                            transactionHandle,
                            tableHandle,
                            cache.getCarbonTable().getTablePath(),
                            queryProperties,
                            queryId,
                            cSplits,
                            configuration);
                }
                return new FixedSplitSource(cSplits.build());
            }
            catch (IOException ex) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while trying to get splits ", ex);
            }
        });
    }

    /*
     * Convert the splits into batches based on task id and wrap around ConnectorSplitSource to send back
     */
    public ConnectorSplitSource getSplitsForCompaction(HiveIdentity identity, ConnectorTransactionHandle transactionHandle, ConnectorTableHandle tableHandle,
            String tablePath, Map<String, Object> queryProperties, String queryId,
            ImmutableList.Builder<ConnectorSplit> allSplitsForComp, Configuration configuration) throws PrestoException
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        SchemaTableName schemaTableName = hiveTable.getSchemaTableName();
        List<List<LoadMetadataDetails>> allGroupedSegList;

        // Step 1: Get table handles and metadata
        SemiTransactionalHiveMetastore metaStore =
                metastoreProvider.apply((HiveTransactionHandle) transactionHandle);
        Table table = metaStore.getTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        Properties hiveSchema = MetastoreUtil.getHiveSchema(table);
        CarbonLoadModel carbonLoadModel = null;
        try {
            carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(hiveSchema, configuration);
        }
        catch (Exception e) {
            LOGGER.error("Cannot create carbon load model");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot create carbon load model");
        }
        CompactionType compactionType = (queryProperties.get("FULL") == Boolean.valueOf("true")) ? CompactionType.MAJOR : CompactionType.MINOR;

        // Step 2: Get segments to be merged based on configuration passed
        allGroupedSegList = CarbondataHetuCompactorUtil.identifyAndGroupSegmentsToBeMerged(carbonLoadModel, configuration,
                compactionType, carbondataConfig.getMajorVacuumSegSize(), carbondataConfig.getMinorVacuumSegCount());

        // All the splits are grouped based on taskIds and compaction level into one builder
        ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
        Gson gson = new Gson();
        for (List<LoadMetadataDetails> segmentsToBeMerged : allGroupedSegList) {
            String mergedLoadName = CarbonDataMergerUtil.getMergedLoadName(segmentsToBeMerged);

            // Step 3:  Get all the splits for the required segments and divide them based on task ids
            Map<String, List<CarbondataLocalInputSplit>> taskIdToSplitMapping = new HashMap<>();
            for (ConnectorSplit connectorSplit : allSplitsForComp.build()) {
                HiveSplit currSplit = ((HiveSplitWrapper) connectorSplit).getSplits().get(0);
                CarbondataLocalMultiBlockSplit currSplits =
                        gson.fromJson(currSplit.getSchema().getProperty("carbonSplit"), CarbondataLocalMultiBlockSplit.class);
                for (CarbondataLocalInputSplit split : currSplits.getSplitList()) {
                    CarbonInputSplit carbonInputSplit = CarbondataLocalInputSplit.convertSplit(split);
                    String taskId = carbonInputSplit.taskId;
                    String segmentNo = carbonInputSplit.getSegmentId();
                    for (LoadMetadataDetails load : segmentsToBeMerged) {
                        if (load.getLoadName().equals(segmentNo)) {
                            List<CarbondataLocalInputSplit> currList = taskIdToSplitMapping.computeIfAbsent(taskId, k -> new ArrayList<>());
                            currList.add(split);
                        }
                    }
                }
            }

            // Step 4: Create the ConnectorSplitSource with the splits divided and return
            long index = 0;
            for (Map.Entry<String, List<CarbondataLocalInputSplit>> splitEntry : taskIdToSplitMapping.entrySet()) {
                CarbondataLocalMultiBlockSplit currSplit = new CarbondataLocalMultiBlockSplit(splitEntry.getValue(),
                        splitEntry.getValue().stream().flatMap(f -> Arrays.stream(getLocations(f))).distinct()
                                .toArray(String[]::new));
                index++;
                Properties properties = new Properties();
                for (Map.Entry<String, String> entry : table.getStorage().getSerdeParameters().entrySet()) {
                    properties.setProperty(entry.getKey(), entry.getValue());
                }
                // TODO: Use the existing CarbondataLocalInputSplit list to convert
                properties.setProperty("tablePath", tablePath);
                properties.setProperty("carbonSplit", currSplit.getJsonString());
                properties.setProperty("queryId", queryId);
                properties.setProperty("index", String.valueOf(index));
                properties.setProperty("mergeLoadName", mergedLoadName);
                properties.setProperty("compactionType", compactionType.toString());
                properties.setProperty("taskNo", splitEntry.getKey());
                cSplits.add(HiveSplitWrapper.wrap(new HiveSplit(schemaTableName.getSchemaName(), schemaTableName.getTableName(),
                        schemaTableName.getTableName(), tablePath, 0L, 0L, 0L, 0L,
                        properties, new ArrayList(), getHostAddresses(currSplit.getLocations()),
                        OptionalInt.empty(), false, new HashMap<>(),
                        Optional.empty(), false, Optional.empty(), Optional.empty(), false, ImmutableMap.of())));
            }
        }
        LOGGER.info("Splits for compaction built and ready");
        return new FixedSplitSource(cSplits.build());
    }

    private String[] getLocations(CarbondataLocalInputSplit carbondataLocalInputSplit)
    {
        return carbondataLocalInputSplit.getLocations().toArray(new String[0]);
    }
}
