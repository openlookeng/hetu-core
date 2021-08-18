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
package io.hetu.core.plugin.carbondata.impl;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.hetu.core.plugin.carbondata.CarbondataHetuFilterUtil;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.exception.CarbonFileException;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.IndexStoreManager;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * CarbonTableReader will be a facade of these utils
 * 1:CarbonMetadata,(logic table)
 * 2:FileFactory, (physic table file)
 * 3:CarbonCommonFactory, (offer some)
 * 4:DictionaryFactory, (parse dictionary util)
 * Currently, it is mainly used to parse metadata of tables under
 * the configured carbondata-store path and filter the relevant
 * input splits with given query predicates.
 */
public class CarbondataTableReader
{
    /**
     * Logger instance
     */
    private static final Logger LOGGER =
            LogServiceFactory.getLogService(CarbondataTableReader.class.getName());
    public CarbondataTableConfig config;
    /**
     * A cache for Carbon reader, with this cache,
     * metadata of a table is only read from file system once.
     */
    private AtomicReference<Map<SchemaTableName, CarbondataTableCacheModel>> carbonCache;
    /**
     * unique query id used for query statistics
     */
    private String queryId;
    /**
     * List Of Schemas
     */
    private List<String> schemaNames = new ArrayList<>();

    @Inject
    public CarbondataTableReader(CarbondataTableConfig config)
    {
        this.config = requireNonNull(config, "CarbonTableConfig is null");
        this.carbonCache = new AtomicReference(new ConcurrentHashMap<>());
        populateCarbonProperties();
    }

    /**
     * For hetu worker node to initialize the metadata cache of a table.
     *
     * @param table the name of the table and schema.
     * @return
     */
    public CarbondataTableCacheModel getCarbonCache(SchemaTableName table, String location,
            Configuration config)
    {
        updateSchemaTables(table, config);
        CarbondataTableCacheModel carbonTableCacheModel = carbonCache.get().get(table);
        if (carbonTableCacheModel == null || !carbonTableCacheModel.isValid()) {
            return parseCarbonMetadata(table, location, config);
        }
        return carbonTableCacheModel;
    }

    /**
     * Find all the tables under the schema store path (this.carbonFileList)
     * and cache all the table names in this.tableList. Notice that whenever this method
     * is called, it clears this.tableList and populate the list by reading the files.
     */
    private void updateSchemaTables(SchemaTableName schemaTableName, Configuration config)
    {
        CarbondataTableCacheModel carbonTableCacheModel = carbonCache.get().get(schemaTableName);
        if (carbonTableCacheModel != null &&
                carbonTableCacheModel.getCarbonTable().isTransactionalTable()) {
            CarbonTable carbonTable = carbonTableCacheModel.getCarbonTable();
            try {
                String schemaFilePath = CarbonTablePath
                        .getSchemaFilePath(
                                carbonTable.getTablePath());
                FileFactory.FileType fileType = FileFactory.getFileType(schemaFilePath);

                CarbonFile carbonFile = FileFactory.getCarbonFile(schemaFilePath,
                        config);

                long latestTime = carbonFile.getLastModifiedTime();
                // Workaround for cache updation error due to round off of latestTime inside LocalCarbonFile
                if (fileType == FileFactory.FileType.LOCAL) {
                    latestTime = Files.readAttributes(new File(schemaFilePath).toPath(), BasicFileAttributes.class).lastModifiedTime().toMillis();
                }
                carbonTableCacheModel.setCurrentSchemaTime(latestTime);
                if (!carbonTableCacheModel.isValid()) {
                    // Invalidate datamaps
                    IndexStoreManager.getInstance()
                            .clearIndex(carbonTableCacheModel.getCarbonTable().getAbsoluteTableIdentifier());
                }
            }
            catch (CarbonFileException | IOException e) {
                carbonCache.get().remove(schemaTableName);
            }
        }
    }

    /**
     * Form carbon case delete table using table name & schema name
     */
    public void deleteTableFromCarbonCache(SchemaTableName table)
    {
        carbonCache.get().remove(table);
    }

    /**
     * Read the metadata of the given table
     * and cache it in this.carbonCache (CarbonTableReader cache).
     *
     * @param table name of the given table.
     * @return the CarbonTableCacheModel instance which contains all the needed metadata for a table.
     */
    private CarbondataTableCacheModel parseCarbonMetadata(SchemaTableName table, String tablePath,
            Configuration config)
    {
        try {
            CarbondataTableCacheModel cache = getValidCacheBySchemaTableName(table);
            if (cache != null) {
                return cache;
            }
            // multiple tasks can be launched in a worker concurrently. Hence need to synchronize this.
            synchronized (CarbondataTableReader.class) {
                // cache might be filled by another thread, so if filled use that cache.
                CarbondataTableCacheModel cacheModel = getValidCacheBySchemaTableName(table);
                if (cacheModel != null) {
                    return cacheModel;
                }
                // Step 1: get store path of the table and cache it.
                String schemaFilePath = CarbonTablePath.getSchemaFilePath(tablePath, config);
                // If metadata folder exists, it is a transactional table
                CarbonFile schemaFile = FileFactory.getCarbonFile(schemaFilePath, config);
                boolean isTransactionalTable = schemaFile.exists();
                org.apache.carbondata.format.TableInfo tableInfo;
                long modifiedTime = System.currentTimeMillis();
                if (isTransactionalTable) {
                    //Step 2: read the metadata (tableInfo) of the table.
                    ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator()
                    {
                        // TBase is used to read and write thrift objects.
                        // TableInfo is a kind of TBase used to read and write table information.
                        // TableInfo is generated by thrift,
                        // see schema.thrift under format/src/main/thrift for details.
                        public TBase create()
                        {
                            return new org.apache.carbondata.format.TableInfo();
                        }
                    };
                    ThriftReader thriftReader = new ThriftReader(schemaFilePath, createTBase, config);
                    thriftReader.open();
                    tableInfo = (org.apache.carbondata.format.TableInfo) thriftReader.read();
                    thriftReader.close();
                    modifiedTime = schemaFile.getLastModifiedTime();
                    FileFactory.FileType fileType = FileFactory.getFileType(schemaFilePath);
                    // Workaround for cache updation error due to round off of latesttime inside LocalCarbonFile
                    if (fileType == FileFactory.FileType.LOCAL) {
                        modifiedTime = Files.readAttributes(new File(schemaFilePath).toPath(), BasicFileAttributes.class).lastModifiedTime().toMillis();
                    }
                }
                else {
                    tableInfo = CarbonUtil.inferSchema(tablePath, table.getTableName(), false, config);
                }
                // Step 3: convert format level TableInfo to code level TableInfo
                SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
                // wrapperTableInfo is the code level information of a table in carbondata core,
                // different from the Thrift TableInfo.
                TableInfo wrapperTableInfo = schemaConverter
                        .fromExternalToWrapperTableInfo(tableInfo, table.getSchemaName(), table.getTableName(),
                                tablePath);
                wrapperTableInfo.setTransactionalTable(isTransactionalTable);
                CarbonMetadata.getInstance().removeTable(wrapperTableInfo.getTableUniqueName());
                // Step 4: Load metadata info into CarbonMetadata
                CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
                CarbonTable carbonTable = requireNonNull(CarbonMetadata.getInstance()
                        .getCarbonTable(table.getSchemaName(), table.getTableName()), "carbontable is null");
                cache = new CarbondataTableCacheModel(modifiedTime, carbonTable);
                // cache the table
                carbonCache.get().put(table, cache);
                cache.setCarbonTable(carbonTable);
            }
            return cache;
        }
        catch (IOException ex) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while reading metadata of the table: ", ex);
        }
    }

    private CarbondataTableCacheModel getValidCacheBySchemaTableName(SchemaTableName schemaTableName)
    {
        CarbondataTableCacheModel cache = carbonCache.get().get(schemaTableName);
        if (cache != null && cache.isValid()) {
            return cache;
        }
        return null;
    }

    /**
     * Get a carbon muti-block input splits
     *
     * @param tableCacheModel cached table
     * @param filters carbonData filters
     * @param constraints hetu filters
     * @param config hadoop conf
     * @return list of multiblock split
     * @throws IOException
     */
    public List<CarbondataLocalMultiBlockSplit> getInputSplits(
            CarbondataTableCacheModel tableCacheModel,
            Expression filters,
            TupleDomain<HiveColumnHandle> constraints,
            Configuration config)
            throws IOException
    {
        List<CarbondataLocalInputSplit> result = new ArrayList<>();
        List<CarbondataLocalMultiBlockSplit> multiBlockSplitList = new ArrayList<>();
        CarbonTable carbonTable = tableCacheModel.getCarbonTable();
        TableInfo tableInfo = tableCacheModel.getCarbonTable().getTableInfo();
        config.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
        String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
        config.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
        config.set(CarbonTableInputFormat.DATABASE_NAME, carbonTable.getDatabaseName());
        config.set(CarbonTableInputFormat.TABLE_NAME, carbonTable.getTableName());
        config.set("query.id", queryId);
        CarbonInputFormat.setTransactionalTable(config, carbonTable.isTransactionalTable());
        CarbonInputFormat.setTableInfo(config, carbonTable.getTableInfo());

        JobConf jobConf = new JobConf(config);
        List<PartitionSpec> filteredPartitions = new ArrayList<>();

        PartitionInfo partitionInfo = carbonTable.getPartitionInfo();
        LoadMetadataDetails[] loadMetadataDetails = null;
        if (partitionInfo != null && partitionInfo.getPartitionType() == PartitionType.NATIVE_HIVE) {
            loadMetadataDetails = SegmentStatusManager.readTableStatusFile(
                    CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()));
            filteredPartitions = findRequiredPartitions(constraints, carbonTable, loadMetadataDetails);
        }
        try {
            CarbonTableInputFormat.setTableInfo(config, tableInfo);
            CarbonTableInputFormat<Object> carbonTableInputFormat =
                    createInputFormat(jobConf, carbonTable.getAbsoluteTableIdentifier(),
                            new IndexFilter(carbonTable, filters, true), filteredPartitions);
            Job job = Job.getInstance(jobConf);
            List<InputSplit> splits = carbonTableInputFormat.getSplits(job);
            Gson gson = new Gson();
            if (splits != null && splits.size() > 0) {
                for (InputSplit inputSplit : splits) {
                    CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
                    result.add(new CarbondataLocalInputSplit(carbonInputSplit.getSegmentId(),
                            carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
                            carbonInputSplit.getLength(), Arrays.asList(carbonInputSplit.getLocations()),
                            carbonInputSplit.getNumberOfBlocklets(), carbonInputSplit.getVersion().number(),
                            carbonInputSplit.getDeleteDeltaFiles(), carbonInputSplit.getBlockletId(),
                            gson.toJson(carbonInputSplit.getDetailInfo()),
                            carbonInputSplit.getFileFormat().ordinal()));
                }
                // Use block distribution
                List<List<CarbondataLocalInputSplit>> inputSplits =
                        new ArrayList<>(result.stream().collect(Collectors.groupingBy(carbonInput -> {
                            if (FileFormat.ROW_V1.equals(carbonInput.getFileFormat())) {
                                return carbonInput.getSegmentId().concat(carbonInput.getPath())
                                        .concat(carbonInput.getStart() + "");
                            }
                            return carbonInput.getSegmentId().concat(carbonInput.getPath());
                        })).values());
                // TODO : try to optimize the below loic as it may slowdown for huge splits
                for (int j = 0; j < inputSplits.size(); j++) {
                    multiBlockSplitList.add(new CarbondataLocalMultiBlockSplit(inputSplits.get(j),
                            inputSplits.get(j).stream().flatMap(f -> Arrays.stream(getLocations(f))).distinct()
                                    .toArray(String[]::new)));
                }
                LOGGER.error("Size of MultiblockList   " + multiBlockSplitList.size());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return multiBlockSplitList;
    }

    /**
     * Returns list of partition specs to query based on the domain constraints
     *
     * @param constraints hetu filter
     * @param carbonTable carbon table
     * @throws IOException
     */
    private List<PartitionSpec> findRequiredPartitions(TupleDomain<HiveColumnHandle> constraints,
            CarbonTable carbonTable, LoadMetadataDetails[] loadMetadataDetails)
            throws IOException
    {
        Set<PartitionSpec> partitionSpecs = new HashSet<>();
        for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
            SegmentFileStore segmentFileStore = null;
            segmentFileStore =
                    new SegmentFileStore(carbonTable.getTablePath(), loadMetadataDetail.getSegmentFile());
            partitionSpecs.addAll(segmentFileStore.getPartitionSpecs());
        }
        List<String> partitionValuesFromExpression =
                CarbondataHetuFilterUtil.getPartitionFilters(carbonTable, constraints);
        return partitionSpecs.stream().filter(partitionSpec -> CollectionUtils
                .isSubCollection(partitionValuesFromExpression, partitionSpec.getPartitions()))
                .collect(Collectors.toList());
    }

    private CarbonTableInputFormat<Object> createInputFormat(Configuration conf,
            AbsoluteTableIdentifier identifier, IndexFilter dataMapFilter,
            List<PartitionSpec> filteredPartitions)
    {
        CarbonTableInputFormat<Object> format = new CarbonTableInputFormat<>();
        CarbonTableInputFormat
                .setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath()));
        CarbonTableInputFormat.setFilterPredicates(conf, dataMapFilter);
        if (filteredPartitions.size() != 0) {
            CarbonTableInputFormat.setPartitionsToPrune(conf, filteredPartitions);
        }
        return format;
    }

    private void populateCarbonProperties()
    {
        addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, config.getUnsafeMemoryInMb());
        addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION,
                config.getEnableUnsafeInQueryExecution());
        addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
                config.getEnableUnsafeColumnPage());
        addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, config.getEnableUnsafeSort());
        addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, config.getEnableQueryStatistics());
        // TODO: Support configurable
        addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "Hetu_Server");
    }

    private void addProperty(String propertyName, String propertyValue)
    {
        if (propertyValue != null) {
            CarbonProperties.getInstance().addProperty(propertyName, propertyValue);
        }
    }

    /**
     * @param cis
     * @return
     */
    private String[] getLocations(CarbondataLocalInputSplit cis)
    {
        return cis.getLocations().toArray(new String[0]);
    }

    public void setQueryId(String queryId)
    {
        this.queryId = queryId;
    }
}
