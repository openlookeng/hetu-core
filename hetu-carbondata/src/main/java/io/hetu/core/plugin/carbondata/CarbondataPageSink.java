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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.carbondata.impl.CarbondataLocalMultiBlockSplit;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveACIDWriteType;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSink;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveSplitWrapper;
import io.prestosql.plugin.hive.HiveWritableTableHandle;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.util.ConfigurationUtils;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.TypeManager;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.SortScopeOptions;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.merger.AbstractResultProcessor;
import org.apache.carbondata.processing.merger.CarbonCompactionExecutor;
import org.apache.carbondata.processing.merger.CarbonCompactionUtil;
import org.apache.carbondata.processing.merger.CompactionResultSortProcessor;
import org.apache.carbondata.processing.merger.CompactionType;
import org.apache.carbondata.processing.merger.RowResultMergerProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CarbondataPageSink
        extends HivePageSink
        implements ConnectorPageSink
{
    private static final Logger LOGGER =
            LogServiceFactory.getLogService(CarbondataPageSink.class.getName());
    private final Map<String, String> additionalConf;
    private final HdfsEnvironment hdfsEnvironment;
    private final ListeningExecutorService writeVerificationExecutor;
    private CarbondataSegmentInfoUtil segmentInfoData;
    private JsonCodec<CarbondataSegmentInfoUtil> segmentInfo;
    private boolean isCompactionCalled;
    List<TableBlockInfo> tableBlockInfoList;

    public CarbondataPageSink(CarbondataWriterFactory writerFactory,
            List<HiveColumnHandle> inputColumns,
            Optional<HiveBucketProperty> bucketProperty,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenWriters,
            ListeningExecutorService writeVerificationExecutor,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            JsonCodec<CarbondataSegmentInfoUtil> segmentInfo,
            ConnectorSession session,
            HiveACIDWriteType acidWriteType,
            HiveWritableTableHandle handle,
            Map<String, String> additionalConf)
    {
        super(writerFactory,
                inputColumns,
                bucketProperty,
                pageIndexerFactory,
                typeManager,
                hdfsEnvironment,
                maxOpenWriters,
                writeVerificationExecutor,
                partitionUpdateCodec,
                session,
                acidWriteType,
                handle);
        this.hdfsEnvironment = hdfsEnvironment;
        this.additionalConf = ImmutableMap.copyOf(requireNonNull(additionalConf, "Additional conf cannot be null"));
        this.writeVerificationExecutor = requireNonNull(writeVerificationExecutor, "writeVerificationExecutor is null");
        this.segmentInfo = requireNonNull(segmentInfo, "segmentInfoCodec is null");
    }

    @Override
    public VacuumResult vacuum(ConnectorPageSourceProvider pageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorTableHandle connectorTableHandle,
            List<ConnectorSplit> splits) throws PrestoException
    {
        // TODO: Convert this to page based calls
        List<HiveSplit> hiveSplits = new ArrayList<>();
        // This will get only 1 split in case of Carbondata as we merged all the same task id ones into 1 CarbonLocalMultiBlockSplit in getSplits
        splits.forEach(split -> {
            hiveSplits.addAll(((HiveSplitWrapper) split).getSplits());
        });
        List<ColumnHandle> inputColumns = new ArrayList<>(super.inputColumns);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                transactionHandle, session, splits.get(0), connectorTableHandle, inputColumns);

        try {
            performCompaction((CarbondataPageSource) pageSource, hiveSplits.get(0));
        }
        catch (PrestoException e) {
            LOGGER.error("Error in worker for performing carbon vacuum");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e.getMessage());
        }
        return new VacuumResult(null, true);
    }

    public void performCompaction(CarbondataPageSource connectorPageSource, HiveSplit split) throws PrestoException
    {
        //set flag here if called and change finish accordingly.
        isCompactionCalled = true;
        HdfsEnvironment finalHdfsEnvironment = connectorPageSource.getHdfsEnvironment();

        finalHdfsEnvironment.doAs(session.getUser(), () -> {
            try {
                // Worker part: each thread to run this code
                boolean mergeStatus = false;
                List<CarbonInputSplit> splitList = convertAndGetCarbonSplits(split);

                CarbonTable carbonTable = connectorPageSource.getCarbonTable();
                String databaseName = carbonTable.getDatabaseName();
                String factTableName = carbonTable.getTableName();

                tableBlockInfoList = CarbonInputSplit.createBlocks(splitList);
                Collections.sort(tableBlockInfoList);

                Map<String, TaskBlockInfo> segmentMapping =
                        CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList);

                Map<String, List<DataFileFooter>> dataFileMetadataSegMapping =
                        CarbonCompactionUtil.createDataFileFooterMappingForSegments(tableBlockInfoList, false);

                CarbonCompactionExecutor executor =
                        new CarbonCompactionExecutor(segmentMapping, null, carbonTable,
                                dataFileMetadataSegMapping, false, new DataTypeConverterImpl());

                // Get all the info passed from coordinator side
                String mergedLoadName = split.getSchema().getProperty("mergeLoadName");
                String taskNo = split.getSchema().getProperty("taskNo");
                String encodedLoadModel = this.additionalConf.get(CarbondataConstants.EncodedLoadModel);

                segmentInfoData = new CarbondataSegmentInfoUtil(mergedLoadName, segmentMapping.keySet());

                CarbonLoadModel carbonLoadModel = getDeserialisedLoadModel(encodedLoadModel);
                if (null == carbonLoadModel) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in deserializing load model");
                }
                carbonLoadModel.setTaskNo(taskNo);

                CompactionType compactionType = split.getSchema().getProperty("compactionType").equals(CarbondataConstants.MajorCompaction) ?
                        CompactionType.MAJOR : CompactionType.MINOR;

                String mergeNumber = mergedLoadName.substring(mergedLoadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER) +
                        CarbonCommonConstants.LOAD_FOLDER.length());
                carbonLoadModel.setSegmentId(mergeNumber);

                Map<String, List<RawResultIterator>> rawResultIteratorMap = new HashMap<>();

                AbstractResultProcessor processor = null;

                List<ColumnSchema> maxSegmentColumnSchemaList = new ArrayList<>();
                CarbonCompactionUtil.updateColumnSchema(carbonTable, maxSegmentColumnSchemaList);

                SegmentProperties segmentProperties = new SegmentProperties(maxSegmentColumnSchemaList);

                // Create temporary locations
                String baseTempPath = super.writableTableHandle.getLocationHandle().getJsonSerializableWritePath();
                String[] tempStoreLoc = getTempLocations(baseTempPath, taskNo, mergeNumber);
                Configuration initialConfiguration = ConfigurationUtils.toJobConf(connectorPageSource.getHdfsEnvironment()
                        .getConfiguration(
                                new HdfsEnvironment.HdfsContext(session, databaseName,
                                        factTableName),
                                new Path(carbonTable.getSegmentPath(mergedLoadName))));
                CarbonTableOutputFormat.setLoadModel(initialConfiguration, carbonLoadModel);
                rawResultIteratorMap = executor.processTableBlocks(initialConfiguration, null);

                // TODO: Check what sort scopes supported in future and which merger to call based on them
                if (carbonTable.getSortScope() == SortScopeOptions.SortScope.NO_SORT ||
                        rawResultIteratorMap.get(CarbonCompactionUtil.UNSORTED_IDX).size() == 0) {
                    LOGGER.info("RowResultMergerProcessor flow is selected");
                    // By default this flow will be selected to merge
                    processor = new RowResultMergerProcessor(
                            databaseName,
                            factTableName,
                            segmentProperties,
                            tempStoreLoc,
                            carbonLoadModel,
                            compactionType,
                            null);
                }
                else {
                    LOGGER.info("CompactionResultSortProcessor flow is selected");
                    processor = new CompactionResultSortProcessor(
                            carbonLoadModel,
                            carbonTable,
                            segmentProperties,
                            compactionType,
                            factTableName,
                            null);
                }

                try {
                    mergeStatus = processor
                            .execute(rawResultIteratorMap.get(CarbonCompactionUtil.UNSORTED_IDX),
                                    rawResultIteratorMap.get(CarbonCompactionUtil.SORTED_IDX));
                }
                catch (Exception e) {
                    LOGGER.error("Problem in merging");
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Carbon compaction merging didn't succeed.", e);
                }

                if (!mergeStatus) {
                    LOGGER.error("Carbon compaction merging failed in worker.");
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Carbon compaction merging failed in worker.");
                }
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Carbon compaction operation failed.");
            }
        });
    }

    private List<CarbonInputSplit> convertAndGetCarbonSplits(HiveSplit split)
    {
        CarbonMultiBlockSplit carbonInputSplit =
                CarbondataLocalMultiBlockSplit.convertSplit(split.getSchema().getProperty("carbonSplit"));
        return carbonInputSplit.getAllSplits();
    }

    private CarbonLoadModel getDeserialisedLoadModel(String encodedString) throws PrestoException
    {
        CarbonLoadModel model;
        try {
            if (encodedString != null) {
                model = (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
                return model;
            }
        }
        catch (IOException e) {
            LOGGER.error("Error getting deserialised model");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in deserializing load model", e);
        }
        return null;
    }

    private static String[] getTempLocations(String baseTempStorePath, String taskId, String segmentId)
    {
        // TODO:Currently getting the only path, check how to get multiple paths as done in carbon
        String[] baseTmpStorePathArray = new String[] {baseTempStorePath};
        String[] localDataFolderLocArray = new String[baseTmpStorePathArray.length];

        for (int i = 0; i < baseTmpStorePathArray.length; i++) {
            String tmpStore = baseTmpStorePathArray[i];
            String carbonDataDirectoryPath = CarbonTablePath.getSegmentPath(tmpStore, segmentId);

            localDataFolderLocArray[i] = carbonDataDirectoryPath + File.separator + taskId;
        }
        return localDataFolderLocArray;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (!isCompactionCalled) {
            return super.finish();
        }
        else {
            ListenableFuture<Collection<Slice>> result = hdfsEnvironment.doAs(session.getUser(), this::doFinish);
            return MoreFutures.toCompletableFuture(result);
        }
    }

    @Override
    public long getRowsWritten()
    {
        if (!isCompactionCalled) {
            return super.getRowsWritten();
        }
        else {
            long rowCount = 0;
            for (TableBlockInfo tableInfo : tableBlockInfoList) {
                rowCount += tableInfo.getDetailInfo().getRowCount();
            }
            return rowCount;
        }
    }

    private ListenableFuture<Collection<Slice>> doFinish()
    {
        ImmutableList.Builder<Slice> dataToReturn = ImmutableList.builder();

        dataToReturn.add(wrappedBuffer(segmentInfo.toJsonBytes(segmentInfoData))); //gson object in the wrapped buffer
        List<Slice> result = dataToReturn.build();
        List<Callable<Object>> verificationTasks = new ArrayList<>();

        if (verificationTasks.isEmpty()) {
            return Futures.immediateFuture(result);
        }

        try {
            List<ListenableFuture<?>> futures = writeVerificationExecutor.invokeAll(verificationTasks).stream()
                    .map(future -> (ListenableFuture<?>) future)
                    .collect(toList());
            return Futures.transform(Futures.allAsList(futures), input -> result, directExecutor());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
