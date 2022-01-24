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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.hetu.core.plugin.carbondata.impl.CarbondataLocalMultiBlockSplit;
import io.hetu.core.plugin.carbondata.readers.HetuCoreVectorBlockBuilder;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.stream.StreamRecordReader;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Carbondata Page Source class for custom Carbondata RecordSet Iteration.
 */
public class CarbondataPageSource
        implements ConnectorPageSource
{
    private static final Logger LOGGER =
            LogServiceFactory.getLogService(CarbondataPageSource.class.getName());
    private final HdfsEnvironment hdfsEnvironment;
    private final String user;
    ConnectorTableHandle tableHandle;
    private HiveSplit split;
    private CarbonTable carbonTable;
    private String queryId;
    private Configuration hadoopConf;
    private FileFormat fileFormat;
    private List<ColumnHandle> columnHandles;
    private int columnCount;
    private boolean closed;
    private long sizeOfData;
    private int batchId;
    private long nanoStart;
    private long nanoEnd;
    private HetuCarbondataReadSupport readSupport;

    // columnar format split
    private HetuCarbondataVectorizedRecordReader vectorReader;
    private boolean isDirectVectorFill;

    // row format split
    private StreamRecordReader rowReader;
    private StructField[] fields;
    private int batchSize = 100;
    private DataType[] dataTypes;
    private boolean isFrstPage = true;
    private boolean isFullAcidTable;
    boolean hasAcidFields;

    CarbondataPageSource(CarbonTable carbonTable, String queryId, HiveSplit split,
                         List<ColumnHandle> columnHandles, ConnectorTableHandle tableHandle, Configuration hadoopConf,
                         boolean isDirectVectorFill, boolean isFullAcidTable,
                         String user, HdfsEnvironment hdfsEnvironment)
    {
        this.carbonTable = carbonTable;
        this.queryId = queryId;
        this.split = split;
        this.columnHandles = columnHandles;
        this.hadoopConf = hadoopConf;
        this.isDirectVectorFill = isDirectVectorFill;
        this.tableHandle = tableHandle;
        this.isFullAcidTable = isFullAcidTable;
        hasAcidFields = false;
        this.user = user;
        this.hdfsEnvironment = hdfsEnvironment;
        initialize();
    }

    private void initialize()
    {
        CarbonMultiBlockSplit carbonInputSplit = CarbondataLocalMultiBlockSplit
                .convertSplit(split.getSchema().getProperty("carbonSplit"));
        fileFormat = carbonInputSplit.getFileFormat();
        if (fileFormat.ordinal() == FileFormat.ROW_V1.ordinal()) {
            initializeForRow();
        }
        else {
            initializeForColumnar();
        }
    }

    private void initializeForColumnar()
    {
        readSupport = new HetuCarbondataReadSupport();
        vectorReader =
                createReaderForColumnar(split, columnHandles, tableHandle, readSupport, hadoopConf);
    }

    private void initializeForRow()
    {
        QueryModel queryModel = createQueryModel(split, tableHandle, columnHandles, hadoopConf);
        rowReader = new StreamRecordReader(queryModel, false);
        List<ProjectionDimension> queryDimension = queryModel.getProjectionDimensions();
        List<ProjectionMeasure> queryMeasures = queryModel.getProjectionMeasures();
        fields = new StructField[queryDimension.size() + queryMeasures.size()];
        for (int i = 0; i < queryDimension.size(); i++) {
            ProjectionDimension dim = queryDimension.get(i);
            if (dim.getDimension().isComplex()) {
                fields[dim.getOrdinal()] =
                        new StructField(dim.getColumnName(), dim.getDimension().getDataType());
            }
            else if (dim.getDimension().getDataType() == DataTypes.DATE) {
                DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
                        .getDirectDictionaryGenerator(dim.getDimension().getDataType());
                fields[dim.getOrdinal()] = new StructField(dim.getColumnName(), generator.getReturnType());
            }
            else {
                fields[dim.getOrdinal()] =
                        new StructField(dim.getColumnName(), dim.getDimension().getDataType());
            }
        }

        for (ProjectionMeasure msr : queryMeasures) {
            DataType dataType = msr.getMeasure().getDataType();
            if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.SHORT || dataType == DataTypes.INT
                    || dataType == DataTypes.LONG) {
                fields[msr.getOrdinal()] =
                        new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
            }
            else if (DataTypes.isDecimal(dataType)) {
                fields[msr.getOrdinal()] =
                        new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
            }
            else {
                fields[msr.getOrdinal()] = new StructField(msr.getColumnName(), DataTypes.DOUBLE);
            }
        }

        this.columnCount = columnHandles.size();
        readSupport = new HetuCarbondataReadSupport();
        readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
        this.dataTypes = readSupport.getDataTypes();
    }

    @Override
    public long getCompletedBytes()
    {
        return sizeOfData;
    }

    @Override
    public long getReadTimeNanos()
    {
        return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        return hdfsEnvironment.doAs(user, () -> {
            if (fileFormat.ordinal() == FileFormat.ROW_V1.ordinal()) {
                return getNextPageForRow();
            }

            return getNextPageForColumnar();
        });
    }

    private Page getNextPageForColumnar()
    {
        if (nanoStart == 0) {
            nanoStart = System.nanoTime();
        }
        CarbondataVectorBatch columnarBatch = null;
        int columnBatchSize = 0;
        try {
            batchId++;
            if (vectorReader.nextKeyValue()) {
                Object vectorBatch = vectorReader.getCurrentValue();
                if (vectorBatch instanceof CarbondataVectorBatch) {
                    columnarBatch = (CarbondataVectorBatch) vectorBatch;
                    columnBatchSize = columnarBatch.numRows();
                    if (columnBatchSize == 0) {
                        close();
                        return null;
                    }
                }
            }
            else {
                close();
                return null;
            }
            if (columnarBatch == null) {
                return null;
            }

            Block[] blocks = new Block[columnHandles.size()];
            for (int column = 0; column < blocks.length; column++) {
                blocks[column] = new LazyBlock(columnBatchSize, new CarbondataBlockLoader(column));
            }
            Page page = new Page(columnBatchSize, blocks);
            return page;
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new CarbonDataLoadingException("Exception when creating the Carbon data Block", e);
        }
    }

    private Page getNextPageForRow()
    {
        if (isFrstPage) {
            isFrstPage = false;
            initialReaderForRow();
        }

        if (nanoStart == 0) {
            nanoStart = System.nanoTime();
        }
        int count = 0;
        try {
            Block[] blocks = new Block[columnCount];
            CarbonColumnVectorImpl[] columns = new CarbonColumnVectorImpl[columnCount];
            for (int i = 0; i < columnCount; ++i) {
                columns[i] = CarbondataVectorBatch
                        .createDirectStreamReader(batchSize, dataTypes[i], fields[i], -1, false);
            }

            while (rowReader.nextKeyValue()) {
                Object[] values = (Object[]) rowReader.getCurrentValue();
                for (int index = 0; index < columnCount; index++) {
                    columns[index].putObject(count, values[index]);
                }
                count++;
                if (count == batchSize) {
                    break;
                }
            }
            if (count == 0) {
                close();
                return null;
            }
            else {
                for (int index = 0; index < columnCount; index++) {
                    blocks[index] = ((HetuCoreVectorBlockBuilder) columns[index]).buildBlock();
                    sizeOfData += blocks[index].getSizeInBytes();
                }
            }
            return new Page(count, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException | IOException e) {
            closeWithSuppression(e);
            throw new CarbonDataLoadingException("Exception when reading the Carbon data Block", e);
        }
    }

    private void initialReaderForRow()
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        String jobTrackerId = formatter.format(new Date());
        TaskAttemptID attemptId = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, 0, 0);
        TaskAttemptContextImpl attemptContext =
                new TaskAttemptContextImpl(FileFactory.getConfiguration(), attemptId);
        CarbonMultiBlockSplit carbonInputSplit = CarbondataLocalMultiBlockSplit
                .convertSplit(split.getSchema().getProperty("carbonSplit"));
        try {
            rowReader.initialize(carbonInputSplit, attemptContext);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return sizeOfData;
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (vectorReader != null) {
                vectorReader.close();
            }
            if (rowReader != null) {
                rowReader.close();
            }
            nanoEnd = System.nanoTime();
        }
        catch (IOException e) {
            Throwables.throwIfUnchecked(e);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            LOGGER.error(e.getMessage(), e);
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    /**
     * Create vector reader using the split.
     */
    private HetuCarbondataVectorizedRecordReader createReaderForColumnar(HiveSplit carbonSplit,
                                                                         List<? extends ColumnHandle> columns, ConnectorTableHandle tableHandle,
                                                                         HetuCarbondataReadSupport readSupport, Configuration conf)
    {
        QueryModel queryModel = createQueryModel(carbonSplit, tableHandle, columns, conf);

        QueryExecutor queryExecutor =
                QueryExecutorFactory.getQueryExecutor(queryModel, new Configuration(conf));
        try {
            CarbonIterator iterator = queryExecutor.execute(queryModel);
            readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
            HetuCarbondataVectorizedRecordReader reader =
                    new HetuCarbondataVectorizedRecordReader(queryExecutor, queryModel,
                            (AbstractDetailQueryResultIterator) iterator, readSupport, (List<HiveColumnHandle>) columns);
            reader.setTaskId(Long.parseLong(carbonSplit.getSchema().getProperty("index")));
            return reader;
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create reader ", e);
        }
    }

    /**
     * @param carbondataSplit
     * @param columns
     * @return
     */
    private QueryModel createQueryModel(HiveSplit carbondataSplit, ConnectorTableHandle tableHandle,
            List<? extends ColumnHandle> columns, Configuration conf)
    {
        try {
            CarbonProjection carbonProjection = getCarbonProjection(columns);
            conf.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
            String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
            CarbonTableInputFormat
                    .setTransactionalTable(conf, carbonTable.getTableInfo().isTransactionalTable());
            CarbonTableInputFormat.setTableInfo(conf, carbonTable.getTableInfo());
            conf.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
            conf.set("query.id", queryId);
            JobConf jobConf = new JobConf(conf);
            HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
            CarbonTableInputFormat carbonTableInputFormat = createInputFormat(
                    jobConf,
                    carbonTable,
                    new IndexFilter(
                            carbonTable,
                            CarbondataHetuFilterUtil.parseFilterExpression(hiveTable.getCompactEffectivePredicate())),
                    carbonProjection);
            TaskAttemptContextImpl hadoopAttemptContext =
                    new TaskAttemptContextImpl(jobConf, new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
            CarbonMultiBlockSplit carbonInputSplit = CarbondataLocalMultiBlockSplit
                    .convertSplit(carbondataSplit.getSchema().getProperty("carbonSplit"));
            QueryModel queryModel =
                    carbonTableInputFormat.createQueryModel(carbonInputSplit, hadoopAttemptContext);
            queryModel.setQueryId(queryId);
            queryModel.setVectorReader(true);
            queryModel.setStatisticsRecorder(
                    CarbonTimeStatisticsFactory.createExecutorRecorder(queryModel.getQueryId()));

            List<TableBlockInfo> tableBlockInfoList =
                    CarbonInputSplit.createBlocks(carbonInputSplit.getAllSplits());
            queryModel.setTableBlockInfos(tableBlockInfoList);
            return queryModel;
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to get the Query Model ", e);
        }
    }

    /**
     * @param conf
     * @param carbonTable
     * @param dataMapFilter
     * @param projection
     * @return
     */
    private CarbonTableInputFormat<Object> createInputFormat(Configuration conf,
            CarbonTable carbonTable, IndexFilter dataMapFilter, CarbonProjection projection)
    {
        AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
        CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
        try {
            CarbonTableInputFormat
                    .setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath()));
            CarbonTableInputFormat
                    .setDatabaseName(conf, identifier.getCarbonTableIdentifier().getDatabaseName());
            CarbonTableInputFormat
                    .setTableName(conf, identifier.getCarbonTableIdentifier().getTableName());
        }
        catch (RuntimeException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to create the CarbonTableInputFormat", e);
        }
        CarbonTableInputFormat.setFilterPredicates(conf, dataMapFilter);
        CarbonTableInputFormat.setColumnProjection(conf, projection);

        return format;
    }

    /**
     * @param columns
     * @return
     */
    private CarbonProjection getCarbonProjection(List<? extends ColumnHandle> columns)
    {
        CarbonProjection carbonProjection = new CarbonProjection();
        // Convert all columns handles
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add(Types.checkType(handle, HiveColumnHandle.class, "handle"));
            if (((HiveColumnHandle) handle).getName()
                    .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)) {
                hasAcidFields = true;
            }

            carbonProjection.addColumn(((HiveColumnHandle) handle).getName());
        }
        return carbonProjection;
    }

    /**
     * Lazy Block Implementation for the Carbondata
     */
    private final class CarbondataBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private boolean loaded;

        CarbondataBlockLoader(int columnIndex)
        {
            this.columnIndex = columnIndex;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }
            checkState(batchId == expectedBatchId);
            try {
                vectorReader.getColumnarBatch().column(columnIndex).loadPage();
                HetuCoreVectorBlockBuilder blockBuilder =
                        (HetuCoreVectorBlockBuilder) vectorReader.getColumnarBatch().column(columnIndex);
                blockBuilder.setBatchSize(lazyBlock.getPositionCount());
                Block block = blockBuilder.buildBlock();
                sizeOfData += block.getSizeInBytes();
                lazyBlock.setBlock(block);
            }
            catch (RuntimeException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in Reading Data from Carbondata ", e);
            }
            loaded = true;
        }
    }

    public CarbonTable getCarbonTable()
    {
        return this.carbonTable;
    }

    public HdfsEnvironment getHdfsEnvironment()
    {
        return hdfsEnvironment;
    }
}
