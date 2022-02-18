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
import com.google.gson.Gson;
import io.prestosql.plugin.hive.HiveACIDWriteType;
import io.prestosql.plugin.hive.HiveFileWriter;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.util.FieldSetterFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.TupleIdEnum;
import org.apache.carbondata.core.mutate.data.RowCountDetailsVO;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.CarbonHiveSerDe;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.exception.MultipleMatchingException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.TableOptionConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;

/**
 * This class implements HiveFileWriter and it creates the carbonFileWriter to write the age data
 * sent from hetu.
 */
public class CarbondataFileWriter
        implements HiveFileWriter
{
    private static final Logger LOG =
            LogServiceFactory.getLogService(CarbondataFileWriter.class.getName());
    private static final io.airlift.log.Logger AIR_LOG = io.airlift.log.Logger.get(CarbondataFileWriter.class);

    private static final String LOAD_MODEL = "mapreduce.carbontable.load.model";

    private final JobConf configuration;

    private final CarbonHiveSerDe serDe;
    private final int fieldCount;
    private final Object row;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final FieldSetterFactory.FieldSetter[] setters;
    private final Properties properties;
    private final Optional<AcidOutputFormat.Options> acidOptions;
    private final HiveACIDWriteType acidWriteType;
    private final String txnTimeStamp;
    private final String tablePath;
    private final int taskId;

    private Path outPutPath;
    private FileSinkOperator.RecordWriter recordWriter;
    private ConcurrentHashMap<String, DeleteDeltaBlockDetails> deleteDeltaDetailsMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SegmentUpdateDetails> segmentUpdateDetailMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, FileSinkOperator.RecordWriter> segmentRecordWriterMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Boolean> deleteSegmentMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> deltaPathSegmentMap = new ConcurrentHashMap<>();
    private Map<String, RowCountDetailsVO> segmentNoRowCountMapping;
    private CarbonTable carbonTable;
    private SegmentUpdateStatusManager segmentUpdateStatusManager;

    private boolean isInitDone;
    private boolean isCommitDone;

    public CarbondataFileWriter(Path paramOutPutPath, List<String> inputColumnNames, Properties properties,
                                JobConf configuration, TypeManager typeManager, Optional<AcidOutputFormat.Options> acidOptions,
                                Optional<HiveACIDWriteType> acidWriteType, OptionalInt taskId) throws SerDeException
    {
        Path localOutPutPath = paramOutPutPath;
        this.outPutPath = requireNonNull(localOutPutPath, "path is null");
        // in table creation this can be null
        if (null != properties.getProperty("location")) {
            this.outPutPath = new Path(properties.getProperty("location"));
            localOutPutPath = new Path(properties.getProperty("location"));
        }
        this.configuration = requireNonNull(configuration, "conf is null");
        this.properties = requireNonNull(properties, "Properties is null");
        this.acidOptions = acidOptions;
        this.acidWriteType = acidWriteType.isPresent() ? acidWriteType.get() : HiveACIDWriteType.INSERT;
        this.txnTimeStamp = configuration.get(CarbondataConstants.TxnBeginTimeStamp,
                Long.toString(System.currentTimeMillis()));
        this.taskId = taskId.orElseGet(() -> 0);

        AIR_LOG.debug("[carbonWriterTask] taskId: " + this.taskId + ", outputPath: " + this.outPutPath);
        try {
            if (HiveACIDWriteType.isUpdateOrDelete(this.acidWriteType)) {
                String encodedCarbonTable = configuration.get(CarbondataConstants.CarbonTable);
                this.carbonTable = (CarbonTable) ObjectSerializationUtil.convertStringToObject(encodedCarbonTable);
                LoadMetadataDetails[] loadMetadataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()));
                this.segmentUpdateStatusManager = new SegmentUpdateStatusManager(carbonTable, loadMetadataDetails);
            }
            if (HiveACIDWriteType.DELETE == this.acidWriteType) {
                this.segmentNoRowCountMapping = (Map<String, RowCountDetailsVO>) ObjectSerializationUtil.convertStringToObject(configuration.get(CarbondataConstants.SegmentNoRowCountMapping));
            }
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while converting string to object", e);
        }

        tablePath = configuration.get("table.write.path");
        String segmentId = configuration.get(CarbondataConstants.NewSegmentId);
        String encodedLoadModal = configuration.get(CarbondataConstants.EncodedLoadModel);

        List<String> columnNames = Arrays
                .asList(properties.getProperty(IOConstants.COLUMNS, "").split(CarbonCommonConstants.COMMA));
        List<Type> fileColumnTypes =
                HiveType.toHiveTypes(properties.getProperty(IOConstants.COLUMNS_TYPES, "")).stream()
                        .map(hiveType -> hiveType.getType(typeManager)).collect(toList());
        fieldCount = columnNames.size();
        serDe = new CarbonHiveSerDe();
        serDe.initialize(configuration, properties);
        tableInspector = (ArrayWritableObjectInspector) serDe.getObjectInspector();

        structFields = ImmutableList.copyOf(
                inputColumnNames.stream().map(tableInspector::getStructFieldRef)
                        .collect(toImmutableList()));

        isInitDone = false;
        isCommitDone = false;

        row = tableInspector.create();

        setters = new FieldSetterFactory.FieldSetter[structFields.size()];

        FieldSetterFactory fieldSetterFactory = new FieldSetterFactory(DateTimeZone.UTC);

        for (int i = 0; i < setters.length; i++) {
            setters[i] = fieldSetterFactory.create(tableInspector, row, structFields.get(i),
                    fileColumnTypes.get(structFields.get(i).getFieldID()));
        }

        if (this.acidWriteType == HiveACIDWriteType.INSERT || this.acidWriteType == HiveACIDWriteType.INSERT_OVERWRITE) {
            try {
                boolean compress = HiveConf.getBoolVar(configuration, COMPRESSRESULT);

                if (segmentId != null && StringUtils.isEmpty(encodedLoadModal)) {
                    recordWriter = getHiveWriter(segmentId,
                            Long.parseLong(configuration.get("carbon.outputformat.taskno", "0")));
                }
                else {
                    if (StringUtils.isNotEmpty(encodedLoadModal)) {
                        configuration.set(LOAD_MODEL, encodedLoadModal);
                    }

                    this.configuration.set(CarbondataConstants.TaskId, getTaskAttemptId(String.valueOf(this.taskId)));

                    Object writer =
                            Class.forName(MapredCarbonOutputFormat.class.getName()).getConstructor().newInstance();
                    recordWriter = ((MapredCarbonOutputFormat<?>) writer)
                            .getHiveRecordWriter(this.configuration, localOutPutPath, Text.class, compress,
                                    properties, Reporter.NULL);
                }

                isInitDone = true;
            }
            catch (Exception e) {
                LOG.error("error while initializing writer", e);
                throw new RuntimeException("writer class not found");
            }
        }
    }

    private FileSinkOperator.RecordWriter getHiveWriter(String segmentId, long taskNo) throws Exception
    {
        Path finalOutPutPath = this.outPutPath;
        Properties finalProperties = this.properties;
        JobConf finalConfiguration = this.configuration;
        boolean compress = HiveConf.getBoolVar(finalConfiguration, COMPRESSRESULT);

        CarbonLoadModel carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(finalProperties, finalConfiguration);
        carbonLoadModel.setSegmentId(segmentId);
        carbonLoadModel.setTaskNo(String.valueOf(taskNo));
        carbonLoadModel.setFactTimeStamp(Long.parseLong(txnTimeStamp));
        carbonLoadModel.setBadRecordsAction(TableOptionConstant.BAD_RECORDS_ACTION.getName() + ",force");

        CarbonTableOutputFormat.setLoadModel(finalConfiguration, carbonLoadModel);
        this.configuration.set(CarbondataConstants.TaskId, getTaskAttemptId(String.valueOf(taskNo)));

        Object writer =
                Class.forName(MapredCarbonOutputFormat.class.getName()).getConstructor().newInstance();
        return ((MapredCarbonOutputFormat<?>) writer)
                .getHiveRecordWriter(finalConfiguration, finalOutPutPath, Text.class, compress,
                        finalProperties, Reporter.NULL);
    }

    @Override
    public long getWrittenBytes()
    {
        if (isCommitDone) {
            try {
                return outPutPath.getFileSystem(configuration).getFileStatus(outPutPath).getLen();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return 0;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void appendRows(Page dataPage)
    {
        for (int position = 0; position < dataPage.getPositionCount(); position++) {
            appendRow(dataPage, position);
        }
    }

    private String getUpdateTupleIdFromRec(Page dataPage, int position)
    {
        Block tupleIdBlock = null;
        tupleIdBlock = dataPage.getBlock(dataPage.getChannelCount() - 1).getLoadedBlock();

        return tupleIdBlock.getString(position, 0, 0);
    }

    public void appendRow(Page dataPage, int position)
    {
        FileSinkOperator.RecordWriter finalRecordWriter = null;
        if (HiveACIDWriteType.isUpdateOrDelete(acidWriteType)) {
            try {
                DeleteDeltaBlockDetails deleteDeltaBlockDetails = null;
                SegmentUpdateDetails segmentUpdateDetails = null;

                String tupleId = getUpdateTupleIdFromRec(dataPage, position);

                String blockId = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.BLOCK_ID);
                String blockletId = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.BLOCKLET_ID);
                String pageId = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.PAGE_ID);
                String rowId = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.OFFSET);
                String segmentId = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID);
                String segmentBlockId = CarbonUpdateUtil.getSegmentWithBlockFromTID(tupleId, false);

                String blockName = CarbonUpdateUtil.getBlockName(CarbonTablePath.addDataPartPrefix(blockId));
                String completeBlockName = CarbonTablePath.addDataPartPrefix(blockId + CarbonCommonConstants.FACT_FILE_EXT);
                String blockPath = CarbonUpdateUtil.getTableBlockPath(tupleId, tablePath, true);
                String deltaPath = CarbonUpdateUtil.getDeleteDeltaFilePath(blockPath, blockName, txnTimeStamp);

                deleteDeltaBlockDetails = deleteDeltaDetailsMap.computeIfAbsent(deltaPath,
                        v -> new DeleteDeltaBlockDetails(blockName));

                deltaPathSegmentMap.put(deltaPath, segmentId);

                segmentUpdateDetails = segmentUpdateDetailMap.computeIfAbsent(segmentBlockId,
                        v -> new SegmentUpdateDetails() {{
                                setSegmentName(segmentId);
                                setBlockName(blockName);
                                setActualBlockName(completeBlockName);
                                setDeleteDeltaEndTimestamp(txnTimeStamp);
                                setDeleteDeltaStartTimestamp(txnTimeStamp);
                                setDeletedRowsInBlock(segmentUpdateStatusManager.getDetailsForABlock(segmentBlockId) != null ?
                                        segmentUpdateStatusManager.getDetailsForABlock(segmentBlockId).getDeletedRowsInBlock() : "0");
                                }});

                Long deletedRows = Long.parseLong(segmentUpdateDetails.getDeletedRowsInBlock()) + 1;
                segmentUpdateDetails.setDeletedRowsInBlock(
                        Long.toString(deletedRows));

                if (!deleteDeltaBlockDetails.addBlocklet(blockletId, rowId, Integer.parseInt(pageId))) {
                    LOG.error("Multiple input rows matched for same row!");
                    throw new MultipleMatchingException("Multiple input rows matched for same row!");
                }

                if (HiveACIDWriteType.DELETE == acidWriteType) {
                    return;
                }

                finalRecordWriter = segmentRecordWriterMap.computeIfAbsent(segmentId, v ->
                {
                    try {
                        return getHiveWriter(segmentId, CarbonUpdateUtil.getLatestTaskIdForSegment(new Segment(segmentId), tablePath) + 1);
                    }
                    catch (Exception e) {
                        LOG.error("error while getting Carbon :: hiveRecordWriter", e);
                        throw new RuntimeException("error while getting Carbon :: hiveRecordWriter");
                    }
                });
            }
            catch (Exception e) {
                LOG.error("error while initializing writer", e);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "writer class not found", e);
            }
        }
        else {
            finalRecordWriter = this.recordWriter;
        }

        for (int field = 0; field < fieldCount; field++) {
            Block block = dataPage.getBlock(field);
            if (block.isNull(position)) {
                tableInspector.setStructFieldData(row, structFields.get(field), null);
            }
            else {
                setters[field].setField(block, position);
            }
        }

        try {
            if (finalRecordWriter != null) {
                finalRecordWriter.write(serDe.serialize(row, tableInspector));
            }
        }
        catch (SerDeException | IOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public void commit()
    {
        try {
            if (HiveACIDWriteType.isUpdateOrDelete(acidWriteType)) {
                // TODO For partitioned tables
                if (HiveACIDWriteType.DELETE == acidWriteType) {
                    for (String segmentBlockId : segmentUpdateDetailMap.keySet()) {
                        SegmentUpdateDetails segmentUpdateDetails = segmentUpdateDetailMap.get(segmentBlockId);
                        RowCountDetailsVO rowCountDetailsVO = segmentNoRowCountMapping.get(segmentUpdateDetails.getSegmentName());
                        Long totalRowsInBlock = rowCountDetailsVO.getTotalNumberOfRows();
                        Long totalDeletedRowsInBlock = Long.parseLong(segmentUpdateDetails.getDeletedRowsInBlock());
                        boolean isSegmentDelete = totalRowsInBlock.equals(totalDeletedRowsInBlock);
                        if (isSegmentDelete) {
                            deleteSegmentMap.put(segmentUpdateDetails.getSegmentName(), true);
                            segmentUpdateDetailMap.get(segmentBlockId).setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
                        }
                    }
                }
                deleteDeltaDetailsMap.forEach((k, v) -> {
                    try {
                        if (deleteSegmentMap.get(deltaPathSegmentMap.get(k)) == null) {
                            (new CarbonDeleteDeltaWriterImpl(k)).write(v);
                        }
                    }
                    catch (IOException e) {
                        LOG.error("Error while writing the deleteDeltas ", e);
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while writing the deleteDeltas", e);
                    }
                });
            }

            if (recordWriter != null) {
                recordWriter.close(false);
            }

            segmentRecordWriterMap.forEach((k, v) -> {
                try {
                    v.close(false);
                }
                catch (IOException e) {
                    LOG.error("Error while closing the record writer", e);
                    throw new RuntimeException(e);
                }
            });
        }
        catch (Exception ex) {
            LOG.error("Error while closing the record writer", ex);
            throw new RuntimeException(ex);
        }
        isCommitDone = true;
    }

    @Override
    public void rollback()
    {
        return;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public ImmutableList<String> getExtraPartitionFiles()
    {
        ImmutableList.Builder<String> deltaFiles = ImmutableList.builder();

        deleteDeltaDetailsMap.forEach((deltaFile, v) -> deltaFiles.add(deltaFile));

        return deltaFiles.build();
    }

    @Override
    public ImmutableList<String> getMiscData()
    {
        ImmutableList.Builder<String> listBuilder = ImmutableList.builder();

        Gson gsonObjectToWrite = new Gson();
        segmentUpdateDetailMap.forEach((k, sud) ->
                listBuilder.add(StringEscapeUtils.escapeJson(gsonObjectToWrite.toJson(sud, SegmentUpdateDetails.class))));

        return listBuilder.build();
    }

    private String getTaskAttemptId(String taskId)
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        String jobTrackerId = formatter.format(new Date());
        TaskAttemptID taskAttemptID = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, Integer.parseInt(taskId), 0);
        return taskAttemptID.toString();
    }
}
