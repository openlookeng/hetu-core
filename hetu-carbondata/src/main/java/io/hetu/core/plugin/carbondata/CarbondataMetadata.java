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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableCacheModel;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableReader;
import io.prestosql.plugin.hive.BaseStorageFormat;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveACIDWriteType;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveDeleteAsInsertTableHandle;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HiveInsertTableHandle;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveOutputTableHandle;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.plugin.hive.HiveUpdateTableHandle;
import io.prestosql.plugin.hive.HiveWriteUtils;
import io.prestosql.plugin.hive.HiveWriterFactory;
import io.prestosql.plugin.hive.HiveWrittenPartitions;
import io.prestosql.plugin.hive.LocationHandle;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.security.AccessControlMetadata;
import io.prestosql.plugin.hive.statistics.HiveStatisticsProvider;
import io.prestosql.plugin.hive.util.ConfigurationUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorDeleteAsInsertTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorUpdateTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableAlreadyExistsException;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.carbondata.common.exceptions.sql.NoSuchMVException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.data.BlockMappingVO;
import org.apache.carbondata.core.mutate.data.RowCountDetailsVO;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.hadoop.api.CarbonOutputCommitter;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.TableProcessingOperations;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil;
import org.apache.carbondata.processing.merger.CompactionType;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.processing.util.TableOptionConstant;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.hetu.core.plugin.carbondata.CarbondataConstants.EncodedLoadModel;
import static io.hetu.core.plugin.carbondata.CarbondataTableProperties.getCarbondataLocation;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.prestosql.plugin.hive.HiveTableProperties.LOCATION_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.NON_INHERITABLE_PROPERTIES;
import static io.prestosql.plugin.hive.HiveTableProperties.TRANSACTIONAL;
import static io.prestosql.plugin.hive.HiveTableProperties.getTransactionalValue;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.getPartitionKeyColumnHandles;
import static io.prestosql.plugin.hive.HiveUtil.hiveColumnHandles;
import static io.prestosql.plugin.hive.HiveUtil.toPartitionValues;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME;

public class CarbondataMetadata
        extends HiveMetadata
{
    private static final Logger LOG =
            LogServiceFactory.getLogService(CarbondataMetadata.class.getName());
    private static final String LOAD_MODEL = "mapreduce.carbontable.load.model";
    private static final String SET_OVERWRITE = "mapreduce.carbontable.set.overwrite";

    private final CarbondataTableReader carbondataTableReader;
    private final String carbondataStorageFolderName = "carbon.store";
    private final String defaultDBName = "default";
    private final String serdeTableName = "tablename";
    private final String serdeIsExternal = "isExternal";
    private final String serdePath = "path";
    private final String serdeTablePath = "tablePath";
    private final String serdeIsTransactional = "isTransactional";
    private final String serdeIsVisible = "isVisible";
    private final String serdeSerializationFormat = "serialization.format";
    private final String serdeDbName = "dbname";
    private final JsonCodec<CarbondataSegmentInfoUtil> segmentInfoCodec;

    private OutputCommitter carbonOutputCommitter;
    private JobContextImpl jobContext;
    private Optional<Table> table;
    private CarbonTable carbonTable;
    private Long timeStamp = System.currentTimeMillis();
    private ICarbonLock metadataLock;
    private ICarbonLock compactionLock;
    private ICarbonLock updateLock;
    private Configuration initialConfiguration;
    private List<SegmentUpdateDetails> blockUpdateDetailsList;
    private State currentState = State.OTHER;
    private CarbonLoadModel carbonLoadModel;
    private AbsoluteTableIdentifier absoluteTableIdentifier;
    private TableInfo tableInfo;
    private SchemaTableName schemaTableName;

    private String user;
    private Optional<String> tableStorageLocation;
    private String carbondataTableStore;
    private ICarbonLock carbonDropTableLock;
    private String schemaName;
    private String compactionType;
    private long minorVacuumSegCount;
    private long majorVacuumSegSize;
    private List<Segment> segmentFilesToBeUpdatedLatest = new ArrayList<>();
    private List<List<LoadMetadataDetails>> segmentsMerged = new ArrayList<>();
    private List<CarbondataAutoCleaner> autoCleanerTasks = new ArrayList<>();

    private static boolean enableTracingCleanupTask;
    private static ConcurrentSkipListSet<Future<?>> queuedTasks = new ConcurrentSkipListSet<>();

    private enum State {
        INSERT,
        UPDATE,
        DELETE,
        VACUUM,
        CREATE_TABLE,
        CREATE_TABLE_AS,
        DROP_TABLE,
        OTHER,
        ADD_COLUMN,
        DROP_COLUMN,
        RENAME_COLUMN
    }

    public CarbondataMetadata(SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled, boolean tableCreatesWithLocationAllowed,
            TypeManager typeManager, LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            JsonCodec<CarbondataSegmentInfoUtil> segmentInfoCodec,
            TypeTranslator typeTranslator, String hetuVersion,
            HiveStatisticsProvider hiveStatisticsProvider, AccessControlMetadata accessControlMetadata,
            CarbondataTableReader carbondataTableReader, String carbondataTableStore, long carbondataMajorVacuumSegSize, long carbondataMinorVacuumSegCount,
            ScheduledExecutorService executorService, ScheduledExecutorService hiveMetastoreClientService)
    {
        super(metastore, hdfsEnvironment, partitionManager,
                writesToNonManagedTablesEnabled, createsOfNonManagedTablesEnabled, tableCreatesWithLocationAllowed,
                typeManager, locationService, partitionUpdateCodec, typeTranslator, hetuVersion,
                hiveStatisticsProvider, accessControlMetadata, false, 2, 0.0, executorService,
                Optional.of(new Duration(5, TimeUnit.MINUTES)), hiveMetastoreClientService);
        this.carbondataTableReader = carbondataTableReader;
        this.carbondataTableStore = carbondataTableStore;
        this.metadataLock = null;
        this.carbonDropTableLock = null;
        this.schemaName = null;
        this.tableStorageLocation = Optional.empty();
        this.majorVacuumSegSize = carbondataMajorVacuumSegSize;
        this.minorVacuumSegCount = carbondataMinorVacuumSegCount;
        this.segmentInfoCodec = requireNonNull(segmentInfoCodec, "segmentInfoCodec is null");
    }

    private void setupCommitWriter(Optional<Table> table, Path outputPath, Configuration initialConfiguration, boolean isOverwrite)
    {
        Properties hiveSchema = MetastoreUtil.getHiveSchema(table.get());
        setupCommitWriter(hiveSchema, outputPath, initialConfiguration, isOverwrite);
    }

    private void setupCommitWriter(Properties hiveSchema, Path outputPath, Configuration initialConfiguration, boolean isOverwrite) throws PrestoException
    {
        CarbonLoadModel finalCarbonLoadModel;
        TaskAttemptID taskAttemptID = TaskAttemptID.forName(initialConfiguration.get("mapred.task.id"));
        try {
            ThreadLocalSessionInfo.setConfigurationToCurrentThread(initialConfiguration);
            finalCarbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(hiveSchema, initialConfiguration);
            finalCarbonLoadModel.setBadRecordsAction(TableOptionConstant.BAD_RECORDS_ACTION.getName() + ",force");
            CarbonTableOutputFormat.setLoadModel(initialConfiguration, finalCarbonLoadModel);
        }
        catch (IOException ex) {
            LOG.error("Error while creating carbon load model", ex);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while creating carbon load model", ex);
        }
        if (taskAttemptID == null) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssSSS");
            String jobTrackerId = formatter.format(new Date());
            taskAttemptID = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, 0, 0);
        }
        TaskAttemptContextImpl context =
                new TaskAttemptContextImpl(initialConfiguration, taskAttemptID);
        initialConfiguration.set(SET_OVERWRITE, String.valueOf(isOverwrite));
        try {
            carbonOutputCommitter = new CarbonOutputCommitter(outputPath, context);
            jobContext = new JobContextImpl(initialConfiguration, new JobID());
            carbonOutputCommitter.setupJob(jobContext);
            ThreadLocalSessionInfo.setConfigurationToCurrentThread(context.getConfiguration());
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error setting the output committer", e);
        }
    }

    @Override
    public CarbondataInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        boolean isInsertExistingPartitionsOverwrite = HiveSessionProperties.getInsertExistingPartitionsBehavior(session) ==
                HiveSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE ? true : false;
        return beginInsertUpdateInternal(session, tableHandle, false || isInsertExistingPartitionsOverwrite);
    }

    @Override
    public CarbondataInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, boolean isOverwrite)
    {
        return beginInsertUpdateInternal(session, tableHandle, isOverwrite);
    }

    private CarbondataInsertTableHandle beginInsertUpdateInternal(ConnectorSession session,
            ConnectorTableHandle tableHandle, boolean isOverwrite)
    {
        currentState = State.INSERT;
        HiveInsertTableHandle parent = super.beginInsert(session, tableHandle);

        this.user = session.getUser();
        return hdfsEnvironment.doAs(user, () -> {
            SchemaTableName tableName = parent.getSchemaTableName();
            Optional<Table> finalTable =
                    metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
            if (finalTable.isPresent() && finalTable.get().getPartitionColumns().size() > 0) {
                throw new PrestoException(NOT_SUPPORTED, "Operations on Partitioned CarbonTables is not supported");
            }

            this.table = finalTable;
            Path outputPath =
                    new Path(parent.getLocationHandle().getJsonSerializableTargetPath());
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                    .getConfiguration(
                            new HdfsEnvironment.HdfsContext(session, parent.getSchemaName(),
                                    parent.getTableName()),
                            new Path(parent.getLocationHandle().getJsonSerializableWritePath())));
            try {
                outputPath.getFileSystem(initialConfiguration);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get file system", e);
            }

            /* Create committer object */
            setupCommitWriter(finalTable, outputPath, initialConfiguration, isOverwrite);

            return new CarbondataInsertTableHandle(parent.getSchemaName(),
                    parent.getTableName(),
                    parent.getInputColumns(),
                    parent.getPageSinkMetadata(),
                    parent.getLocationHandle(),
                    parent.getBucketProperty(),
                    parent.getTableStorageFormat(),
                    parent.getPartitionStorageFormat(), ImmutableMap.<String, String>of(
                    EncodedLoadModel, jobContext.getConfiguration().get(LOAD_MODEL)),
                    isOverwrite);
        });
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        Optional<ConnectorOutputMetadata> connectorOutputMetadata =
                super.finishInsert(session, insertHandle, fragments, computedStatistics);
        writeSegmentFileAndSetLoadModel();
        autoCleanerTasks.add(new CarbondataAutoCleaner(table.get(), initialConfiguration, carbondataTableReader, metastore, hdfsEnvironment, user));
        return connectorOutputMetadata;
    }

    @Override
    public CarbondataUpdateTableHandle beginUpdateAsInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        currentState = State.UPDATE;
        HiveInsertTableHandle parent = super.beginInsert(session, tableHandle);
        SchemaTableName tableName = parent.getSchemaTableName();
        Optional<Table> finalTable =
                this.metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
        if (finalTable.isPresent() && finalTable.get().getPartitionColumns().size() > 0) {
            throw new PrestoException(NOT_SUPPORTED, "Operations on Partitioned CarbonTables is not supported");
        }

        this.table = finalTable;
        this.user = session.getUser();
        hdfsEnvironment.doAs(user, () -> {
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                    .getConfiguration(
                            new HdfsEnvironment.HdfsContext(session, parent.getSchemaName(),
                                    parent.getTableName()),
                            new Path(parent.getLocationHandle().getJsonSerializableWritePath())));
            Properties schema = MetastoreUtil.getHiveSchema(finalTable.get());
            schema.setProperty("tablePath", finalTable.get().getStorage().getLocation());
            carbonTable = getCarbonTable(parent.getSchemaName(),
                    parent.getTableName(),
                    schema,
                    initialConfiguration);
            takeLocks(currentState);
            try {
                CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while cleaning Delta files", e);
            }
        });
        Map<String, String> additionalConf = new HashMap<>();
        additionalConf.put(CarbondataConstants.TxnBeginTimeStamp, timeStamp.toString());
        try {
            additionalConf.put(CarbondataConstants.CarbonTable, ObjectSerializationUtil.convertObjectToString(carbonTable));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while converting objects to string", e);
        }
        return new CarbondataUpdateTableHandle(parent.getSchemaName(),
                parent.getTableName(),
                parent.getInputColumns(),
                parent.getPageSinkMetadata(),
                parent.getLocationHandle(),
                parent.getBucketProperty(),
                parent.getTableStorageFormat(),
                parent.getPartitionStorageFormat(),
                additionalConf);
    }

    @Override
    public CarbonDeleteAsInsertTableHandle beginDeletesAsInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        currentState = State.DELETE;
        HiveInsertTableHandle parent = super.beginInsert(session, tableHandle);
        List<HiveColumnHandle> inputColumns = parent.getInputColumns().stream().filter(HiveColumnHandle::isRequired).collect(toList());
        SchemaTableName tableName = parent.getSchemaTableName();
        Optional<Table> finalTable =
                this.metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
        if (finalTable.isPresent() && finalTable.get().getPartitionColumns().size() > 0) {
            throw new PrestoException(NOT_SUPPORTED, "Operations on Partitioned CarbonTables is not supported");
        }

        this.table = finalTable;
        this.user = session.getUser();
        hdfsEnvironment.doAs(user, () -> {
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                    .getConfiguration(
                            new HdfsEnvironment.HdfsContext(session, parent.getSchemaName(),
                                    parent.getTableName()),
                            new Path(parent.getLocationHandle().getJsonSerializableWritePath())));
            Properties schema = MetastoreUtil.getHiveSchema(finalTable.get());
            schema.setProperty("tablePath", finalTable.get().getStorage().getLocation());
            carbonTable = getCarbonTable(parent.getSchemaName(),
                    parent.getTableName(),
                    schema,
                    initialConfiguration);
            takeLocks(currentState);
            try {
                CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while cleaning Delta files", e);
            }
        });
        Map<String, String> additionalConf = new HashMap<>();
        initialConfiguration.set(CarbonTableInputFormat.INPUT_DIR, parent.getLocationHandle().getTargetPath().toString());
        initialConfiguration.set(CarbonTableInputFormat.DATABASE_NAME, parent.getSchemaName());
        initialConfiguration.set(CarbonTableInputFormat.TABLE_NAME, parent.getTableName());
        additionalConf.put(CarbondataConstants.TxnBeginTimeStamp, timeStamp.toString());
        try {
            Job job = new Job(initialConfiguration);
            CarbonTableInputFormat carbonTableInputFormat = new CarbonTableInputFormat();
            FileInputFormat.addInputPath((JobConf) initialConfiguration, parent.getLocationHandle().getTargetPath());
            BlockMappingVO blockMappingVO = carbonTableInputFormat.getBlockRowCount(job, carbonTable, null, true);
            LoadMetadataDetails[] loadMetadataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()));
            SegmentUpdateStatusManager segmentUpdateStatusManager = new SegmentUpdateStatusManager(carbonTable, loadMetadataDetails);
            CarbonUpdateUtil.createBlockDetailsMap(blockMappingVO, segmentUpdateStatusManager);

            Map<String, RowCountDetailsVO> segmentNoRowCountMapping = new HashMap<>();
            TreeMap<String, Long> blockRowCountMapping = new TreeMap<>();
            for (Map.Entry<String, Long> entry : blockMappingVO.getBlockRowCountMapping().entrySet()) {
                blockRowCountMapping.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, Long> entry : blockRowCountMapping.entrySet()) {
                String key = entry.getKey();
                segmentNoRowCountMapping.putIfAbsent(blockMappingVO.getBlockToSegmentMapping().get(key), blockMappingVO.getCompleteBlockRowDetailVO().get(key));
            }
            additionalConf.put(CarbondataConstants.CarbonTable, ObjectSerializationUtil.convertObjectToString(carbonTable));
            additionalConf.put(CarbondataConstants.SegmentNoRowCountMapping, ObjectSerializationUtil.convertObjectToString(segmentNoRowCountMapping));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while converting objects to string", e);
        }
        return new CarbonDeleteAsInsertTableHandle(parent.getSchemaName(),
                parent.getTableName(),
                inputColumns,
                parent.getPageSinkMetadata(),
                parent.getLocationHandle(),
                parent.getBucketProperty(),
                parent.getTableStorageFormat(),
                parent.getPartitionStorageFormat(),
                additionalConf);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishUpdateAsInsert(ConnectorSession session,
                                                                  ConnectorUpdateTableHandle updateHandle,
                                                                  Collection<Slice> fragments,
                                                                  Collection<ComputedStatistics> computedStatistics)
    {
        HiveUpdateTableHandle updateTableHandle = (HiveUpdateTableHandle) updateHandle;
        HiveInsertTableHandle insertTableHandle = new HiveInsertTableHandle(
                updateTableHandle.getSchemaName(), updateTableHandle.getTableName(),
                updateTableHandle.getInputColumns(), updateTableHandle.getPageSinkMetadata(),
                updateTableHandle.getLocationHandle(), updateTableHandle.getBucketProperty(),
                updateTableHandle.getTableStorageFormat(), updateTableHandle.getPartitionStorageFormat(), false);

        autoCleanerTasks.add(new CarbondataAutoCleaner(table.get(), initialConfiguration, carbondataTableReader, metastore, hdfsEnvironment, user));
        return finishUpdateAndDelete(session, insertTableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishDeleteAsInsert(ConnectorSession session, ConnectorDeleteAsInsertTableHandle deleteHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveDeleteAsInsertTableHandle deleteTableHandle = (HiveDeleteAsInsertTableHandle) deleteHandle;
        HiveInsertTableHandle insertTableHandle = new HiveInsertTableHandle(
                deleteTableHandle.getSchemaName(), deleteTableHandle.getTableName(),
                deleteTableHandle.getInputColumns(), deleteTableHandle.getPageSinkMetadata(),
                deleteTableHandle.getLocationHandle(), deleteTableHandle.getBucketProperty(),
                deleteTableHandle.getTableStorageFormat(), deleteTableHandle.getPartitionStorageFormat(), false);

        autoCleanerTasks.add(new CarbondataAutoCleaner(table.get(), initialConfiguration, carbondataTableReader, metastore, hdfsEnvironment, user));
        return finishUpdateAndDelete(session, insertTableHandle, fragments, computedStatistics);
    }

    @Override
    public CarbondataVacuumTableHandle beginVacuum(ConnectorSession session, ConnectorTableHandle tableHandle, boolean full, boolean merge, Optional<String> partition) throws PrestoException
    {
        if (merge) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support vacuum merge");
        }
        // Not calling carbondata's beginInsert as no need to setup committer job, just get the table handle and locks
        currentState = State.VACUUM;
        HiveInsertTableHandle insertTableHandle = super.beginInsert(session, tableHandle);
        this.user = session.getUser();
        SchemaTableName tableName = insertTableHandle.getSchemaTableName();
        this.table =
                this.metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());

        try {
            hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, table.get().getDatabaseName()), new Path(table.get().getStorage().getLocation()));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get file system" + e.getMessage());
        }
        return hdfsEnvironment.doAs(user, () -> {
            // Fill this conf here and use it in finishVacuum also
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                    .getConfiguration(
                            new HdfsEnvironment.HdfsContext(session, insertTableHandle.getSchemaName(),
                                    insertTableHandle.getTableName()),
                            new Path(insertTableHandle.getLocationHandle().getJsonSerializableWritePath())));
            Properties hiveSchema = MetastoreUtil.getHiveSchema(table.get());
            this.carbonTable = getCarbonTable(insertTableHandle.getSchemaName(), insertTableHandle.getTableName(),
                    hiveSchema, initialConfiguration);
            try {
                takeLocks(currentState);
                carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(hiveSchema, initialConfiguration);
                CarbonTableOutputFormat.setLoadModel(initialConfiguration, carbonLoadModel);
            }
            catch (IOException ex) {
                releaseLocks();
                LOG.error("Error while creating carbon load model", ex);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while creating carbon load model", ex);
            }

            try {
                //cleaning the stale delta files
                CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false);
            }
            catch (IOException ex) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while cleaning up delta files", ex);
            }

            addToAutoVacuumMap(session);

            return new CarbondataVacuumTableHandle(insertTableHandle.getSchemaName(),
                    insertTableHandle.getTableName(),
                    insertTableHandle.getInputColumns(),
                    insertTableHandle.getPageSinkMetadata(),
                    insertTableHandle.getLocationHandle(),
                    insertTableHandle.getBucketProperty(),
                    insertTableHandle.getTableStorageFormat(),
                    insertTableHandle.getPartitionStorageFormat(),
                    full,
                    ImmutableMap.<String, String>of(CarbondataConstants.EncodedLoadModel, initialConfiguration.get(LOAD_MODEL),
                    CarbondataConstants.TxnBeginTimeStamp, timeStamp.toString()));
        });
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishVacuum(ConnectorSession session, ConnectorVacuumTableHandle handle,
             Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        List<PartitionUpdate> partitionUpdates = new ArrayList<>();
        CarbondataVacuumTableHandle carbondataVacuumTableHandle = (CarbondataVacuumTableHandle) handle;

        return hdfsEnvironment.doAs(session.getUser(), () -> {
            Properties hiveSchema = MetastoreUtil.getHiveSchema(this.table.get());
            CarbonTable finalCarbonTable = getCarbonTable(carbondataVacuumTableHandle.getSchemaName(),
                    carbondataVacuumTableHandle.getTableName(),
                    hiveSchema,
                    initialConfiguration);

            compactionType = carbondataVacuumTableHandle.isFullVacuum() ? CarbondataConstants.MajorCompaction : CarbondataConstants.MinorCompaction;

            //get the codec here and unpack
            List<CarbondataSegmentInfoUtil> mergedSegmentInfoUtilList = fragments.stream()
                    .map(Slice::getBytes)
                    .map(segmentInfoCodec::fromJson)
                    .collect(toList());

            Map<String, Set<String>> mergedSegmentInfoUtilListMap = new HashMap<>();

            for (CarbondataSegmentInfoUtil segment : mergedSegmentInfoUtilList) {
                if (mergedSegmentInfoUtilListMap.containsKey(segment.getDestinationSegment())) {
                    Set<String> sourceSegments = mergedSegmentInfoUtilListMap.get(segment.getDestinationSegment());
                    sourceSegments.addAll(segment.getSourceSegmentIdSet());
                    mergedSegmentInfoUtilListMap.put(segment.getDestinationSegment(), sourceSegments);
                }
                else {
                    mergedSegmentInfoUtilListMap.put(segment.getDestinationSegment(), segment.getSourceSegmentIdSet());
                }
            }

            List<CarbondataSegmentInfoUtil> newMergedSegmentInfoUtilList = new ArrayList<>();

            for (Map.Entry<String, Set<String>> entrySet : mergedSegmentInfoUtilListMap.entrySet()) {
                CarbondataSegmentInfoUtil seg = new CarbondataSegmentInfoUtil(entrySet.getKey(), entrySet.getValue());
                newMergedSegmentInfoUtilList.add(seg);
            }

            Set<Set<String>> segmentIdSuperSet = new HashSet<>();
            for (CarbondataSegmentInfoUtil segment : newMergedSegmentInfoUtilList) {
                Set<String> tempSet = segment.getSourceSegmentIdSet();
                segmentIdSuperSet.add(tempSet);
            }

            for (Set<String> segmentIDSet : segmentIdSuperSet) {
                List<LoadMetadataDetails> segmentsMergedList = new ArrayList<>();
                for (LoadMetadataDetails load : carbonLoadModel.getLoadMetadataDetails()) {
                    if (load.isCarbonFormat() && load.getSegmentStatus() == SegmentStatus.SUCCESS
                            && loadIsMergedInWorker(load.getLoadName(), segmentIDSet)) {
                        segmentsMergedList.add(load);
                    }
                }
                segmentsMerged.add(segmentsMergedList);
            }

            if (this.carbonTable.isHivePartitionTable()) {
                List<String> partitionNames = partitionUpdates
                        .stream()
                        .map(PartitionUpdate::getName).collect(toList());
                String readPath = CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath() +
                        CarbonCommonConstants.FILE_SEPARATOR + carbonLoadModel.getSegmentId() + "_" + carbonLoadModel.getFactTimeStamp() + ".tmp");
                String segmentFileName = SegmentFileStore.genSegmentFileName(carbonLoadModel.getSegmentId(), String.valueOf(timeStamp));

                try {
                    SegmentFileStore.mergeSegmentFiles(readPath, segmentFileName, CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath()));
                    String source;
                    for (String currPartitionName : partitionNames) {
                        source = finalCarbonTable.getTablePath() + "/" + currPartitionName;
                        moveFromTempFolder(source + "/" + carbonLoadModel.getSegmentId() + "_" + timeStamp + ".tmp", source);
                    }
                    segmentFilesToBeUpdatedLatest.add(new Segment(carbonLoadModel.getSegmentId(), segmentFileName));
                }
                catch (IOException e) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while merging segment files", e);
                }
                segmentFileName = segmentFileName + CarbonTablePath.SEGMENT_EXT;
            }
            else {
                for (CarbondataSegmentInfoUtil segmentInfo : newMergedSegmentInfoUtilList) {
                    String mergedLoadNumber = segmentInfo.getDestinationSegment();
                    try {
                        String segmentFileName = SegmentFileStore.writeSegmentFile(finalCarbonTable, mergedLoadNumber, String.valueOf(carbonLoadModel.getFactTimeStamp()));
                    }
                    catch (IOException e) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while merging segment files", e);
                    }
                }
            }

            List<String> allSourceSegmentNames = new ArrayList<>();
            for (CarbondataSegmentInfoUtil segment : mergedSegmentInfoUtilList) {
                allSourceSegmentNames.addAll(segment.getSourceSegmentIdSet());
            }

            autoCleanerTasks.add(new CarbondataAutoCleaner(table.get(), initialConfiguration, carbondataTableReader, metastore, hdfsEnvironment, user));
            return Optional.of(new HiveWrittenPartitions(allSourceSegmentNames));
        });
    }

    private boolean loadIsMergedInWorker(String loadName, Set<String> mergedSegmentNames)
    {
        return mergedSegmentNames.contains(loadName);
    }

    private void moveFromTempFolder(String source, String dest)
    {
        CarbonFile oldFolder = FileFactory.getCarbonFile(source);
        CarbonFile[] oldFiles = oldFolder.listFiles();
        for (CarbonFile file : oldFiles) {
            file.renameForce(dest + CarbonCommonConstants.FILE_SEPARATOR + file.getName());
        }
        oldFolder.delete();
    }

    @Override
    public Optional<ConnectorNewTableLayout> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getInsertLayout(session, tableHandle);
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // Hive connector only supports metadata delete. It does not support generic row-by-row deletion.
        // Metadata delete is implemented in Hetu by generating a plan for row-by-row delete first,
        // and then optimize it into metadata delete. As a result, Hive connector must provide partial
        // plan-time support for row-by-row delete so that planning doesn't fail. This is why we need
        // rowid handle. Note that in Hive connector, rowid handle is not implemented beyond plan-time.
        return new HiveColumnHandle(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
                HIVE_STRING, HIVE_STRING.getTypeSignature(), -13, SYNTHESIZED,
                Optional.empty());
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return new HiveColumnHandle(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
                HIVE_STRING, HIVE_STRING.getTypeSignature(), -13, SYNTHESIZED,
                Optional.empty());
    }

    @Override
    protected Map<String, String> getEmptyTableProperties(ConnectorTableMetadata tableMetadata,
                                                          Optional<HiveBucketProperty> bucketProperty,
                                                          HdfsEnvironment.HdfsContext hdfsContext)
    {
        CarbondataStorageFormat hiveStorageFormat = (CarbondataStorageFormat) CarbondataTableProperties.getCarbondataStorageFormat(tableMetadata.getProperties());
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
        if (getTransactionalValue(tableMetadata.getProperties())) {
            if (!hiveStorageFormat.name().equals("CARBON")) {
                throw new PrestoException(NOT_SUPPORTED, "Only CARBON storage format supports creating transactional table.");
            }

            // set transactional property.
            tableProperties.put(TRANSACTIONAL, Boolean.toString(getTransactionalValue(tableMetadata.getProperties())));
        }

        tableMetadata.getComment().ifPresent(value -> tableProperties.put(TABLE_COMMENT, value));
        return tableProperties.build();
    }

    private CarbonTable getCarbonTable(String dbName, String tableName, Properties schema, Configuration configuration)
    {
        return getCarbonTable(dbName, tableName, schema, configuration, carbondataTableReader);
    }

    static CarbonTable getCarbonTable(String dbName, String tableName, Properties schema, Configuration configuration, CarbondataTableReader carbondataTableReader)
    {
        CarbondataTableCacheModel tableCacheModel = carbondataTableReader
                .getCarbonCache(new SchemaTableName(dbName, tableName),
                        schema.getProperty("tablePath"), configuration);
        requireNonNull(tableCacheModel, "tableCacheModel should not be null");
        requireNonNull(tableCacheModel.getCarbonTable(),
                "tableCacheModel.carbonTable should not be null");
        requireNonNull(tableCacheModel.getCarbonTable().getTableInfo(),
                "tableCacheModel.carbonTable.tableInfo should not be null");
        return tableCacheModel.getCarbonTable();
    }

    @Override
    public void rollback()
    {
        try {
            switch (currentState) {
                case INSERT: {
                    hdfsEnvironment.doAs(user, () -> {
                        if (carbonLoadModel != null) {
                            CarbonLoaderUtil.deleteSegment(carbonLoadModel, Integer.parseInt(carbonLoadModel.getSegmentId()));
                        }
                        try {
                            carbonOutputCommitter.abortJob(jobContext, JobStatus.State.FAILED);
                        }
                        catch (IOException e) {
                            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to abort job", e);
                        }
                    });
                    break;
                }
                case UPDATE:
                case DELETE: {
                    hdfsEnvironment.doAs(user, () -> {
                        if (carbonTable != null) {
                            CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timeStamp.toString());
                        }
                    });
                    break;
                }
                case VACUUM: {
                    hdfsEnvironment.doAs(user, () -> {
                        if (carbonTable != null) {
                            try {
                                TableProcessingOperations.deletePartialLoadDataIfExist(carbonTable, true);
                            }
                            catch (IOException e) {
                                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("carbondata Vacuum Rollback failed: %s", e.getMessage()), e);
                            }
                        }
                    });
                    break;
                }
                case CREATE_TABLE_AS:
                case CREATE_TABLE: {
                    hdfsEnvironment.doAs(user, () -> {
                        if (this.tableStorageLocation.isPresent()) {
                            try {
                                CarbonUtil.dropDatabaseDirectory(this.tableStorageLocation.get());
                            }
                            catch (IOException | InterruptedException e) {
                                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("cabondata table storage cleanup: %s", e.getMessage()), e);
                            }
                        }
                    });
                    break;
                }
                case DROP_TABLE: {
                    break;
                }
                case ADD_COLUMN:
                case DROP_COLUMN:
                case RENAME_COLUMN: {
                    hdfsEnvironment.doAs(user, () -> {
                        revertAlterTableChanges();
                    });
                    break;
                }
                default: {
                    return;
                }
            }
        }
        catch (RuntimeException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("error in rollback, %s", e.getMessage()), e);
        }
        finally {
            super.rollback();
            releaseLocks();
        }
    }

    private LocationHandle getCarbonDataTableCreationPath(ConnectorSession session, ConnectorTableMetadata tableMetadata, HiveWriteUtils.OpertionType opertionType) throws PrestoException
    {
        Path targetPath = null;
        SchemaTableName finalSchemaTableName = tableMetadata.getTable();
        String finalSchemaName = finalSchemaTableName.getSchemaName();
        String tableName = finalSchemaTableName.getTableName();
        Optional<String> location = getCarbondataLocation(tableMetadata.getProperties());
        LocationHandle locationHandle;
        FileSystem fileSystem;
        String targetLocation = null;
        try {
            // User specifies the location property
            if (location.isPresent()) {
                if (!tableCreatesWithLocationAllowed) {
                    throw new PrestoException(NOT_SUPPORTED, format("Setting %s property is not allowed", LOCATION_PROPERTY));
                }
                /* if path not having prefix with filesystem type, than we will take fileSystem type from core-site.xml using below methods */
                fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, finalSchemaName), new Path(location.get()));
                targetLocation = fileSystem.getFileStatus(new Path(location.get())).getPath().toString();
                targetPath = getPath(new HdfsEnvironment.HdfsContext(session, finalSchemaName, tableName), targetLocation, false);
            }
            else {
                updateEmptyCarbondataTableStorePath(session, finalSchemaName);
                targetLocation = carbondataTableStore;
                targetLocation = targetLocation + File.separator + finalSchemaName + File.separator + tableName;
                targetPath = new Path(targetLocation);
            }
        }
        catch (IllegalArgumentException | IOException e) {
            throw new PrestoException(NOT_SUPPORTED, format("Error %s store path %s ", e.getMessage(), targetLocation));
        }
        locationHandle = locationService.forNewTable(metastore, session, finalSchemaName, tableName, Optional.empty(), Optional.of(targetPath), opertionType);
        return locationHandle;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName localSchemaTableName = tableMetadata.getTable();
        String localSchemaName = localSchemaTableName.getSchemaName();
        String tableName = localSchemaTableName.getTableName();
        this.user = session.getUser();
        this.schemaName = localSchemaName;
        currentState = State.CREATE_TABLE;
        List<String> partitionedBy = new ArrayList<String>();
        List<SortingColumn> sortBy = new ArrayList<SortingColumn>();
        List<HiveColumnHandle> columnHandles = new ArrayList<HiveColumnHandle>();
        Map<String, String> tableProperties = new HashMap<String, String>();
        getParametersForCreateTable(session, tableMetadata, partitionedBy, sortBy, columnHandles, tableProperties);

        metastore.getDatabase(localSchemaName).orElseThrow(() -> new SchemaNotFoundException(localSchemaName));

        BaseStorageFormat hiveStorageFormat = CarbondataTableProperties.getCarbondataStorageFormat(tableMetadata.getProperties());
        // it will get final path to create carbon table
        LocationHandle locationHandle = getCarbonDataTableCreationPath(session, tableMetadata, HiveWriteUtils.OpertionType.CREATE_TABLE);
        Path targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();

        AbsoluteTableIdentifier finalAbsoluteTableIdentifier = AbsoluteTableIdentifier.from(targetPath.toString(),
                new CarbonTableIdentifier(localSchemaName, tableName, UUID.randomUUID().toString()));
        hdfsEnvironment.doAs(session.getUser(), () -> {
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment.getConfiguration(
                        new HdfsEnvironment.HdfsContext(session, localSchemaName, tableName),
                        new Path(locationHandle.getJsonSerializableTargetPath())));

            CarbondataMetadataUtils.createMetaDataFolderSchemaFile(hdfsEnvironment, session, columnHandles, finalAbsoluteTableIdentifier, partitionedBy,
                    sortBy.stream().map(s -> s.getColumnName().toLowerCase(Locale.ENGLISH)).collect(toList()), targetPath.toString(), initialConfiguration);

            this.tableStorageLocation = Optional.of(targetPath.toString());
            try {
                Map<String, String> serdeParameters = initSerDeProperties(tableName);
                Table localTable = buildTableObject(
                        session.getQueryId(),
                        localSchemaName,
                        tableName,
                        session.getUser(),
                        columnHandles,
                        hiveStorageFormat,
                        partitionedBy,
                        Optional.empty(),
                        tableProperties,
                        targetPath,
                        true, // carbon table is set as external table
                        prestoVersion,
                        serdeParameters);
                PrincipalPrivileges principalPrivileges = MetastoreUtil.buildInitialPrivilegeSet(localTable.getOwner());
                HiveBasicStatistics basicStatistics = localTable.getPartitionColumns().isEmpty() ? HiveBasicStatistics.createZeroStatistics() : HiveBasicStatistics.createEmptyStatistics();
                metastore.createTable(
                        session,
                        localTable,
                        principalPrivileges,
                        Optional.empty(),
                        ignoreExisting,
                        new PartitionStatistics(basicStatistics, ImmutableMap.of()));
            }
            catch (RuntimeException ex) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error: creating table: %s ", ex.getMessage()), ex);
            }
        });
    }

    @Override
    public void commit()
    {
        try {
            switch (currentState) {
                case INSERT: {
                    hdfsEnvironment.doAs(user, () -> {
                        carbonOutputCommitter.commitJob(jobContext);
                        return true; //added return to avoid compilation issue
                    });
                    super.commit();
                    break;
                }
                case UPDATE:
                case DELETE: {
                    hdfsEnvironment.doAs(user, () -> {
                        commitUpdateAndDelete();
                        return true; //added return to avoid compilation issue
                    });
                    super.commit();
                    break;
                }
                case VACUUM: {
                    hdfsEnvironment.doAs(user, () -> {
                        commitVacuum();
                    });
                    super.commit();
                    break;
                }
                case DROP_TABLE: {
                    super.commit();
                    hdfsEnvironment.doAs(user, () -> {
                        CarbonUtil.dropDatabaseDirectory(this.tableStorageLocation.get());
                        return true; //added return to avoid compilation issue
                    });
                    break;
                }
                case CREATE_TABLE: {
                    super.commit();
                    break;
                }
                case CREATE_TABLE_AS: {
                    hdfsEnvironment.doAs(user, () -> {
                        try {
                            carbonOutputCommitter.commitJob(jobContext);
                        }
                        catch (IOException e) {
                            LOG.error("Error occurred while carbon commitJob.", e);
                            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error occurred while carbon commitJob %s", e.getMessage()), e);
                        }
                    });
                    super.commit();
                    break;
                }
                case ADD_COLUMN:
                case RENAME_COLUMN:
                case DROP_COLUMN: {
                    hdfsEnvironment.doAs(user, () -> {
                        writeSchemaFile();
                    });
                    super.commit();
                    break;
                }
                default: {
                    super.commit();
                }
            }
        }
        //doAs is throwing exception we need to catch, it no option
        catch (TableAlreadyExistsException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error commit %s", e.getMessage()), e);
        }
        catch (Exception e) {
            if (tableStorageLocation.isPresent() && ((currentState.CREATE_TABLE == currentState) || (currentState.CREATE_TABLE_AS == currentState))) {
                hdfsEnvironment.doAs(user, () -> {
                    try {
                        CarbonUtil.dropDatabaseDirectory(this.tableStorageLocation.get());
                    }
                    catch (IOException | InterruptedException ex) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error in commit & carbon cleanup %s", ex.getMessage()), e);
                    }
                });
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error commit %s", e.getMessage()), e);
        }
        finally {
            releaseLocks();
        }
        submitCleanupTasks();
    }

    @Override
    public CarbondataTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Optional<Table> finalTable = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
        if (!finalTable.isPresent()) {
            return null;
        }

        // we must not allow system tables due to how permissions are checked in SystemTableAwareAccessControl
        if (getSourceTableNameFromSystemTable(tableName).isPresent()) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Unexpected table present in Hive metastore: " + tableName);
        }

        MetastoreUtil.verifyOnline(tableName, Optional.empty(), MetastoreUtil.getProtectMode(finalTable.get()), finalTable.get().getParameters());

        return new CarbondataTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                finalTable.get().getParameters(),
                getPartitionKeyColumnHandles(finalTable.get()),
                HiveBucketing.getHiveBucketHandle(finalTable.get()));
    }

    private Optional<ConnectorOutputMetadata> finishUpdateAndDelete(ConnectorSession session,
                                                                    HiveInsertTableHandle tableHandle,
                                                                    Collection<Slice> fragments,
                                                                    Collection<ComputedStatistics> computedStatistics)
    {
        List<PartitionUpdate> partitionUpdates = new ArrayList<>();

        Optional<ConnectorOutputMetadata> connectorOutputMetadata =
                super.finishInsert(session, tableHandle, fragments, computedStatistics, partitionUpdates);

        /* Write SegmentUpdateStatusManager and Update SegmentStatusManager */
        Gson gson = new Gson();
        blockUpdateDetailsList = partitionUpdates
                .stream()
                .map(PartitionUpdate::getMiscData)
                .flatMap(List::stream)
                .map(json -> gson.fromJson(StringEscapeUtils.unescapeJson(json), SegmentUpdateDetails.class))
                .collect(Collectors.toList());

        hdfsEnvironment.doAs(user, () -> {
            if (blockUpdateDetailsList.size() > 0) {
                CarbonTable finalCarbonTable = getCarbonTable(tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        MetastoreUtil.getHiveSchema(table.get()),
                        initialConfiguration);

                SegmentUpdateStatusManager statusManager = new SegmentUpdateStatusManager(finalCarbonTable);
                SegmentUpdateDetails[] segementDetailsList = statusManager.getUpdateStatusDetails();
                for (SegmentUpdateDetails segementDetails : segementDetailsList) {
                    segementDetails.getDeletedRowsInBlock();
                }

                /*
                 * Todo: Check how to extract block bitmap for deleted records...
                 *  Use the same to mark Segment deleted instead of jus the row.
                 */
            }
        });
        return connectorOutputMetadata;
    }

    private void updateEmptyCarbondataTableStorePath(ConnectorSession session, String schemaName) throws IOException
    {
        FileSystem fileSystem;
        String targetLocation;
        if (StringUtils.isEmpty(carbondataTableStore)) {
            Database database = metastore.getDatabase(defaultDBName).orElseThrow(() -> new SchemaNotFoundException(defaultDBName));
            String tableStore = database.getLocation().get();
            /* if path not having prefix with filesystem type, than we will take fileSystem type (ex:hdfs,file:) from core-site.xml using below methods */
            fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, schemaName), new Path(tableStore));
            targetLocation = fileSystem.getFileStatus(new Path(tableStore)).getPath().toString();
            carbondataTableStore = targetLocation.endsWith(File.separator) ?
                    (targetLocation + carbondataStorageFolderName) : (targetLocation + File.separator + carbondataStorageFolderName);
        }
        else {
            fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, schemaName), new Path(carbondataTableStore));
            carbondataTableStore = fileSystem.getFileStatus(new Path(carbondataTableStore)).getPath().toString();
        }
    }

    public void getParametersForCreateTable(ConnectorSession session,
                                            ConnectorTableMetadata tableMetadata,
                                            List<String> partitionedBy,
                                            List<SortingColumn> sortBy,
                                            List<HiveColumnHandle> columnHandles,
                                            Map<String, String> tableProperties)
    {
        SchemaTableName finalSchemaTableName = tableMetadata.getTable();
        String finalSchemaName = finalSchemaTableName.getSchemaName();
        String finalTableName = finalSchemaTableName.getTableName();

        partitionedBy.addAll(CarbondataTableProperties.getPartitionedBy(tableMetadata.getProperties()));
        sortBy.addAll(CarbondataTableProperties.getSortedBy(tableMetadata.getProperties()));
        Optional<HiveBucketProperty> bucketProperty = Optional.empty();
        columnHandles.addAll(getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator));
        tableProperties.putAll(getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsEnvironment.HdfsContext(session, finalSchemaName, finalTableName)));
    }

    @Override
    public CarbondataOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        // get the root directory for the database
        SchemaTableName finalSchemaTableName = tableMetadata.getTable();
        String finalSchemaName = finalSchemaTableName.getSchemaName();
        String finalTableName = finalSchemaTableName.getTableName();
        this.user = session.getUser();
        this.schemaName = finalSchemaName;
        currentState = State.CREATE_TABLE_AS;

        List<String> partitionedBy = new ArrayList<String>();
        List<SortingColumn> sortBy = new ArrayList<SortingColumn>();
        List<HiveColumnHandle> columnHandles = new ArrayList<HiveColumnHandle>();
        Map<String, String> tableProperties = new HashMap<String, String>();
        getParametersForCreateTable(session, tableMetadata, partitionedBy, sortBy, columnHandles, tableProperties);
        metastore.getDatabase(finalSchemaName).orElseThrow(() -> new SchemaNotFoundException(finalSchemaName));

        // to avoid type mismatch between HiveStorageFormat & Carbondata StorageFormat this hack no option
        HiveStorageFormat tableStorageFormat = HiveStorageFormat.valueOf("CARBON");

        HiveStorageFormat partitionStorageFormat = tableStorageFormat;
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        // it will get final path to create carbon table
        LocationHandle locationHandle = getCarbonDataTableCreationPath(session, tableMetadata, HiveWriteUtils.OpertionType.CREATE_TABLE_AS);
        Path targetPath = locationService.getTableWriteInfo(locationHandle, false).getTargetPath();
        AbsoluteTableIdentifier finalAbsoluteTableIdentifier = AbsoluteTableIdentifier.from(targetPath.toString(),
                new CarbonTableIdentifier(finalSchemaName, finalTableName, UUID.randomUUID().toString()));

        hdfsEnvironment.doAs(session.getUser(), () -> {
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment.getConfiguration(
                                new HdfsEnvironment.HdfsContext(session, finalSchemaName, finalTableName),
                                new Path(locationHandle.getJsonSerializableTargetPath())));
            // Create Carbondata metadata folder and Schema file
            CarbondataMetadataUtils.createMetaDataFolderSchemaFile(hdfsEnvironment, session, columnHandles, finalAbsoluteTableIdentifier, partitionedBy,
                    sortBy.stream().map(s -> s.getColumnName().toLowerCase(Locale.ENGLISH)).collect(toList()), targetPath.toString(), initialConfiguration);

            this.tableStorageLocation = Optional.of(targetPath.toString());
            Path outputPath = new Path(locationHandle.getJsonSerializableTargetPath());
            Properties schema = readSchemaForCarbon(finalSchemaName, finalTableName, targetPath, columnHandles, partitionColumns);
            // Create committer object
            setupCommitWriter(schema, outputPath, initialConfiguration, false);
        });
        try {
            CarbondataOutputTableHandle result = new CarbondataOutputTableHandle(
                    finalSchemaName,
                    finalTableName,
                    columnHandles,
                    metastore.generatePageSinkMetadata(new HiveIdentity(session), finalSchemaTableName),
                    locationHandle,
                    tableStorageFormat,
                    partitionStorageFormat,
                    partitionedBy,
                    Optional.empty(),
                    session.getUser(),
                    tableProperties, ImmutableMap.<String, String>of(
                    EncodedLoadModel, jobContext.getConfiguration().get(LOAD_MODEL)));

            LocationService.WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
            metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), finalSchemaTableName);
            return result;
        }
        catch (RuntimeException ex) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error: creating table: %s ", ex.getMessage()), ex);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        Optional<ConnectorOutputMetadata> connectorOutputMetadata;
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;
        setExternalTable(true);
        Map<String, String> serdeParameters = initSerDeProperties(handle.getTableName());
        connectorOutputMetadata = super.finishCreateTable(session, tableHandle, fragments, computedStatistics, serdeParameters);
        writeSegmentFileAndSetLoadModel();
        return connectorOutputMetadata;
    }

    private void initLocksForCurrentTable()
    {
        metadataLock = CarbonLockFactory.getCarbonLockObj(carbonTable
                .getAbsoluteTableIdentifier(), LockUsage.METADATA_LOCK);
        compactionLock = CarbonLockFactory.getCarbonLockObj(carbonTable
                .getAbsoluteTableIdentifier(), LockUsage.COMPACTION_LOCK);
        updateLock = CarbonLockFactory.getCarbonLockObj(carbonTable
                .getAbsoluteTableIdentifier(), LockUsage.UPDATE_LOCK);
        carbonDropTableLock = CarbonLockFactory.getCarbonLockObj(carbonTable
                .getAbsoluteTableIdentifier(), LockUsage.DROP_TABLE_LOCK);
    }

    private void takeLocks(State state) throws PrestoException
    {
        initLocksForCurrentTable();
        try {
            switch (state) {
                case UPDATE:
                case DELETE: {
                    takeMetadataLock();
                    takeUpdateCompactionLock();
                    break;
                }
                case DROP_TABLE: {
                    takeDropTableLock();
                    break;
                }
                case VACUUM: {
                    takeUpdateCompactionLock();
                    break;
                }
                default: {
                    LOG.error("Should not take locks for " + state + " state.");
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Should not take locks in " + state + " state");
                }
            }
        }
        catch (Exception e) {
            LOG.error("Exception in " + state + " operation.", e);
            releaseLocks();
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while taking locks", e);
        }
    }

    private void takeMetadataLock() throws RuntimeException
    {
        boolean lockStatus = metadataLock.lockWithRetries();
        if (lockStatus) {
            LOG.info("Successfully able to get the table metadata file lock");
        }
        else {
            throw new RuntimeException("Unable to get metadata lock. Please try after some time");
        }
    }

    private void takeDropTableLock() throws RuntimeException
    {
        //first take metadata lock and then take drop table lock
        takeMetadataLock();
        boolean lockStatus = carbonDropTableLock.lockWithRetries();
        if (lockStatus) {
            LOG.info("Successfully able to get the drop table lock");
        }
        else {
            throw new RuntimeException("Unable to get Drop Table lock. Please try after some time");
        }
    }

    private void takeUpdateCompactionLock() throws RuntimeException
    {
        try {
            if (updateLock.lockWithRetries() &&
                    compactionLock.lockWithRetries()) {
                LOG.info("Successfully able to get update and compaction locks");
            }
            else {
                throw new RuntimeException("Unable to get update and compaction locks");
            }
        }
        catch (RuntimeException e) {
            /*
            * Using try-catch here to ensure that update lock is released if compaction lock is not taken.
            * */
            LOG.error("Exception in taking update and compaction locks", e);
            releaseLocks();
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while taking locks", e);
        }
    }

    private void releaseLocks()
    {
        if (null != updateLock) {
            updateLock.unlock();
        }
        if (null != compactionLock) {
            compactionLock.unlock();
        }
        if (null != carbonDropTableLock) {
            CarbonLockUtil.fileUnlock(carbonDropTableLock, LockUsage.DROP_TABLE_LOCK);
        }
        if (null != metadataLock) {
            CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK);
        }
    }

    private void commitUpdateAndDelete() throws IOException
    {
        boolean updateSegmentStatusSuccess = CarbonUpdateUtil.updateSegmentStatus(blockUpdateDetailsList, carbonTable, timeStamp.toString(), false);
        List<Segment> segmentFilesToBeUpdated = blockUpdateDetailsList.stream()
                .map(SegmentUpdateDetails::getSegmentName)
                .map(Segment::new).collect(Collectors.toList());
        List<Segment> finalSegmentFilesToBeUpdatedLatest = new ArrayList<>();
        List<Segment> segmentFilesToBeDeleted = blockUpdateDetailsList.stream()
                .filter(segmentUpdateDetails -> segmentUpdateDetails.getSegmentStatus() != null &&
                        segmentUpdateDetails.getSegmentStatus().equals(SegmentStatus.MARKED_FOR_DELETE))
                .map(SegmentUpdateDetails::getSegmentName)
                .map(Segment::new).collect(Collectors.toList());

        for (Segment segment : segmentFilesToBeUpdated) {
            String file =
                    SegmentFileStore.writeSegmentFile(carbonTable, segment.getSegmentNo(), timeStamp.toString());
            finalSegmentFilesToBeUpdatedLatest.add(new Segment(segment.getSegmentNo(), file));
        }
        if (!(updateSegmentStatusSuccess &&
                CarbonUpdateUtil.updateTableMetadataStatus(new HashSet<>(segmentFilesToBeUpdated),
                        carbonTable, timeStamp.toString(), true, segmentFilesToBeDeleted,
                        finalSegmentFilesToBeUpdatedLatest, ""))) {
            CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timeStamp.toString());
        }
    }

    @Override
    protected void finishInsertOverwrite(ConnectorSession session, HiveInsertTableHandle handle, Table table, PartitionUpdate partitionUpdate, PartitionStatistics partitionStatistics)
    {
        metastore.finishInsertIntoExistingTable(
                session,
                handle.getSchemaName(),
                handle.getTableName(),
                partitionUpdate.getWritePath(),
                partitionUpdate.getFileNames(),
                partitionStatistics,
                HiveACIDWriteType.INSERT_OVERWRITE);
        markSegmentsForDelete(session, table);
    }

    @Override
    protected void finishInsertInNewPartition(ConnectorSession session, HiveInsertTableHandle handle, Table table, Map<String, Type> columnTypes, PartitionUpdate partitionUpdate, Map<List<String>, ComputedStatistics> partitionComputedStatistics, HiveACIDWriteType acidWriteType)
    {
        // insert into new partition or overwrite existing partition
        if (partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.OVERWRITE) {
            List<String> partitionValues = toPartitionValues(partitionUpdate.getName());
            PartitionStatistics partitionStatistics = createPartitionStatistics(
                    session,
                    partitionUpdate.getStatistics(),
                    columnTypes,
                    getColumnStatistics(partitionComputedStatistics, partitionValues));
            metastore.finishInsertIntoExistingPartition(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    partitionValues,
                    partitionUpdate.getWritePath(),
                    partitionUpdate.getFileNames(),
                    partitionStatistics,
                    acidWriteType);
        }
        else if (partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.APPEND) {
            Partition partition = buildPartitionObject(session, table, partitionUpdate);
            if (!partition.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && HiveSessionProperties.isRespectTableFormat(session)) {
                throw new PrestoException(HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
            }

            PartitionStatistics partitionStatistics = createPartitionStatistics(
                    session,
                    partitionUpdate.getStatistics(),
                    columnTypes,
                    getColumnStatistics(partitionComputedStatistics, partition.getValues()));
            metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath(), partitionStatistics, acidWriteType);
        }
    }

    private void markSegmentsForDelete(ConnectorSession session, Table table)
    {
        hdfsEnvironment.doAs(session.getUser(), () -> {
            try {
                Properties hiveschema = MetastoreUtil.getHiveSchema(table);
                Configuration configuration = jobContext.getConfiguration();
                configuration.set(SET_OVERWRITE, "false");
                CarbonLoadModel loadModel = HiveCarbonUtil.getCarbonLoadModel(hiveschema, configuration);
                LoadMetadataDetails loadMetadataDetails = loadModel.getCurrentLoadMetadataDetail();
                loadModel.setSegmentId(loadMetadataDetails.getLoadName());
                CarbonLoaderUtil.recordNewLoadMetadata(loadMetadataDetails, loadModel, false, true);
            }
            catch (IOException e) {
                LOG.error("Error occurred while committing the insert job.", e);
                throw new RuntimeException(e);
            }
        });
    }

    private Properties readSchemaForCarbon(String schemaName, String tableName, Path targetPath, List<HiveColumnHandle> columnHandles, List<Column> partitionColumns)
    {
        ImmutableList.Builder<HiveWriterFactory.DataColumn> dataColumns = ImmutableList.builder();
        List<HiveWriterFactory.DataColumn> dataColumnsList;

        for (HiveColumnHandle column : columnHandles) {
            HiveType hiveType = column.getHiveType();
            if (!column.isPartitionKey()) {
                dataColumns.add(new HiveWriterFactory.DataColumn(column.getName(), hiveType));
            }
        }
        dataColumnsList = dataColumns.build();

        Properties schema;
        schema = new Properties();
        schema.setProperty(IOConstants.COLUMNS, dataColumnsList.stream()
                .map(HiveWriterFactory.DataColumn::getName)
                .collect(joining(",")));
        schema.setProperty(IOConstants.COLUMNS_TYPES, dataColumnsList.stream()
                .map(HiveWriterFactory.DataColumn::getHiveType)
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        String name = schemaName + "." + tableName;
        schema.setProperty(META_TABLE_NAME, name);

        schema.setProperty(META_TABLE_LOCATION, targetPath.toString());
        MetastoreUtil.insertPartitionIntoProperties(partitionColumns, schema);
        return schema;
    }

    // carbondata don't support bucket, there is No option
    @Override
    protected void validateBucketColumns(ConnectorTableMetadata tableMetadata)
    {
        return;
    }

    @Override
    protected void validateCsvColumns(ConnectorTableMetadata tableMetadata)
    {
        return;
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        Optional<Table> target = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        if (target.get().getPartitionColumns().size() > 0) {
            throw new PrestoException(NOT_SUPPORTED, "Operations on Partitioned CarbonTables is not supported");
        }

        this.user = session.getUser();
        this.schemaName = target.get().getDatabaseName();
        this.currentState = State.DROP_TABLE;

        this.tableStorageLocation = Optional.of(target.get().getStorage().getLocation());

        try {
            hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, target.get().getDatabaseName()), new Path(this.tableStorageLocation.get()));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "hdfsEnvironment.getFileSystem", e);
        }
        try {
            hdfsEnvironment.doAs(session.getUser(), () -> {
                metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
                Configuration finalInitialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                        .getConfiguration(new HdfsEnvironment.HdfsContext(session, handle.getSchemaName(),
                                handle.getTableName()), new Path(this.tableStorageLocation.get())));

                Properties schema = MetastoreUtil.getHiveSchema(target.get());
                schema.setProperty("tablePath", this.tableStorageLocation.get());
                this.carbonTable = getCarbonTable(handle.getSchemaName(), handle.getTableName(),
                        schema, finalInitialConfiguration);
                takeLocks(State.DROP_TABLE);
                AbsoluteTableIdentifier identifier = this.carbonTable.getAbsoluteTableIdentifier();
                if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Table insert or overwrite in Progress");
                }
                try {
                    //Simultaneous case after acquiring locks we should check table exist.
                    //if table is not there clean the lock folders
                    carbonTable = getCarbonTable(handle.getSchemaName(), handle.getTableName(), schema, finalInitialConfiguration);
                }//CarbonFileException
                catch (RuntimeException e) {
                    try {
                        CarbonUtil.dropDatabaseDirectory(this.tableStorageLocation.get());
                    }
                    catch (IOException | InterruptedException ex) {
                        LOG.error(format("Failed carbon cleanup db directory:  %s", ex.getMessage()));
                    }
                    throw new TableNotFoundException(handle.getSchemaTableName());
                }

                carbondataTableReader.deleteTableFromCarbonCache(new SchemaTableName(schemaName, target.get().getTableName()));
            });
        }
        catch (PrestoException e) {
            releaseLocks();
            throw e;
        }
        catch (RuntimeException ex) {
            releaseLocks();
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error in drop table %s", ex.getMessage()), ex);
        }
    }

    @Override
    protected void verifyStorageFormatForCatalog(StorageFormat storageFormat)
    {
        requireNonNull(storageFormat, "Storage format is null");

        if (!storageFormat.getInputFormat().contains("CarbonInputFormat")) {
            String sf = storageFormat.getInputFormat();
            throw new PrestoException(NOT_SUPPORTED,
                    String.format("Tables with %s are not supported by Carbondata connector", sf.substring(sf.lastIndexOf(".") + 1)));
        }
    }

    private Map<String, String> initSerDeProperties(String tableName)
    {
        Map<String, String> serdeParameters = new HashMap<String, String>();
        serdeParameters.put(serdeDbName, this.schemaName);
        serdeParameters.put(serdeTableName, tableName);
        serdeParameters.put(serdeIsExternal, "false");
        serdeParameters.put(serdePath, this.tableStorageLocation.get());
        serdeParameters.put(serdeTablePath, this.tableStorageLocation.get());
        serdeParameters.put(serdeIsTransactional, "true");
        serdeParameters.put(serdeIsVisible, "true");
        serdeParameters.put(serdeSerializationFormat, "1");
        return serdeParameters;
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        currentState = State.ADD_COLUMN;
        updateSchemaInfo(session, tableHandle, column, null, null);

        super.addColumn(session, tableHandle, column);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        currentState = State.RENAME_COLUMN;
        updateSchemaInfo(session, tableHandle, null, source, target);

        super.renameColumn(session, tableHandle, source, target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        currentState = State.DROP_COLUMN;
        updateSchemaInfo(session, tableHandle, null, column, null);

        super.dropColumn(session, tableHandle, column);
    }

    private void updateSchemaInfo(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnHandle source, String target)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        table = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName());
        String tablePath = table.get().getStorage().getLocation();
        hdfsEnvironment.doAs(user, () -> {
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                    .getConfiguration(
                            new HdfsEnvironment.HdfsContext(session, handle.getSchemaName(),
                                    handle.getTableName()), new Path(tablePath)));
            Properties schema = MetastoreUtil.getHiveSchema(table.get());
            schema.setProperty("tablePath", tablePath);
            carbonTable = getCarbonTable(handle.getSchemaName(),
                    handle.getTableName(),
                    schema,
                    initialConfiguration);
        });
        acquireLocksForAlter();
        schemaTableName = handle.getSchemaTableName();
        absoluteTableIdentifier = AbsoluteTableIdentifier.from(tablePath, handle.getTableName(), handle.getSchemaName());
        tableInfo = carbonTable.getTableInfo();
        SchemaEvolutionEntry schemaEvolutionEntry;
        switch (currentState) {
            case ADD_COLUMN: {
                schemaEvolutionEntry = updateSchemaInfoAddColumn(column);
                break;
            }
            case RENAME_COLUMN: {
                schemaEvolutionEntry = updateSchemaInfoRenameColumn(source, target);
                break;
            }
            case DROP_TABLE: {
                schemaEvolutionEntry = updateSchemaInfoDropColumn(source);
                break;
            }
            default: {
                return;
            }
        }
        if (schemaEvolutionEntry != null) {
            tableInfo.getFactTable().getSchemaEvolution().getSchemaEvolutionEntryList()
                    .add(schemaEvolutionEntry);
        }
    }

    private SchemaEvolutionEntry updateSchemaInfoAddColumn(ColumnMetadata column)
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle(column.getName(), HiveType.toHiveType(typeTranslator, column.getType()),
                column.getType().getTypeSignature(), tableInfo.getFactTable().getListOfColumns().size(), HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        TableSchema tableSchema = tableInfo.getFactTable();
        List<ColumnSchema> tableColumns = tableSchema.getListOfColumns();
        int currentSchemaOrdinal = tableColumns.stream().max(Comparator.comparing(ColumnSchema::getSchemaOrdinal))
                .orElseThrow(NoSuchElementException::new).getSchemaOrdinal() + 1;
        List<ColumnSchema> longStringColumns = new ArrayList<>();
        List<ColumnSchema> allColumns = tableColumns.stream().filter(cols -> cols.isDimensionColumn()
                && !cols.getDataType().isComplexType() && cols.getSchemaOrdinal() != -1 && (cols.getDataType() != DataTypes.VARCHAR)).collect(toList());

        TableSchemaBuilder schemaBuilder = new TableSchemaBuilder();
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        ColumnSchema newColumn = schemaBuilder.addColumn(new StructField(columnHandle.getName(),
                        CarbondataHetuFilterUtil.spi2CarbondataTypeMapper(columnHandle)), null,
                false, false);
        newColumn.setSchemaOrdinal(currentSchemaOrdinal);
        columnSchemas.add(newColumn);

        if (newColumn.getDataType() == DataTypes.VARCHAR) {
            longStringColumns.add(newColumn);
        }
        else if (newColumn.isDimensionColumn()) {
            // add the column which is not long string
            allColumns.add(newColumn);
        }
        // put the old long string columns
        allColumns.addAll(tableColumns.stream().filter(cols -> cols.isDimensionColumn() && (cols.getDataType() == DataTypes.VARCHAR)).collect(toList()));
        // and the new long string column after old long string columns
        allColumns.addAll(longStringColumns);
        // put complex type columns at the end of dimension columns
        allColumns.addAll(tableColumns.stream().filter(cols -> cols.isDimensionColumn() &&
                (cols.isComplexColumn() || cols.getSchemaOrdinal() == -1)).collect(toList()));
        // original measure columns
        allColumns.addAll(tableColumns.stream().filter(cols -> !cols.isDimensionColumn()).collect(toList()));
        // add new measure column
        if (!newColumn.isDimensionColumn()) {
            allColumns.add(newColumn);
        }
        allColumns.stream().filter(cols -> !cols.isInvisible()).collect(Collectors.groupingBy(ColumnSchema::getColumnName))
                .forEach((columnName, schemaList) -> {
                    if (schemaList.size() > 2) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Duplicate columns found"));
                    }
                });
        if (newColumn.isComplexColumn()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Complex column cannot be added"));
        }

        List<ColumnSchema> finalAllColumns = allColumns;
        allColumns.stream().forEach(columnSchema -> {
            List<ColumnSchema> colWithSameId = finalAllColumns.stream().filter(x ->
                    x.getColumnUniqueId().equals(columnSchema.getColumnUniqueId())).collect(toList());
            if (colWithSameId.size() > 1) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Two columns can not have same columnId"));
            }
        });
        if (tableInfo.getFactTable().getPartitionInfo() != null) {
            List<ColumnSchema> par = tableInfo.getFactTable().getPartitionInfo().getColumnSchemaList();
            allColumns = allColumns.stream().filter(cols -> !par.contains(cols)).collect(toList());
            allColumns.addAll(par);
        }
        tableSchema.setListOfColumns(allColumns);
        tableInfo.setLastUpdatedTime(timeStamp);
        tableInfo.setFactTable(tableSchema);
        SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
        schemaEvolutionEntry.setTimeStamp(timeStamp);
        schemaEvolutionEntry.setAdded(columnSchemas);

        return schemaEvolutionEntry;
    }

    private SchemaEvolutionEntry updateSchemaInfoRenameColumn(ColumnHandle source, String target)
    {
        HiveColumnHandle oldColumnHandle = (HiveColumnHandle) source;
        String oldColumnName = oldColumnHandle.getColumnName();
        String newColumnName = target;
        if (!carbonTable.canAllow(carbonTable, TableOperation.ALTER_COLUMN_RENAME, oldColumnHandle.getName())) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Alter table rename column is  not supported for index indexschema"));
        }

        TableSchema tableSchema = tableInfo.getFactTable();
        List<ColumnSchema> tableColumns = tableSchema.getListOfColumns();

        if (!tableColumns.stream().map(cols -> cols.getColumnName()).collect(toList()).contains(oldColumnHandle.getName())) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Column " + oldColumnHandle.getName() + "does not exist in " +
                    carbonTable.getDatabaseName() + "." + carbonTable.getTableName()));
        }

        List<ColumnSchema> carbonColumns = carbonTable.getCreateOrderColumn().stream().filter(cols -> !cols.isInvisible())
                .map(cols -> cols.getColumnSchema()).collect(toList());

        ColumnSchema oldCarbonColumn = carbonColumns.stream().filter(cols -> cols.getColumnName().equalsIgnoreCase(oldColumnName)).findFirst().get();
        validateColumnsForRenaming(oldCarbonColumn);

        TableSchemaBuilder schemaBuilder = new TableSchemaBuilder();
        ColumnSchema deletedColumn = schemaBuilder.addColumn(new StructField(oldColumnHandle.getName(),
                CarbondataHetuFilterUtil.spi2CarbondataTypeMapper(oldColumnHandle)), null, false, false);

        SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
        tableColumns.forEach(cols -> {
            if (cols.getColumnName().equalsIgnoreCase(oldColumnName)) {
                cols.setColumnName(newColumnName);
                schemaEvolutionEntry.setTimeStamp(timeStamp);
                schemaEvolutionEntry.setAdded(Arrays.asList(cols));
                schemaEvolutionEntry.setRemoved(Arrays.asList(deletedColumn));
            }
        });

        Map<String, String> tableProperties = tableInfo.getFactTable().getTableProperties();
        tableProperties.forEach((tablePropertyKey, tablePropertyValue) -> {
            if (tablePropertyKey.equalsIgnoreCase(oldColumnName)) {
                tableProperties.put(tablePropertyKey, newColumnName);
            }
        });

        tableInfo.setLastUpdatedTime(System.currentTimeMillis());
        tableInfo.setFactTable(tableSchema);
        return schemaEvolutionEntry;
    }

    private SchemaEvolutionEntry updateSchemaInfoDropColumn(ColumnHandle column)
    {
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;
        TableSchema tableSchema = tableInfo.getFactTable();
        List<ColumnSchema> tableColumns = tableSchema.getListOfColumns();
        int currentSchemaOrdinal = tableColumns.stream().max(Comparator.comparing(ColumnSchema::getSchemaOrdinal))
                .orElseThrow(NoSuchElementException::new).getSchemaOrdinal() + 1;

        TableSchemaBuilder schemaBuilder = new TableSchemaBuilder();
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        ColumnSchema newColumn = schemaBuilder.addColumn(new StructField(columnHandle.getColumnName(),
                        CarbondataHetuFilterUtil.spi2CarbondataTypeMapper(columnHandle)), null,
                false, false);
        newColumn.setSchemaOrdinal(currentSchemaOrdinal);
        columnSchemas.add(newColumn);

        PartitionInfo partitionInfo = tableInfo.getFactTable().getPartitionInfo();
        if (partitionInfo != null) {
            List<String> partitionColumnSchemaList = tableInfo.getFactTable().getPartitionInfo()
                    .getColumnSchemaList().stream().map(cols -> cols.getColumnName()).collect(toList());
            if (partitionColumnSchemaList.stream().anyMatch(partitionColumn -> partitionColumn.equals(newColumn.getColumnName()))) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Partition columns cannot be dropped");
            }
            // when table has two columns, dropping unpartitioned column will be wrong
            if (tableColumns.stream().filter(cols -> !cols.getColumnName().equals(newColumn.getColumnName()))
                    .map(cols -> cols.getColumnName()).equals(partitionColumnSchemaList)) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot have table with all columns as partition columns");
            }
        }

        if (!tableColumns.stream().filter(cols -> cols.getColumnName().equals(newColumn.getColumnName())).collect(toList()).isEmpty()) {
            if (newColumn.isComplexColumn()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Complex column cannot be dropped");
            }
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot have table with all columns as partition columns");
        }
        tableInfo.setLastUpdatedTime(System.currentTimeMillis());
        tableInfo.setFactTable(tableSchema);

        SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
        schemaEvolutionEntry.setTimeStamp(timeStamp);
        schemaEvolutionEntry.setRemoved(columnSchemas);

        return schemaEvolutionEntry;
    }

    private void revertAlterTableChanges()
    {
        String tableName = absoluteTableIdentifier.getTableName();
        String databaseName = absoluteTableIdentifier.getDatabaseName();
        TableInfo finalTableInfo = carbonTable.getTableInfo();
        List<SchemaEvolutionEntry> evolutionEntryList = finalTableInfo.getFactTable().getSchemaEvolution().getSchemaEvolutionEntryList();
        Long updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).getTimeStamp();
        LOG.info("Reverting changes for " + databaseName + "." + tableName);
        List<ColumnSchema> addedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).getAdded();
        List<ColumnSchema> removedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).getRemoved();
        if (updatedTime == timeStamp) {
            switch (currentState) {
                case ADD_COLUMN: {
                    carbonTable.getTableInfo().getFactTable().getListOfColumns().removeAll(addedSchemas);
                    break;
                }
                case DROP_COLUMN: {
                    finalTableInfo.getFactTable().getListOfColumns().forEach(cols -> removedSchemas.forEach(removedCols -> {
                        if (cols.isInvisible() && removedCols.getColumnUniqueId().equals(cols.getColumnUniqueId())) {
                            cols.setInvisible(false);
                        }
                    }));
                    break;
                }
                default:
                    break;
            }
            evolutionEntryList.remove(evolutionEntryList.size() - 1);
            writeSchemaFile();
        }
    }

    private void writeSchemaFile()
    {
        try {
            String schemaFilePath = CarbonTablePath.getSchemaFilePath(table.get().getStorage().getLocation());
            SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
            ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
            thriftWriter.open(FileWriteOperation.OVERWRITE);
            thriftWriter.write(schemaConverter.fromWrapperToExternalTableInfo(tableInfo, absoluteTableIdentifier.getTableName(),
                    absoluteTableIdentifier.getDatabaseName()));
            thriftWriter.close();
            FileFactory.getCarbonFile(schemaFilePath).setLastModifiedTime(timeStamp);
            carbondataTableReader.deleteTableFromCarbonCache(new SchemaTableName(absoluteTableIdentifier.getDatabaseName(), absoluteTableIdentifier.getTableName()));
            CarbonMetadata.getInstance().removeTable(absoluteTableIdentifier.getTablePath(), absoluteTableIdentifier.getDatabaseName());
            CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
            LOG.info("Schema file written");
        }
        catch (IOException e) {
            //TODO handle cases while exception thrown
            releaseLocks();
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error while writing to schema file", e));
        }
    }

    private void acquireLocksForAlter()
    {
        metadataLock = CarbonLockFactory.getCarbonLockObj(carbonTable
                .getAbsoluteTableIdentifier(), LockUsage.METADATA_LOCK);
        compactionLock = CarbonLockFactory.getCarbonLockObj(carbonTable
                .getAbsoluteTableIdentifier(), LockUsage.COMPACTION_LOCK);
        try {
            boolean lockStatus = metadataLock.lockWithRetries();
            if (lockStatus) {
                LOG.info("Successfully able to get the table metadata file lock");
            }
            else {
                throw new Exception("Table is already locked");
            }

            if (compactionLock.lockWithRetries()) {
                LOG.info("Successfully able to get compaction lock");
            }
            else {
                throw new RuntimeException("Unable to get compaction locks");
            }
        }
        catch (Exception e) {
            LOG.error("Exception in alter operation", e);
            releaseLocks();
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error while taking locks", e);
        }
    }

    private void validateColumnsForRenaming(ColumnSchema oldColumn)
    {
        if (carbonTable != null) {
            // if the column rename is for complex column, block the operation
            if (oldColumn.isComplexColumn()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Rename column is not supported for complex datatype"));
            }
            // if column rename operation is on partition column, then fail the rename operation
            if (null != carbonTable.getPartitionInfo()) {
                List<ColumnSchema> partitionColumns = carbonTable.getPartitionInfo().getColumnSchemaList();
                if (!partitionColumns.stream().filter(cols -> cols.getColumnName()
                        .equalsIgnoreCase(oldColumn.getColumnName())).collect(toList()).isEmpty()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Cannot rename a partition column"));
                }
            }
        }
    }

    private void writeSegmentFileAndSetLoadModel()
    {
        hdfsEnvironment.doAs(user, () -> {
            try {
                carbonLoadModel = MapredCarbonOutputFormat.getLoadModel(initialConfiguration);
                ThreadLocalSessionInfo.unsetAll();
                /*
                CarbonOutputCommitter.commitJob() is expecting segment files inside tmp folder.
                Since, Carbondata doesn't provide any api to write segment in tmp folder,
                therefore we are implementing writeSegment api in hetu.
                 */
                CarbondataMetadataUtils.writeSegmentFile(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
                        carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));
                CarbonTableOutputFormat.setLoadModel(initialConfiguration, carbonLoadModel);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in write segment ", e);
            }
        });
    }

    private void commitVacuum() throws PrestoException
    {
        try {
            if (segmentsMerged.size() != 0) {
                /*
                * this conditional check ensures that if two vacuums are called one after the other,
                * the second one will not throw any exception and just fall through
                * */
                for (List<LoadMetadataDetails> segmentsMergedList : segmentsMerged) {
                    /*
                    * If there are nX segments that may be merged, then segmentsMerged has n lists, one for each of the X segments
                    * that are merged. This ensures that the tablestatus is correctly updated with the merged segment name.
                    * */
                    String mergedLoadNumber = CarbonDataMergerUtil.getMergedLoadName(segmentsMergedList).split("_")[1];
                    String segmentFileName = SegmentFileStore.writeSegmentFile(
                            carbonTable,
                            mergedLoadNumber,
                            String.valueOf(carbonLoadModel.getFactTimeStamp()));
                    boolean updateMergeStatus = CarbonDataMergerUtil.updateLoadMetadataWithMergeStatus(
                            segmentsMergedList,
                            carbonTable.getMetadataPath(),
                            mergedLoadNumber,
                            carbonLoadModel,
                            compactionType.equals(CarbondataConstants.MajorCompaction) ? CompactionType.MAJOR : CompactionType.MINOR,
                            segmentFileName,
                            null);
                    if (!updateMergeStatus) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in updating load metadata with merge status");
                    }
                }
            }
        }
        catch (IOException | NoSuchMVException e) {
            LOG.error("Error in updating table status file after compaction");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in updating table status file after compaction");
        }
    }

    @Override
    protected ConnectorTableMetadata doGetTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<Table> finalTable = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
        if (!finalTable.isPresent() || finalTable.get().getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            throw new TableNotFoundException(tableName);
        }

        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(finalTable.get(), typeManager);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(finalTable.get())) {
            columns.add(metadataGetter.apply(columnHandle));
        }

        // External location property
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(LOCATION_PROPERTY, finalTable.get().getStorage().getLocation());

        // Storage format property
        properties.put(HiveTableProperties.STORAGE_FORMAT_PROPERTY, CarbondataStorageFormat.CARBON);

        // Partitioning property
        List<String> partitionedBy = finalTable.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(HiveTableProperties.PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        Optional<String> comment = Optional.ofNullable(finalTable.get().getParameters().get(TABLE_COMMENT));

        // add partitioned columns into immutableColumns
        ImmutableList.Builder<ColumnMetadata> immutableColumns = ImmutableList.builder();

        for (HiveColumnHandle columnHandle : hiveColumnHandles(finalTable.get())) {
            if (columnHandle.getColumnType().equals(HiveColumnHandle.ColumnType.PARTITION_KEY)) {
                immutableColumns.add(metadataGetter.apply(columnHandle));
            }
        }

        return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), comment,
                Optional.of(immutableColumns.build()), Optional.of(NON_INHERITABLE_PROPERTIES));
    }

    @Override
    public List<ConnectorVacuumTableInfo> getTablesForVacuum()
    {
        return CarbondataAutoVacuumThread.getAutoVacuumTableList(this.getMetastore());
    }

    public void submitCleanupTasks()
    {
        if (autoCleanerTasks.size() > 0) {
            if (enableTracingCleanupTask) {
                queuedTasks.addAll(autoCleanerTasks
                        .stream()
                        .map(act -> act.submitCarbondataAutoCleanupTask(vacuumExecutorService))
                        .collect(Collectors.toList()));
            }
            else {
                autoCleanerTasks.forEach(c -> c.submitCarbondataAutoCleanupTask(vacuumExecutorService));
            }
        }
    }

    @VisibleForTesting
    public static void enableTracingCleanupTask(boolean isEnabled)
    {
        enableTracingCleanupTask = isEnabled;
    }

    @VisibleForTesting
    public static void waitForSubmittedTasksFinish()
    {
        queuedTasks.stream().forEach(f -> {
            try {
                f.get();
            }
            catch (InterruptedException e) {
                LOG.debug("Interrupted to get the result of autoCleanup : " + e);
            }
            catch (ExecutionException e) {
                LOG.debug("Exception to get the result of autoCleanup : " + e);
            }
        });
        queuedTasks.clear();
        LOG.info("All autocleanup tasks finished");
    }

    @Override
    protected boolean checkIfSuitableToPush(Set<ColumnHandle> allColumnHandles, ConnectorTableHandle tableHandle, ConnectorSession session)
    {
        return false;
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        super.cleanupQuery(session);
        if ((currentState == State.VACUUM) &&
                (!session.getSource().get().isEmpty()) &&
                session.getSource().get().equals("auto-vacuum")) {
            CarbondataAutoVacuumThread.removeTableFromVacuumTablesMap(table.get().getDatabaseName() + "." + table.get().getTableName());
        }
    }

    public void addToAutoVacuumMap(ConnectorSession session)
    {
        if ((!session.getSource().get().isEmpty()) && session.getSource().get().equals("auto-vacuum")) {
            CarbondataAutoVacuumThread.addTableToVacuumTablesMap(table.get().getDatabaseName() + "." + table.get().getTableName());
        }
    }
}
