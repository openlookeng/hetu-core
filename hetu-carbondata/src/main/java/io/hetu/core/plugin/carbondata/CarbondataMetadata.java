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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableCacheModel;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableReader;
import io.prestosql.plugin.hive.BaseStorageFormat;
import io.prestosql.plugin.hive.HdfsEnvironment;
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
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.plugin.hive.HiveUpdateTableHandle;
import io.prestosql.plugin.hive.HiveWriterFactory;
import io.prestosql.plugin.hive.LocationHandle;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.security.AccessControlMetadata;
import io.prestosql.plugin.hive.statistics.HiveStatisticsProvider;
import io.prestosql.plugin.hive.util.ConfigurationUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorDeleteAsInsertTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorUpdateTableHandle;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonOutputCommitter;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.processing.util.TableOptionConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.hetu.core.plugin.carbondata.CarbondataConstants.EncodedLoadModel;
import static io.hetu.core.plugin.carbondata.CarbondataTableProperties.getCarbondataLocation;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.prestosql.plugin.hive.HiveTableProperties.LOCATION_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.TRANSACTIONAL;
import static io.prestosql.plugin.hive.HiveTableProperties.getTransactionalValue;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.getPartitionKeyColumnHandles;
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
    private String user;
    private Optional<String> tableStorageLocation;
    private String carbondataTableStore;
    private ICarbonLock carbonDropTableLock;
    private String schemaName;

    private enum State {
        INSERT,
        UPDATE,
        DELETE,
        CREATE_TABLE,
        CREATE_TABLE_AS,
        DROP_TABLE,
        OTHER,
    }

    public CarbondataMetadata(SemiTransactionalHiveMetastore metastore,
                              HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager, DateTimeZone timeZone,
                              boolean allowCorruptWritesForTesting, boolean writesToNonManagedTablesEnabled,
                              boolean createsOfNonManagedTablesEnabled, boolean tableCreatesWithLocationAllowed,
                              TypeManager typeManager, LocationService locationService,
                              JsonCodec<PartitionUpdate> partitionUpdateCodec,
                              TypeTranslator typeTranslator, String hetuVersion,
                              HiveStatisticsProvider hiveStatisticsProvider, AccessControlMetadata accessControlMetadata,
                              CarbondataTableReader carbondataTableReader, String carbondataTableStore)
    {
        super(metastore, hdfsEnvironment, partitionManager, timeZone, allowCorruptWritesForTesting,
                writesToNonManagedTablesEnabled, createsOfNonManagedTablesEnabled, tableCreatesWithLocationAllowed,
                typeManager, locationService, partitionUpdateCodec, typeTranslator, hetuVersion,
                hiveStatisticsProvider, accessControlMetadata);
        this.carbondataTableReader = carbondataTableReader;
        this.carbondataTableStore = carbondataTableStore;
        this.metadataLock = null;
        this.carbonDropTableLock = null;
        this.schemaName = null;
        this.tableStorageLocation = Optional.empty();
    }

    private void setupCommitWriter(Optional<Table> table, Path outputPath, Configuration initialConfiguration, boolean isOverwrite)
    {
        Properties hiveSchema = MetastoreUtil.getHiveSchema(table.get());
        setupCommitWriter(hiveSchema, outputPath, initialConfiguration, isOverwrite);
    }

    private void setupCommitWriter(Properties hiveSchema, Path outputPath, Configuration initialConfiguration, boolean isOverwrite) throws PrestoException
    {
        CarbonLoadModel carbonLoadModel = null;
        TaskAttemptID taskAttemptID = TaskAttemptID.forName(initialConfiguration.get("mapred.task.id"));
        try {
            ThreadLocalSessionInfo.setConfigurationToCurrentThread(initialConfiguration);
            carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(hiveSchema, initialConfiguration);
            carbonLoadModel.setBadRecordsAction(TableOptionConstant.BAD_RECORDS_ACTION.getName() + ",force");
            CarbonTableOutputFormat.setLoadModel(initialConfiguration, carbonLoadModel);
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
        return beginInsertUpdateInternal(session, tableHandle, false);
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
            Optional<Table> table =
                    metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            if (table.isPresent() && table.get().getPartitionColumns().size() > 0) {
                throw new PrestoException(NOT_SUPPORTED, "Operations on Partitioned CarbonTables is not supported");
            }

            this.table = table;
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
            setupCommitWriter(table, outputPath, initialConfiguration, isOverwrite);

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
        return connectorOutputMetadata;
    }

    @Override
    public CarbondataUpdateTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        currentState = State.UPDATE;
        HiveInsertTableHandle parent = super.beginInsert(session, tableHandle);
        SchemaTableName tableName = parent.getSchemaTableName();
        Optional<Table> table =
                this.metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isPresent() && table.get().getPartitionColumns().size() > 0) {
            throw new PrestoException(NOT_SUPPORTED, "Operations on Partitioned CarbonTables is not supported");
        }

        this.table = table;
        this.user = session.getUser();
        hdfsEnvironment.doAs(user, () -> {
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                    .getConfiguration(
                            new HdfsEnvironment.HdfsContext(session, parent.getSchemaName(),
                                    parent.getTableName()),
                            new Path(parent.getLocationHandle().getJsonSerializableWritePath())));
            Properties schema = MetastoreUtil.getHiveSchema(table.get());
            schema.setProperty("tablePath", table.get().getStorage().getLocation());
            carbonTable = getCarbonTable(parent.getSchemaName(),
                    parent.getTableName(),
                    schema,
                    initialConfiguration);
            takeLocksForUpdateAndDelete();
            try {
                CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while cleaning Delta files", e);
            }
        });
        return new CarbondataUpdateTableHandle(parent.getSchemaName(),
                parent.getTableName(),
                parent.getInputColumns(),
                parent.getPageSinkMetadata(),
                parent.getLocationHandle(),
                parent.getBucketProperty(),
                parent.getTableStorageFormat(),
                parent.getPartitionStorageFormat(), ImmutableMap.<String, String>of(
                CarbondataConstants.TxnBeginTimeStamp, timeStamp.toString()));
    }

    @Override
    public CarbonDeleteAsInsertTableHandle beginDeletesAsInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        currentState = State.DELETE;
        HiveInsertTableHandle parent = super.beginInsert(session, tableHandle);
        SchemaTableName tableName = parent.getSchemaTableName();
        Optional<Table> table =
                this.metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isPresent() && table.get().getPartitionColumns().size() > 0) {
            throw new PrestoException(NOT_SUPPORTED, "Operations on Partitioned CarbonTables is not supported");
        }

        this.table = table;
        this.user = session.getUser();
        hdfsEnvironment.doAs(user, () -> {
            initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                    .getConfiguration(
                            new HdfsEnvironment.HdfsContext(session, parent.getSchemaName(),
                                    parent.getTableName()),
                            new Path(parent.getLocationHandle().getJsonSerializableWritePath())));
            Properties schema = MetastoreUtil.getHiveSchema(table.get());
            schema.setProperty("tablePath", table.get().getStorage().getLocation());
            carbonTable = getCarbonTable(parent.getSchemaName(),
                    parent.getTableName(),
                    schema,
                    initialConfiguration);
            takeLocksForUpdateAndDelete();
            try {
                CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed while cleaning Delta files", e);
            }
        });
        return new CarbonDeleteAsInsertTableHandle(parent.getSchemaName(),
                parent.getTableName(),
                parent.getInputColumns(),
                parent.getPageSinkMetadata(),
                parent.getLocationHandle(),
                parent.getBucketProperty(),
                parent.getTableStorageFormat(),
                parent.getPartitionStorageFormat(), ImmutableMap.<String, String>of(
                CarbondataConstants.TxnBeginTimeStamp, timeStamp.toString()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishUpdate(ConnectorSession session,
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

        return finishUpdateAndDelete(session, insertTableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getInsertLayout(session, tableHandle);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
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

    private LocationHandle getCarbonDataTableCreationPath(ConnectorSession session, ConnectorTableMetadata tableMetadata) throws PrestoException
    {
        Path targetPath = null;
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
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
                fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, schemaName), new Path(location.get()));
                targetLocation = fileSystem.getFileStatus(new Path(location.get())).getPath().toString();
                targetPath = getPath(new HdfsEnvironment.HdfsContext(session, schemaName, tableName), targetLocation, false);
            }
            else {
                updateEmptyCarbondataTableStorePath(session, schemaName);
                targetLocation = carbondataTableStore;
                targetLocation = targetLocation + File.separator + schemaName + File.separator + tableName;
                targetPath = new Path(targetLocation);
            }
        }
        catch (IllegalArgumentException | IOException e) {
            throw new PrestoException(NOT_SUPPORTED, format("Error %s store path %s ", e.getMessage(), targetLocation));
        }
        locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, Optional.empty(), Optional.of(targetPath));
        return locationHandle;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        this.user = session.getUser();
        this.schemaName = schemaName;
        currentState = State.CREATE_TABLE;
        List<String> partitionedBy = new ArrayList<String>();
        List<SortingColumn> sortBy = new ArrayList<SortingColumn>();
        List<HiveColumnHandle> columnHandles = new ArrayList<HiveColumnHandle>();
        Map<String, String> tableProperties = new HashMap<String, String>();
        getParametersForCreateTable(session, tableMetadata, partitionedBy, sortBy, columnHandles, tableProperties);

        metastore.getDatabase(schemaName).orElseThrow(() -> new SchemaNotFoundException(schemaName));

        BaseStorageFormat hiveStorageFormat = CarbondataTableProperties.getCarbondataStorageFormat(tableMetadata.getProperties());
        // it will get final path to create carbon table
        LocationHandle locationHandle = getCarbonDataTableCreationPath(session, tableMetadata);
        Path targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
        this.tableStorageLocation = Optional.of(targetPath.toString());

        AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier.from(targetPath.toString(),
                new CarbonTableIdentifier(schemaName, tableName, UUID.randomUUID().toString()));

        try {
            hdfsEnvironment.doAs(session.getUser(), () -> {
                initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment.getConfiguration(
                        new HdfsEnvironment.HdfsContext(session, schemaName, tableName),
                        new Path(locationHandle.getJsonSerializableTargetPath())));
                CarbondataMetadataUtils.createMetaDataFolderSchemaFile(hdfsEnvironment, session, columnHandles, absoluteTableIdentifier, partitionedBy,
                        sortBy.stream().map(s -> s.getColumnName().toLowerCase(Locale.ENGLISH)).collect(toList()), targetPath.toString(), initialConfiguration);

                Map<String, String> serdeParameters = initSerDeProperties(tableName);
                Table table = buildTableObject(
                        session.getQueryId(),
                        schemaName,
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
                PrincipalPrivileges principalPrivileges = MetastoreUtil.buildInitialPrivilegeSet(table.getOwner());
                HiveBasicStatistics basicStatistics = table.getPartitionColumns().isEmpty() ? HiveBasicStatistics.createZeroStatistics() : HiveBasicStatistics.createEmptyStatistics();
                metastore.createTable(
                        session,
                        table,
                        principalPrivileges,
                        Optional.empty(),
                        ignoreExisting,
                        new PartitionStatistics(basicStatistics, ImmutableMap.of()));
            });
        }
        catch (FolderAlreadyExistException e) {
            this.tableStorageLocation = Optional.empty();
            throw e;
        }
        catch (RuntimeException ex) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error: creating table: %s ", ex.getMessage()), ex);
        }
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
                default: {
                    super.commit();
                }
            }
        }
        //doAs is throwing exception we need to catch, it no option
        catch (Exception e) {
            if ((currentState.CREATE_TABLE == currentState) || (currentState.CREATE_TABLE_AS == currentState)) {
                try {
                    CarbonUtil.dropDatabaseDirectory(this.tableStorageLocation.get());
                }
                catch (IOException | InterruptedException ioException) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error in commit & carbon cleanup %s", e.getMessage()), e);
                }
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error commit %s", e.getMessage()), e);
        }
        finally {
            releaseLocks();
        }
    }

    @Override
    public CarbondataTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            return null;
        }

        // we must not allow system tables due to how permissions are checked in SystemTableAwareAccessControl
        if (getSourceTableNameFromSystemTable(tableName).isPresent()) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Unexpected table present in Hive metastore: " + tableName);
        }

        MetastoreUtil.verifyOnline(tableName, Optional.empty(), MetastoreUtil.getProtectMode(table.get()), table.get().getParameters());

        return new CarbondataTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().getParameters(),
                getPartitionKeyColumnHandles(table.get()),
                HiveBucketing.getHiveBucketHandle(table.get()));
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
                .map(json -> gson.fromJson(json, SegmentUpdateDetails.class))
                .collect(Collectors.toList());

        hdfsEnvironment.doAs(user, () -> {
            if (blockUpdateDetailsList.size() > 0) {
                CarbonTable carbonTable = getCarbonTable(tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        MetastoreUtil.getHiveSchema(table.get()),
                        initialConfiguration);

                SegmentUpdateStatusManager statusManager = new SegmentUpdateStatusManager(carbonTable);
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
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        partitionedBy.addAll(CarbondataTableProperties.getPartitionedBy(tableMetadata.getProperties()));
        sortBy.addAll(CarbondataTableProperties.getSortedBy(tableMetadata.getProperties()));
        Optional<HiveBucketProperty> bucketProperty = Optional.empty();
        columnHandles.addAll(getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator));
        tableProperties.putAll(getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsEnvironment.HdfsContext(session, schemaName, tableName)));
    }

    @Override
    public CarbondataOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        verifyJvmTimeZone();

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        this.user = session.getUser();
        this.schemaName = schemaName;
        currentState = State.CREATE_TABLE_AS;

        List<String> partitionedBy = new ArrayList<String>();
        List<SortingColumn> sortBy = new ArrayList<SortingColumn>();
        List<HiveColumnHandle> columnHandles = new ArrayList<HiveColumnHandle>();
        Map<String, String> tableProperties = new HashMap<String, String>();
        getParametersForCreateTable(session, tableMetadata, partitionedBy, sortBy, columnHandles, tableProperties);
        metastore.getDatabase(schemaName).orElseThrow(() -> new SchemaNotFoundException(schemaName));

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
        LocationHandle locationHandle = getCarbonDataTableCreationPath(session, tableMetadata);
        Path targetPath = locationService.getTableWriteInfo(locationHandle, false).getTargetPath();
        this.tableStorageLocation = Optional.of(targetPath.toString());
        AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier.from(targetPath.toString(),
                new CarbonTableIdentifier(schemaName, tableName, UUID.randomUUID().toString()));
        try {
            hdfsEnvironment.doAs(session.getUser(), () -> {
                initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment.getConfiguration(
                                new HdfsEnvironment.HdfsContext(session, schemaName, tableName),
                                new Path(locationHandle.getJsonSerializableTargetPath())));

                // Create Carbondata metadata folder and Schema file
                CarbondataMetadataUtils.createMetaDataFolderSchemaFile(hdfsEnvironment, session, columnHandles, absoluteTableIdentifier, partitionedBy,
                        sortBy.stream().map(s -> s.getColumnName().toLowerCase(Locale.ENGLISH)).collect(toList()), targetPath.toString(), initialConfiguration);

                Path outputPath = new Path(locationHandle.getJsonSerializableTargetPath());
                Properties schema = readSchemaForCarbon(schemaName, tableName, targetPath, columnHandles, partitionColumns);
                // Create committer object
                setupCommitWriter(schema, outputPath, initialConfiguration, false);
            });
            CarbondataOutputTableHandle result = new CarbondataOutputTableHandle(
                    schemaName,
                    tableName,
                    columnHandles,
                    metastore.generatePageSinkMetadata(schemaTableName),
                    locationHandle,
                    tableStorageFormat,
                    partitionStorageFormat,
                    partitionedBy,
                    Optional.empty(),
                    session.getUser(),
                    tableProperties, ImmutableMap.<String, String>of(
                    EncodedLoadModel, jobContext.getConfiguration().get(LOAD_MODEL)));

            LocationService.WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
            metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), schemaTableName);
            return result;
        }
        catch (FolderAlreadyExistException e) {
            this.tableStorageLocation = Optional.empty();
            throw e;
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
    }

    private void takeLocksForUpdateAndDelete()
    {
        initLocksForCurrentTable();
        try {
            boolean lockStatus = metadataLock.lockWithRetries();
            if (lockStatus) {
                LOG.info("Successfully able to get the table metadata file lock");
            }
            else {
                throw new Exception("Table is locked for updation. Please try after some time");
            }

            if (updateLock.lockWithRetries() &&
                    compactionLock.lockWithRetries()) {
                LOG.info("Successfully able to get update and compaction locks");
            }
            else {
                throw new RuntimeException("Unable to get update and compaction locks");
            }
        }
        catch (Exception e) {
            LOG.error("Exception in update operation", e);
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
        List<Segment> segmentFilesToBeUpdatedLatest = new ArrayList<>();
        for (Segment segment : segmentFilesToBeUpdated) {
            String file =
                    SegmentFileStore.writeSegmentFile(carbonTable, segment.getSegmentNo(), timeStamp.toString());
            segmentFilesToBeUpdatedLatest.add(new Segment(segment.getSegmentNo(), file));
        }
        if (!(updateSegmentStatusSuccess &&
                CarbonUpdateUtil.updateTableMetadataStatus(new HashSet<>(segmentFilesToBeUpdated),
                        carbonTable, timeStamp.toString(), true, ImmutableList.of(),
                        segmentFilesToBeUpdatedLatest, ""))) {
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
                partitionStatistics);
        markSegmentsForDelete(session, table);
    }

    @Override
    protected void finishInsertInNewPartition(ConnectorSession session, HiveInsertTableHandle handle, Table table, Map<String, Type> columnTypes, PartitionUpdate partitionUpdate, Map<List<String>, ComputedStatistics> partitionComputedStatistics)
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
                    partitionStatistics);
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
            metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath(), partitionStatistics);
        }
    }

    private void markSegmentsForDelete(ConnectorSession session, Table table)
    {
        hdfsEnvironment.doAs(session.getUser(), () -> {
            try {
                Properties hiveschema = MetastoreUtil.getHiveSchema(table);
                Configuration configuration = jobContext.getConfiguration();
                configuration.set(SET_OVERWRITE, "false");
                CarbonLoadModel carbonLoadModel =
                        HiveCarbonUtil.getCarbonLoadModel(hiveschema, configuration);
                LoadMetadataDetails loadMetadataDetails = carbonLoadModel.getCurrentLoadMetadataDetail();
                carbonLoadModel.setSegmentId(loadMetadataDetails.getLoadName());
                CarbonLoaderUtil.recordNewLoadMetadata(loadMetadataDetails, carbonLoadModel, false, true);
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
        Optional<Table> target = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        this.user = session.getUser();
        this.schemaName = target.get().getDatabaseName();
        this.currentState = State.DROP_TABLE;

        this.tableStorageLocation = Optional.of(target.get().getStorage().getLocation());
        //this.tableStorageLocation = Optional.of(target.get().getStorage().getSerdeParameters().get("tablePath"));

        try {
            hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session, target.get().getDatabaseName()), new Path(this.tableStorageLocation.get()));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "hdfsEnvironment.getFileSystem", e);
        }
        try {
            hdfsEnvironment.doAs(session.getUser(), () -> {
                metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
                Configuration initialConfiguration = ConfigurationUtils.toJobConf(this.hdfsEnvironment
                        .getConfiguration(new HdfsEnvironment.HdfsContext(session, handle.getSchemaName(),
                                handle.getTableName()), new Path(this.tableStorageLocation.get())));

                Properties schema = MetastoreUtil.getHiveSchema(target.get());
                schema.setProperty("tablePath", this.tableStorageLocation.get());
                this.carbonTable = getCarbonTable(handle.getSchemaName(), handle.getTableName(),
                        schema, initialConfiguration);

                AbsoluteTableIdentifier identifier = this.carbonTable.getAbsoluteTableIdentifier();
                this.metadataLock = CarbonLockUtil.getLockObject(identifier, LockUsage.METADATA_LOCK);
                this.carbonDropTableLock = CarbonLockUtil.getLockObject(identifier, LockUsage.DROP_TABLE_LOCK);
                if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Table insert or overwrite in Progress");
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

    private void writeSegmentFileAndSetLoadModel()
    {
        hdfsEnvironment.doAs(user, () -> {
            try {
                carbonLoadModel = MapredCarbonOutputFormat.getLoadModel(initialConfiguration);
                ThreadLocalSessionInfo.unsetAll();
                SegmentFileStore.writeSegmentFile(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
                        carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));
                CarbonTableOutputFormat.setLoadModel(initialConfiguration, carbonLoadModel);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in write segment ", e);
            }
        });
    }
}
