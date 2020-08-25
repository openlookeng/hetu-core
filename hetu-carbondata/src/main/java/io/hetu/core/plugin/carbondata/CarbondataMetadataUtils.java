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

import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.CarbonTableBuilder;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.prestosql.plugin.hive.HiveWriteUtils.createDirectory;
import static io.prestosql.plugin.hive.HiveWriteUtils.pathExists;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;

public class CarbondataMetadataUtils
{
    private CarbondataMetadataUtils()
    {
    }

    public static void createMetaDataFolderSchemaFile(HdfsEnvironment hdfsEnvironment, ConnectorSession session, List<HiveColumnHandle> columnHandles,
                                                      AbsoluteTableIdentifier absoluteTableIdentifier,
                                                      List<String> partitionedBy,
                                                      List<String> sortBy, String tablePath, Configuration initialConfiguration)
    {
        AtomicInteger valIndex = new AtomicInteger(0);
        TableSchemaBuilder schemaBuilder = new TableSchemaBuilder();
        List<StructField> partitionStructFields = new ArrayList<StructField>();
        ICarbonLock metadataLock = null;

        columnHandles.forEach(col -> {
            if (partitionedBy.contains(col.getName())) {
                partitionStructFields.add(new StructField(col.getName(), CarbondataHetuFilterUtil.spi2CarbondataTypeMapper(col)));
            }
            else {
                schemaBuilder.addColumn(new StructField(col.getName(), CarbondataHetuFilterUtil.spi2CarbondataTypeMapper(col)),
                        valIndex, sortBy.contains(col.getName()), false);
            }
        });
        PartitionInfo partitionInfo = null;
        if (!partitionStructFields.isEmpty()) {
            List<ColumnSchema> partitionColumnSchemas = new ArrayList<>();
            for (StructField partitionStructField : partitionStructFields) {
                partitionColumnSchemas.add(schemaBuilder.addColumn(partitionStructField, valIndex,
                        sortBy.contains(partitionStructField.getFieldName()), false));
            }
            partitionInfo = new PartitionInfo(partitionColumnSchemas, PartitionType.NATIVE_HIVE);
        }

        schemaBuilder.tableName(absoluteTableIdentifier.getTableName());
        TableSchema schema = schemaBuilder.build();

        //adding partition info to schema
        schema.setPartitionInfo(partitionInfo);
        //adding sorted_by local schope to schema
        schema.getTableProperties().put(CarbonCommonConstants.SORT_SCOPE, "LOCAL_SORT");

        CarbonTableBuilder tableBuilder = new CarbonTableBuilder();

        tableBuilder.databaseName(absoluteTableIdentifier.getDatabaseName())
                .tableName(absoluteTableIdentifier.getTableName())
                .tablePath(tablePath)
                .isTransactionalTable(true)
                .tableSchema(schema);

        CarbonTable carbonTable = tableBuilder.build();

        TableInfo tableInfo = carbonTable.getTableInfo();

        String schemaFilePath = CarbonTablePath.getSchemaFilePath(tablePath, initialConfiguration);
        String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);

        CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
        SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();

        org.apache.carbondata.format.TableInfo thriftTableInfo =
                schemaConverter.fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
                        tableInfo.getFactTable().getTableName());

        org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
                new org.apache.carbondata.format.SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
        thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history().add(schemaEvolutionEntry);

        try {
            HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(session, absoluteTableIdentifier.getDatabaseName(), absoluteTableIdentifier.getTableName());

            Path metadataPath = new Path(schemaMetadataPath);

            if (!pathExists(context, hdfsEnvironment, new Path(tablePath))) {
                // metadata directory is created
                try {
                    createDirectory(context, hdfsEnvironment, metadataPath);
                }
                catch (PrestoException ex) {
                    throw new FolderAlreadyExistException(GENERIC_INTERNAL_ERROR, format("Folder is not empty %s", ex.getMessage()), ex);
                }
                AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
                try {
                    metadataLock = CarbonLockUtil.getLockObject(identifier, LockUsage.METADATA_LOCK);
                }
                catch (RuntimeException ex) {
                    throw new FolderAlreadyExistException(GENERIC_INTERNAL_ERROR, format("Error in getting lock: %s", ex.getMessage()), ex);
                }
                //after acquiring lock check schemaFile exist to avoid simultaneous cases issues
                if (FileFactory.isFileExist(schemaFilePath)) {
                    throw new FolderAlreadyExistException(GENERIC_INTERNAL_ERROR, "Folder is not empty");
                }
                // inside metadata directory schema file is created
                ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
                try {
                    thriftWriter.open();
                    thriftWriter.write(thriftTableInfo);
                    thriftWriter.close();
                }
                catch (IOException e) {
                    CarbonUtil.dropDatabaseDirectory(tablePath);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error while creating carbon schema file %s", e.getMessage()), e);
                }
            }
            else {
                throw new FolderAlreadyExistException(ALREADY_EXISTS, "Folder is not empty");
            }
        }
        catch (IOException | InterruptedException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error while creating carbon Metadata %s", e.getMessage()), e);
        }
        finally {
            if (null != metadataLock) {
                CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK);
            }
        }
    }

    public static void writeSegmentFile(CarbonTable carbonTable, String segmentId, String uuid)
            throws IOException
    {
        String tablePath = carbonTable.getTablePath();
        boolean supportFlatFolder = carbonTable.isSupportFlatFolder();
        String segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
        CarbonFile segmentFolder = FileFactory.getCarbonFile(segmentPath);
        CarbonFile[] indexFiles = segmentFolder.listFiles(new CarbonFileFilter()
        {
            @Override
            public boolean accept(CarbonFile file)
            {
                return (file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
                        .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT));
            }
        });
        if (indexFiles != null && indexFiles.length > 0) {
            SegmentFileStore.SegmentFile segmentFile = new SegmentFileStore.SegmentFile();
            SegmentFileStore.FolderDetails folderDetails = new SegmentFileStore.FolderDetails();
            folderDetails.setRelative(true);
            folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
            for (CarbonFile file : indexFiles) {
                if (file.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
                    folderDetails.setMergeFileName(file.getName());
                }
                else {
                    folderDetails.getFiles().add(file.getName());
                }
            }
            String segmentRelativePath = "/";
            if (!supportFlatFolder) {
                segmentRelativePath = segmentPath.substring(tablePath.length());
            }
            segmentFile.getLocationMap().put(segmentRelativePath, folderDetails);
            String segmentFileFolder = CarbonTablePath.getSegmentFilesLocation(tablePath) + CarbonCommonConstants.FILE_SEPARATOR +
                    segmentId + "_" + uuid + ".tmp";
            CarbonFile carbonFile = FileFactory.getCarbonFile(segmentFileFolder);
            if (!carbonFile.exists()) {
                carbonFile.mkdirs();
            }
            String segmentFileName = SegmentFileStore.genSegmentFileName(segmentId, uuid) + CarbonTablePath.SEGMENT_EXT;
            // write segment info to new file.
            SegmentFileStore.writeSegmentFile(segmentFile, segmentFileFolder + File.separator + segmentFileName);
        }
        else {
            // Index files are not present at segment data location
        }
    }
}
