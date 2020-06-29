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

import io.airlift.event.client.EventClient;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveACIDWriteType;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveFileWriterFactory;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveWriter;
import io.prestosql.plugin.hive.HiveWriterFactory;
import io.prestosql.plugin.hive.HiveWriterStats;
import io.prestosql.plugin.hive.LocationHandle;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.OrcFileWriterFactory;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadataProvider;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.mapred.JobConf;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;

public class CarbondataWriterFactory
        extends HiveWriterFactory
{
    private final Map<String, String> additionalJobConf;

    public CarbondataWriterFactory(Set<HiveFileWriterFactory> fileWriterFactories,
                                   String schemaName,
                                   String tableName,
                                   boolean isCreateTable,
                                   HiveACIDWriteType acidWriteType,
                                   List<HiveColumnHandle> inputColumns,
                                   HiveStorageFormat tableStorageFormat,
                                   HiveStorageFormat partitionStorageFormat,
                                   Map<String, String> additionalTableParameters,
                                   OptionalInt bucketCount,
                                   List<SortingColumn> sortedBy,
                                   LocationHandle locationHandle,
                                   LocationService locationService,
                                   String queryId,
                                   HivePageSinkMetadataProvider pageSinkMetadataProvider,
                                   TypeManager typeManager,
                                   HdfsEnvironment hdfsEnvironment,
                                   PageSorter pageSorter,
                                   DataSize sortBufferSize,
                                   int maxOpenSortFiles,
                                   boolean immutablePartitions,
                                   ConnectorSession session,
                                   NodeManager nodeManager,
                                   EventClient eventClient,
                                   HiveSessionProperties hiveSessionProperties,
                                   HiveWriterStats hiveWriterStats,
                                   OrcFileWriterFactory orcFileWriterFactory,
                                   Map<String, String> additionalJobConf)
    {
        super(fileWriterFactories, schemaName, tableName, isCreateTable, acidWriteType,
                inputColumns, tableStorageFormat, partitionStorageFormat,
                additionalTableParameters, bucketCount, sortedBy, locationHandle,
                locationService, queryId, pageSinkMetadataProvider,
                typeManager, hdfsEnvironment, pageSorter, sortBufferSize,
                maxOpenSortFiles, immutablePartitions, session, nodeManager,
                eventClient, hiveSessionProperties, hiveWriterStats, orcFileWriterFactory);

        this.additionalJobConf = requireNonNull(additionalJobConf, "Additional JobConf is null");
    }

    private JobConf getSuperJobConf()
    {
        try {
            Field field = HiveWriterFactory.class.getDeclaredField("conf");
            field.setAccessible(true);
            Object value = field.get(this);
            field.setAccessible(false);

            if (value == null) {
                return null;
            }
            else if (JobConf.class.isAssignableFrom(value.getClass())) {
                return (JobConf) value;
            }

            throw new RuntimeException("Fields jobConf doesn't match the type!");
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException("Fields jobConf Not Found");
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Fields jobConf Not Found");
        }
    }

    @Override
    public HiveWriter createWriter(Page partitionColumns, int position, OptionalInt bucketNumber, Optional<Options> vacuumOptions)
    {
        /* set Additional JobConf */
        JobConf jobConf = getSuperJobConf();

        additionalJobConf.forEach((k, v) -> jobConf.set(k, v));
        return super.createWriter(partitionColumns, position, bucketNumber, vacuumOptions);
    }

    protected void checkWriteMode(LocationService.WriteInfo writeInfo)
    {
    }

    protected void setAdditionalSchemaProperties(Properties schema)
    {
        schema.setProperty(META_TABLE_LOCATION, locationService.getTableWriteInfo(locationHandle, false).getTargetPath().toString());
    }
}
