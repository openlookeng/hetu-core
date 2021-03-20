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

import com.google.inject.Inject;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveACIDWriteType;
import io.prestosql.plugin.hive.HiveFileWriter;
import io.prestosql.plugin.hive.HiveFileWriterFactory;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class CarbondataFileWriterFactory
        implements HiveFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats stats;

    @Inject
    public CarbondataFileWriterFactory(HdfsEnvironment hdfsEnvironment, TypeManager typeManager,
                                       NodeVersion nodeVersion, FileFormatDataSourceStats stats)
    {
        this(typeManager, hdfsEnvironment, nodeVersion, stats);
    }

    public CarbondataFileWriterFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment,
                                       NodeVersion nodeVersion,
                                       FileFormatDataSourceStats stats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(Path path, List<String> inputColumnNames,
                                                     StorageFormat storageFormat, Properties schema,
                                                     JobConf configuration, ConnectorSession session,
                                                     Optional<AcidOutputFormat.Options> acidOptions,
                                                     Optional<HiveACIDWriteType> acidWriteType)
    {
        try {
            int taskId = session.getTaskId().getAsInt();
            int driverId = session.getDriverId().getAsInt();
            int taskWriterCount = session.getTaskWriterCount();
            //taskId starts from 0.
            //driverId starts from 0 and will be < taskWriterCount.
            //taskWriterCount starts from 1
            // for taskId n, buckets will be between n*taskWriterCount (inclusive) and (n+1)*taskWriterCount (exclusive)
            int bucketNumber = taskId * taskWriterCount + driverId;

            /* Create a DeleteDeltaFileWriter */
            return Optional
                    .of(new CarbondataFileWriter(path, inputColumnNames, schema, configuration,
                            typeManager, acidOptions, acidWriteType, OptionalInt.of(bucketNumber)));
        }
        catch (SerDeException e) {
            throw new RuntimeException("Error while creating carbon file writer", e);
        }
    }
}
