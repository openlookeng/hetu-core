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
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveLocationService;
import io.prestosql.plugin.hive.HiveWriteUtils;
import io.prestosql.plugin.hive.LocationHandle;
import io.prestosql.plugin.hive.WriteIdInfo;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static io.prestosql.plugin.hive.HiveWriteUtils.getTableDefaultLocation;
import static io.prestosql.plugin.hive.HiveWriteUtils.pathExists;
import static java.lang.String.format;

public class CarbondataLocationService
        extends HiveLocationService
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public CarbondataLocationService(HdfsEnvironment hdfsEnvironment)
    {
        super(hdfsEnvironment);
        this.hdfsEnvironment = hdfsEnvironment;
    }

    @Override
    public LocationHandle forNewTable(SemiTransactionalHiveMetastore metastore,
                                      ConnectorSession session, String schemaName,
                                      String tableName, Optional<WriteIdInfo> writeIdInfo,
                                      Optional<Path> tablePath,
                                      HiveWriteUtils.OpertionType opertionType)
    {
        // TODO: check and make it compatible for cloud scenario
        super.forNewTable(metastore, session, schemaName, tableName, writeIdInfo, tablePath, opertionType);
        Path targetPath;
        HdfsEnvironment.HdfsContext context =
                new HdfsEnvironment.HdfsContext(session, schemaName, tableName);

        if (tablePath.isPresent()) {
            targetPath = tablePath.get();
        }
        else {
            targetPath = getTableDefaultLocation(context, metastore, hdfsEnvironment, schemaName, tableName);
        }

        if (pathExists(context, hdfsEnvironment, targetPath)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS,
                    format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        return new LocationHandle(targetPath, targetPath, false,
                LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY, writeIdInfo);
    }

    @Override
    public LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore,
                                           ConnectorSession session, Table table, Optional<WriteIdInfo> writeIdInfo, HiveWriteUtils.OpertionType opertionType)
    {
        // TODO: check and make it compatible for cloud scenario

        HdfsEnvironment.HdfsContext context =
                new HdfsEnvironment.HdfsContext(session, table.getDatabaseName(), table.getTableName());
        Path targetPath = new Path(table.getStorage().getLocation());

        return new LocationHandle(targetPath, targetPath, true,
                LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY, writeIdInfo);
    }

    @Override
    public WriteInfo getTableWriteInfo(LocationHandle locationHandle, boolean overwrite)
    {
        return new WriteInfo(locationHandle.getTargetPath(), locationHandle.getWritePath(), locationHandle.getWriteMode());
    }
}
