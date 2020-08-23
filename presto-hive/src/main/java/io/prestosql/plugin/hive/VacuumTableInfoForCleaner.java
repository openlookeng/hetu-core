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

package io.prestosql.plugin.hive;

import org.apache.hadoop.fs.Path;

public class VacuumTableInfoForCleaner
{
    private final String dbName;
    private final String tableName;
    private final String partitionName;
    private final long maxId;
    private final Path directoryPath;

    public VacuumTableInfoForCleaner(String dbName, String tableName, String partitionName, long maxId, Path directoryPath)
    {
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitionName = partitionName;
        this.maxId = maxId;
        this.directoryPath = directoryPath;
    }

    public String getDbName()
    {
        return dbName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public long getMaxId()
    {
        return maxId;
    }

    public Path getDirectoryPath()
    {
        return directoryPath;
    }
}
