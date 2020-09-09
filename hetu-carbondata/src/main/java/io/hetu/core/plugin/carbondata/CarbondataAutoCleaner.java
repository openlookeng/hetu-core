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

import io.hetu.core.plugin.carbondata.impl.CarbondataTableReader;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static io.hetu.core.plugin.carbondata.CarbondataMetadata.getCarbonTable;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static org.apache.carbondata.core.locks.LockUsage.CLEAN_FILES_LOCK;

public class CarbondataAutoCleaner
{
    private final Configuration initialConfiguration;
    private final Table table;
    private final Logger log = LogServiceFactory.getLogService(CarbondataAutoCleaner.class.getName());
    private final CarbondataTableReader carbondataTableReader;
    private final SemiTransactionalHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final String user;

    public CarbondataAutoCleaner(Table table, Configuration initialConfiguration,
                                 CarbondataTableReader carbondataTableReader,
                                 SemiTransactionalHiveMetastore metastore,
                                 HdfsEnvironment hdfsEnvironment, String user)
    {
        this.table = table;
        this.initialConfiguration = initialConfiguration;
        this.carbondataTableReader = carbondataTableReader;
        this.metastore = metastore;
        this.hdfsEnvironment = hdfsEnvironment;
        this.user = user;
    }

    public Future<?> submitCarbondataAutoCleanupTask(ScheduledExecutorService executorService)
    {
        Future<?> result = null;
        if (null != executorService) {
            result = executorService.submit(new CarbondataAutoCleanerTask());
            log.debug("Submitting task to Vacuum Cleaner thread pool");
        }

        return result;
    }

    private class CarbondataAutoCleanerTask
            implements Runnable
    {
        @Override
        public void run()
        {
            hdfsEnvironment.doAs(user, () -> {
                cleanupTable();
            });
        }
    }

    private void cleanupTable()
    {
        CarbonTable carbonTable = null;

        try {
            Properties schema = MetastoreUtil.getHiveSchema(table);
            schema.setProperty("tablePath", table.getStorage().getLocation());
            carbonTable = getCarbonTable(table.getDatabaseName(),
                    table.getTableName(),
                    schema,
                    initialConfiguration,
                    carbondataTableReader);

            cleanupVacuumTable(carbonTable);
        }
        catch (Exception e) {
            log.debug("Exception in  cleanup: " + e.getMessage());
        }
    }

    public static void cleanupVacuumTable(CarbonTable carbonTable)
    {
        ICarbonLock cleanFileLock = null;
        try {
            PartitionInfo partitionInfo = carbonTable.getPartitionInfo();
            LoadMetadataDetails[] loadMetadataDetails = null;
            List<PartitionSpec> currPartitions = null;
            if (partitionInfo != null && partitionInfo.getPartitionType() == PartitionType.NATIVE_HIVE) {
                loadMetadataDetails = SegmentStatusManager.readTableStatusFile(
                        CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()));
                Set<PartitionSpec> partitionSpecs = new HashSet<>();
                for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
                    SegmentFileStore segmentFileStore = new SegmentFileStore(carbonTable.getTablePath(), loadMetadataDetail.getSegmentFile());

                    partitionSpecs.addAll(segmentFileStore.getPartitionSpecs());
                }
                currPartitions = new ArrayList<PartitionSpec>(partitionSpecs);
            }
            AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
            cleanFileLock = CarbonLockUtil.getLockObject(identifier, LockUsage.CLEAN_FILES_LOCK);
            // Clean up the old invalid segment data before creating a new entry for new load.
            SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, false, currPartitions);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Failed while cleaning load: %s", e.getMessage()), e);
        }
        finally {
            if (null != cleanFileLock) {
                CarbonLockUtil.fileUnlock(cleanFileLock, CLEAN_FILES_LOCK);
            }
        }
    }
}
