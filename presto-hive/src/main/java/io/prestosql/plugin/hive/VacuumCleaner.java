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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class VacuumCleaner
{
    private final VacuumTableInfoForCleaner vacuumTableInfo;
    private static final Logger log = Logger.get(VacuumCleaner.class);
    private final ScheduledExecutorService executorService;
    private final long cleanupInterval;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsEnvironment.HdfsContext hdfsContext;
    private final Configuration configuration;
    private final SemiTransactionalHiveMetastore metastore;

    private ScheduledFuture<?> cleanupTask;
    private Set<Long> lockIds;

    public VacuumCleaner(VacuumTableInfoForCleaner vacuumTableInfo,
                         SemiTransactionalHiveMetastore metastore,
                         HdfsEnvironment hdfsEnvironment,
                         HdfsEnvironment.HdfsContext hdfsContext)
    {
        this.vacuumTableInfo = vacuumTableInfo;
        this.hdfsEnvironment = hdfsEnvironment;
        this.hdfsContext = hdfsContext;
        this.metastore = metastore;

        this.executorService = this.metastore.getVacuumExecutorService();
        this.cleanupInterval = this.metastore.getVacuumCleanupInterval();
        this.configuration = hdfsEnvironment.getConfiguration(hdfsContext, this.vacuumTableInfo.getDirectoryPath());
    }

    private void log(String message)
    {
        String logPrefix = String.format("%s.%s", vacuumTableInfo.getDbName(), vacuumTableInfo.getTableName())
                + ((vacuumTableInfo.getPartitionName().length() > 0) ? ("." + vacuumTableInfo.getPartitionName()) : "");
        log.debug(logPrefix + " : " + message);
    }

    public void submitVacuumCleanupTask()
    {
        log("Submitting task to Vacuum Cleaner thread pool");
        cleanupTask = executorService.scheduleAtFixedRate(
                new CleanerTask(),
                0,
                cleanupInterval,
                TimeUnit.MILLISECONDS);
    }

    void stopScheduledCleanupTask()
    {
        log("Vacuum cleanup task Finished");
        cleanupTask.cancel(true);
    }

    private class CleanerTask
            implements Runnable
    {
        private int maxCleanerAttempts = 5;
        private int currentAttempt = 1;
        private boolean stop;

        @Override
        public void run()
        {
            log("Starting Vacuum cleaner task. Attempt: " + currentAttempt);

            try {
                if (!readyToClean()) {
                    log("Waiting for readers to finish");
                    currentAttempt++;
                    if (currentAttempt <= maxCleanerAttempts) {
                        return;
                    }
                    else {
                        log("Vacuum Cleaner task reached to the maximum number of attempts.");
                    }
                }
                else {
                    // All readers which had started before vacuum operation
                    // have been finished. Now we are ready to clean up.
                    String fullTableName = vacuumTableInfo.getDbName() + "." + vacuumTableInfo.getTableName();
                    long highestWriteId = vacuumTableInfo.getMaxId();
                    final ValidWriteIdList validWriteIdList = (highestWriteId > 0)
                            ? new ValidReaderWriteIdList(fullTableName, new long[0], new BitSet(), highestWriteId)
                            : new ValidReaderWriteIdList();
                    hdfsEnvironment.doAs(hdfsContext.getIdentity().getUser(), () -> {
                        removeFiles(validWriteIdList);
                    });
                }
                stop = true;
            }
            catch (Exception e) {
                log.info("Exception in Vacuum cleanup: " + e.toString());
                stop = true;
            }
            finally {
                if (stop) {
                    stopScheduledCleanupTask();
                }
            }
        }

        private void removeFiles(ValidWriteIdList writeIdList)
        {
            FileSystem fileSystem = null;
            List<Path> filesToDelete;
            try {
                AcidUtils.Directory dir = AcidUtils.getAcidState(
                        vacuumTableInfo.getDirectoryPath(),
                        configuration,
                        writeIdList);
                filesToDelete = dir.getObsolete().stream()
                        .map(fs -> fs.getPath())
                        .collect(Collectors.toList());
                if (filesToDelete.size() < 1) {
                    log("No files to delete");
                    return;
                }
                fileSystem = filesToDelete.get(0).getFileSystem(configuration);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failure while getting file system: ", e);
            }
            for (Path filePath : filesToDelete) {
                log(String.format("Removing directory on path : %s", filePath.toString()));
                try {
                    fileSystem.delete(filePath, true);
                }
                catch (IOException e) {
                    // Exception in cleaning one directory should not stop clean up of other directories.
                    // Therefore, ignoring this exception.
                    log(String.format("Directory %s deletion failed: %s", filePath, e.getMessage()));
                }
            }
        }

        // Checks if there is any reader started before vacuum operation.
        private boolean readyToClean()
        {
            // Get list of locks taken on given table and partition
            ShowLocksResponse response = metastore.showLocks(vacuumTableInfo);
            // Only wait for release of those locks which had been
            // taken before the time when we had visited this for first time.
            if (lockIds == null) {
                lockIds = lockResponseToSet(response);
                if (lockIds.size() < 1) {
                    log("No readers at present");
                    return true;
                }
            }
            log(String.format("Number of readers = %d", lockIds.size()));
            Set<Long> currentLockIds = lockResponseToSet(response);
            for (Long lockId : lockIds) {
                if (currentLockIds.contains(lockId)) {
                    return false;
                }
                else {
                    lockIds.remove(lockId);
                }
            }
            return true;
        }

        private Set<Long> lockResponseToSet(ShowLocksResponse response)
        {
            if (response.getLocks() == null) {
                return ImmutableSet.of();
            }
            return response.getLocks().stream().map(e -> e.getLockid()).collect(Collectors.toSet());
        }
    }
}
