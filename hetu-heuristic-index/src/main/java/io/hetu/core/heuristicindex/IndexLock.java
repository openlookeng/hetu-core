/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.heuristicindex;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.hetu.core.spi.heuristicindex.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Class used to manage the lock file during index creation
 * The whole table should be locked for indexer to be concurrently safe by creating a lock file on the table folder.
 * The lock file is considered valid if all of the following requirements are met:
 * <li>lock file exists</li>
 * <li>the time from now to the last modified time of the lock file should not exceed the pre-set limit</li>
 */
public class IndexLock
{
    static final String LOCK_FILE_SUFFIX = ".lock";

    private static final Logger LOG = LoggerFactory.getLogger(IndexLock.class);
    /**
     * Default lock file timeout if the timeout is not passed to the constructor.
     */
    private static final long DEFAULT_LOCK_FILE_TIMEOUT = 5000L;
    /**
     * The time interval for re-checking the lock file in milliseconds.
     */
    private static final long SLEEP_INTERVAL = 1000L;
    /**
     * This is used to calculate the interval for refreshing the lock file last modified date. It will be:
     * lockFileTimeout / DEFAULT_REFRESH_RATIO
     */
    private static final long DEFAULT_REFRESH_RATIO = 2L;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private IndexStore is;
    private String lockFilePath;
    private String tempFilePath;
    private ScheduledFuture<?> heartBeat;
    private UUID uuid = UUID.randomUUID();
    /**
     * If the current time subtract the last modified time of the lock file exceeds this limit, the lock file will
     * be considered stale.
     */
    private long lockFileTimeout = DEFAULT_LOCK_FILE_TIMEOUT;

    /**
     * Periodic task that updates the lock file every `lockFileTimeout` / `DEFAULT_REFRESH_RATIO` milliseconds.
     */
    private class LockBeatTask
            implements Runnable
    {
        @Override
        public void run()
        {
            try {
                is.write("lock", lockFilePath, true);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error updating the lock file: " + lockFilePath, e);
            }
        }
    }

    /**
     * Create the index lock object, but does not perform any locking until `lock()` is called
     *
     * @param is       IndexStore instance, e.g. LocalIndexStore, HDFSIndexStore
     * @param tableDir full absolute path required. locker file will be created in the parent directory of \
     *                 <code>tableDir</code>. Lock file name will be <code>tableDir + LOCK_FILE_SUFFIX</code>
     */
    public IndexLock(IndexStore is, String tableDir)
    {
        this.is = is;
        this.lockFilePath = tableDir + LOCK_FILE_SUFFIX;
        this.tempFilePath = this.lockFilePath + ".tmp";
    }

    /**
     * Create the index lock object, but does not perform any locking until `lock()` is called
     *
     * @param is          IndexStore instance, e.g. LocalIndexStore, HDFSIndexStore
     * @param tableDir    full absolute path required. locker file will be created in the parent directory of \
     *                    <code>tableDir</code>. Lock file name will be <code>tableDir + LOCK_FILE_SUFFIX</code>
     * @param lockTimeout Time to live for a lockfile. Meaning if the lockfile has expired, it will be ignored by \
     *                    the indexer. This is to deal with forced exits of the indexer that do not delete the lockFile
     */
    public IndexLock(IndexStore is, String tableDir, long lockTimeout)
    {
        this(is, tableDir);
        this.lockFileTimeout = lockTimeout;
    }

    /**
     * Wait (if the table is being locked) and create the lock file.
     */
    public void lock()
    {
        while (isLocked() || !acquiredLock()) {
            try {
                LOG.info("Waiting for file lock ...");
                Thread.sleep(SLEEP_INTERVAL);
            }
            catch (InterruptedException e) {
                throw new UncheckedExecutionException("waiting for lock file to be released was interrupted", e);
            }
        }
        // we run it first to avoid the async delay caused by scheduler
        Runnable heartBeatTask = new LockBeatTask();
        heartBeatTask.run();
        // then, schedule it with scheduler
        heartBeat = scheduler.scheduleAtFixedRate(heartBeatTask, 0,
                lockFileTimeout / DEFAULT_REFRESH_RATIO, TimeUnit.MILLISECONDS);
    }

    /**
     * Release the file lock. This method should be called in a finally clause.
     */
    public void release()
    {
        if (heartBeat != null) {
            heartBeat.cancel(true);
            heartBeat = null;
            scheduler.shutdown();
        }
        Set<String> failedDeletions = new HashSet<>(1);
        failedDeletions.add(lockFilePath);
        failedDeletions.add(tempFilePath);
        try {
            if (!is.exists(lockFilePath) || is.delete(lockFilePath)) {
                failedDeletions.remove(lockFilePath);
            }
            if (!is.exists(tempFilePath) || is.delete(tempFilePath)) {
                failedDeletions.remove(tempFilePath);
            }
            if (!failedDeletions.isEmpty()) {
                throw new IllegalStateException("File lock failed to delete, manual deletion is required: "
                        + failedDeletions.toString());
            }
        }
        catch (IOException e) {
            if (!failedDeletions.isEmpty()) {
                throw new IllegalStateException("File lock failed to delete, manual deletion is required: "
                        + failedDeletions.toString(), e);
            }
            LOG.warn("{} {}", "Exception thrown during lock release, but lock files are deleted successfully.",
                    "No manual deletion is required.");
            LOG.debug("Exception thrown during lock.release(): ", e);
        }
    }

    private boolean isLocked()
    {
        // to compare the last modified time of the existing lock file, we need to get the current time of the
        // file system that indexStore is pointing to. We have two choices to get that information:
        //
        // 1. add a getTime() method to IndexStore SPI.
        // 2. create a temp file and get the LastModifiedTime of that temp file. (current implementation)
        try {
            if (is.exists(lockFilePath)) {
                is.write("", tempFilePath, true);
                long cur = is.getLastModifiedTime(tempFilePath);
                is.delete(tempFilePath);
                return cur - is.getLastModifiedTime(lockFilePath) <= lockFileTimeout;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error checking the lock file: " + lockFilePath, e);
        }
        return false;
    }

    private boolean acquiredLock()
    {
        // To avoid race between multiple processes, one process will write a UUID to the temp lock file and read it
        // back. If the string read is not interwined, that is, the process is the only process that's accessing the
        // lock file.
        // If the UUID is appended by some other UUID (created by other processes), the process that wrote the first
        // UUID will acquire the lock, and the other will wait.
        // If not then everyone waits, and then lock will be reacquired after the lock file is considered expired again.
        try {
            // Write the UUID to the file
            if (is.exists(tempFilePath)) {
                return false;
            }
            is.write(uuid.toString(), tempFilePath, false);
            try (
                    InputStream in = is.read(tempFilePath);
                    InputStreamReader reader = new InputStreamReader(in, Charset.forName("utf8"))) {
                // Read in the id, and check if the first ID equals the UUID written
                int idLength = uuid.toString().length();
                char[] firstIdChars = new char[idLength];
                return reader.read(firstIdChars, 0, idLength) > 0
                        && uuid.toString().equals(new String(firstIdChars));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Execption thrown during accessing lock temp file: " + tempFilePath, e);
        }
    }

    /**
     * This should be set before lock() is called to be effective.
     *
     * @param timeout time in milliseconds to determine whether the lock file is still active.
     */
    public void setLockFileTimeout(long timeout)
    {
        this.lockFileTimeout = timeout;
    }
}
