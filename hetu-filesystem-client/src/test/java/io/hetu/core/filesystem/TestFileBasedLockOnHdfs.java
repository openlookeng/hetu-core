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
package io.hetu.core.filesystem;

import io.hetu.core.filesystem.utils.DockerizedHive;
import io.prestosql.spi.filesystem.FileBasedLock;
import org.apache.hadoop.conf.Configuration;
import org.testng.SkipException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test for FileBasedLock
 *
 * @since 2020-04-01
 */
@Test(singleThreaded = true)
public class TestFileBasedLockOnHdfs
{
    private HetuHdfsFileSystemClient fs;
    private String testLockRoot;

    @BeforeClass
    public void prepare()
            throws IOException
    {
        // Will skip the test if exception occurs in this process
        Configuration config = null;
        int trialCount = 0;

        while (config == null && trialCount < 3) {
            config = DockerizedHive.getContainerConfig(this.getClass().getName());
            trialCount++;
        }

        if (config == null) {
            throw new SkipException("Docker environment for this test not setup");
        }

        config.setBoolean("fs.hdfs.impl.disable.cache", true);
        testLockRoot = "/tmp/test-hdfs-lock";
        fs = new HetuHdfsFileSystemClient(new HdfsConfig(config), Paths.get(testLockRoot));
    }

    @Test
    public void testIsLocked()
            throws IOException, InterruptedException
    {
        Path testDir = Paths.get(testLockRoot + "/testIsLocked");
        FileBasedLock lock = new FileBasedLock(fs, testDir, 1000L,
                FileBasedLock.DEFAULT_RETRY_INTERVAL, FileBasedLock.DEFAULT_REFRESH_RATE);
        OutputStream os = fs.newOutputStream(testDir.resolve(".lockFile"));
        os.write("test".getBytes());
        os.close();
        assertTrue(lock.isLocked());
        Thread.sleep(1200L);
        // Expired, not locked
        assertFalse(lock.isLocked());
    }

    @Test
    public void testAcquiredLock()
            throws IOException, InterruptedException
    {
        Path testDir = Paths.get(testLockRoot + "/testAcquiredLock");
        FileBasedLock lock = new FileBasedLock(fs, testDir, 1000L,
                FileBasedLock.DEFAULT_RETRY_INTERVAL, FileBasedLock.DEFAULT_REFRESH_RATE);
        OutputStream os = fs.newOutputStream(testDir.resolve(".lockInfo"));
        os.write("test".getBytes());
        os.close();
        assertFalse(lock.acquiredLock());
        Thread.sleep(1200L);
        // Expired, can be acquired
        assertTrue(lock.acquiredLock());
    }

    @Test
    public void testTwoLocks()
            throws IOException
    {
        Path testDir = Paths.get(testLockRoot + "/testRaceCondition");
        FileBasedLock lock1 = new FileBasedLock(fs, testDir, 2000L,
                FileBasedLock.DEFAULT_RETRY_INTERVAL, FileBasedLock.DEFAULT_REFRESH_RATE);
        FileBasedLock lock2 = new FileBasedLock(fs, testDir, 2000L,
                FileBasedLock.DEFAULT_RETRY_INTERVAL, FileBasedLock.DEFAULT_REFRESH_RATE);
        lock1.lock();
        assertTrue(lock1.isLocked());
        assertTrue(lock1.acquiredLock());
        assertTrue(lock2.isLocked());
        assertFalse(lock2.acquiredLock());
        lock1.unlock();
        assertFalse(lock2.isLocked());
        lock2.lock();
        assertTrue(lock2.isLocked());
        assertTrue(lock2.acquiredLock());
        lock2.unlock();
        assertFalse(lock1.isLocked());
        assertFalse(lock2.isLocked());
    }

    @Test
    public void testTryLock()
            throws InterruptedException, IOException
    {
        Path testDir = Paths.get(testLockRoot + "/testTryLock");
        FileBasedLock lock = new FileBasedLock(fs, testDir);
        if (lock.tryLock()) {
            assertTrue(lock.isLocked());
            lock.unlock();
        }
        assertFalse(lock.isLocked());
        lock = new FileBasedLock(fs, testDir);
        if (lock.tryLock(1000, TimeUnit.MILLISECONDS)) {
            assertTrue(lock.isLocked());
            lock.unlock();
        }
        assertFalse(lock.isLocked());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testReuse()
            throws IOException
    {
        Path testDir = Paths.get(testLockRoot + "/testReuse");
        FileBasedLock lock = new FileBasedLock(fs, testDir);
        lock.lock();
        lock.unlock();
        // Should throw an exception
        lock.lock();
    }

    @AfterTest
    public void tearDown()
            throws IOException
    {
        fs.deleteRecursively(Paths.get(testLockRoot));
    }
}
