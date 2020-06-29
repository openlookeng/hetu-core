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

import io.hetu.core.heuristicindex.base.LocalIndexStore;
import io.hetu.core.spi.heuristicindex.IndexStore;
import io.prestosql.spi.filesystem.TempFolder;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestIndexLock
{
    @Test
    public void testLockFileCreationAndDeletion() throws IOException
    {
        IndexStore indexStore = new LocalIndexStore();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tableDir = folder.newFolder();

            IndexLock lock = new IndexLock(indexStore, tableDir.getCanonicalPath());

            lock.lock();
            File lockFile = new File(tableDir.getParent(), tableDir.getName() + IndexLock.LOCK_FILE_SUFFIX);
            assertTrue(lockFile.exists());

            lock.release();
            assertFalse(lockFile.exists());
            // also check for double-deletion
            lock.release();
            assertFalse(lockFile.exists());
        }
    }

    @Test
    public void testBlocking() throws IOException, ExecutionException, InterruptedException
    {
        IndexStore indexStoreA = new LocalIndexStore();
        IndexStore indexStoreB = new LocalIndexStore();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tableDir = folder.newFolder();

            IndexLock lockA = new IndexLock(indexStoreA, tableDir.getCanonicalPath());
            IndexLock lockB = new IndexLock(indexStoreB, tableDir.getCanonicalPath());

            StringBuffer sb = new StringBuffer();
            ExecutorService scheduler = new ThreadPoolExecutor(30, 30, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1024));

            lockA.lock();
            Runnable taskA = () -> {
                for (int i = 0; i < 10; i++) {
                    sb.append('A');
                    try {
                        Thread.sleep(50);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                lockA.release();
            };
            Runnable taskB = () -> {
                lockB.lock();
                for (int i = 0; i < 10; i++) {
                    sb.append('B');
                    try {
                        Thread.sleep(50);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                lockB.release();
            };

            Future<?> futureB = scheduler.submit(taskB);
            Future<?> futureA = scheduler.submit(taskA);
            futureB.get();
            futureA.get();

            assertTrue(isIntertwined(sb.toString()));
            File lockFile = new File(tableDir.getParent(), tableDir.getName() + IndexLock.LOCK_FILE_SUFFIX);
            assertFalse(lockFile.exists());
            scheduler.shutdown();
        }
    }

    @Test
    public void testConcurrency() throws IOException, ExecutionException, InterruptedException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tableDir = folder.newFolder();

            StringBuffer sb = new StringBuffer();
            ArrayList<Future<?>> jobs = new ArrayList<>();
            ExecutorService scheduler = new ThreadPoolExecutor(6, 6, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1024));
            for (char ch = 'A'; ch <= 'Z'; ch++) {
                Character c = ch;
                IndexStore indexStore = new LocalIndexStore();
                IndexLock lock = new IndexLock(indexStore, tableDir.getCanonicalPath(), 1000);
                Runnable task = () -> {
                    lock.lock();
                    System.out.println("Acquired lock for " + c);
                    for (int i = 0; i < 10; i++) {
                        try {
                            Thread.sleep(5);
                            sb.append(c);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    lock.release();
                    System.out.println("Released lock for " + c);
                };
                jobs.add(scheduler.submit(task));
            }
            for (Future<?> job : jobs) {
                job.get();
            }

            String result = sb.toString();
            System.out.println("Test string: " + result);
            assertTrue(isIntertwined(sb.toString()), result);
            File lockFile = new File(tableDir.getParent(), tableDir.getName() + IndexLock.LOCK_FILE_SUFFIX);
            assertFalse(lockFile.exists());
            scheduler.shutdown();
        }
    }

    @Test
    public void testLockTimeout() throws IOException, InterruptedException, TimeoutException, ExecutionException
    {
        IndexStore indexStore = new LocalIndexStore();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tableDir = folder.newFile();
            File lockFile = createLocalTestLockFile(tableDir);
            ExecutorService scheduler = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1024));

            IndexLock lock = new IndexLock(indexStore, tableDir.getCanonicalPath(), 500);
            Future<Boolean> lockTask = scheduler.submit(() -> {
                lock.lock();
                return true;
            });
            // Shouldn't exceed 3 seconds for it to process
            assertTrue(lockTask.get(3, TimeUnit.SECONDS));

            lock.release();
            assertFalse(lockFile.exists());
        }
    }

    @Test
    public void testLockTimeoutSetter() throws IOException, InterruptedException, TimeoutException, ExecutionException
    {
        IndexStore indexStore = new LocalIndexStore();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tableDir = folder.newFile();
            File lockFile = createLocalTestLockFile(tableDir);
            ExecutorService scheduler = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1024));

            IndexLock lock = new IndexLock(indexStore, tableDir.getCanonicalPath());
            lock.setLockFileTimeout(500);
            Future<Boolean> lockTask = scheduler.submit(() -> {
                lock.lock();
                return true;
            });
            // Shouldn't exceed 3 seconds for it to process
            assertTrue(lockTask.get(3, TimeUnit.SECONDS));

            lock.release();
            assertFalse(lockFile.exists());
        }
    }

    @Test
    public void testInterruption() throws IOException, InterruptedException
    {
        IndexStore indexStore = new LocalIndexStore();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tableDir = folder.newFile();
            File lockFile = createLocalTestLockFile(tableDir);

            IndexLock lock = new IndexLock(indexStore, tableDir.getCanonicalPath(), 10000);
            Thread currentThread = Thread.currentThread();
            Thread interruptThread = new Thread(() -> {
                try {
                    // wait for lock to be running
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                currentThread.interrupt();
            });
            interruptThread.start();
            assertThrows(RuntimeException.class, lock::lock);

            // test deletion
            assertTrue(lockFile.exists());
            lock.release();
            assertFalse(lockFile.exists());
        }
    }

    private File createLocalTestLockFile(File tableDir)
    {
        try {
            File lockFile = new File(tableDir.getParent(), tableDir.getName() + IndexLock.LOCK_FILE_SUFFIX);
            assertTrue(lockFile.createNewFile());
            assertTrue(lockFile.exists());

            return lockFile;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Test lock file creation failed", e);
        }
    }

    /**
     * check if a string is interwined with characters.
     * e.g. AAABBB, AB returns true
     * ABAAABB returns false
     *
     * @param str string to be checked
     * @return true if every character is appeared right after another
     */
    private boolean isIntertwined(String str)
    {
        int i = 0;
        while (i < str.length()) {
            char ch = str.charAt(i);
            int last = str.lastIndexOf(ch);
            for (int j = i; j < last; j++) {
                if (str.charAt(j) != ch) {
                    return false;
                }
            }
            i = last + 1;
        }
        return true;
    }
}
