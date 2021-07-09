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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableMap;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestMemorySpilling
        extends AbstractTestQueryFramework
{
    private static final String maxDataPerNode = "2MB";
    private static final int processingDelay = 10000;
    private static final int numLoops = 3;

    public TestMemorySpilling()
    {
        super(() -> MemoryQueryRunner.createQueryRunner(2, ImmutableMap.of(), ImmutableMap.of("memory.max-data-per-node", maxDataPerNode), false));
    }

    @Test
    public void testEvictionCreate() throws InterruptedException
    {
        synchronized (this) {
            try {
                //create a copy of tiny.orders in memory connector, use 1.8MB out of 2MB
                assertQuerySucceeds("CREATE TABLE test_t1_1 AS SELECT * FROM tpch.tiny.orders");
                Thread.sleep(processingDelay);

                //without memory eviction this table creation would not succeed, it would take 3.6MB out of 2MB
                //so memory connector evicts `test_t1_1` to spill_root
                //and then creates and stores `test_t1_2` in the memory connector
                assertQuerySucceeds("CREATE TABLE test_t1_2 AS SELECT * FROM tpch.tiny.orders");

                //when more data in inserted into `test_t1_1`, it becomes too big to hold within the memory limit
                //creation fails
                assertQueryFails("CREATE TABLE test_t1_3 AS SELECT * FROM tpch.tiny.lineitem", "Memory limit \\[.+\\] for memory connector exceeded. Current: \\[.+\\]. Requested: \\[.+\\]");
            }
            finally {
                assertQuerySucceeds("DROP TABLE test_t1_1");
                assertQuerySucceeds("DROP TABLE test_t1_2");
                assertQuerySucceeds("DROP TABLE IF EXISTS test_t1_3");
                Thread.sleep(processingDelay);
            }
        }
    }

    @Test
    public void testEvictionSelect() throws InterruptedException
    {
        synchronized (this) {
            try {
                assertQuerySucceeds("CREATE TABLE test_t2_1 AS SELECT * FROM tpch.tiny.orders");
                Thread.sleep(processingDelay);

                assertQuerySucceeds("CREATE TABLE test_t2_2 AS SELECT * FROM tpch.tiny.orders");
                Thread.sleep(processingDelay);

                assertQuerySucceeds("SELECT COUNT(*) FROM test_t2_1");
                assertQuerySucceeds("SELECT COUNT(*) FROM test_t2_2");
            }
            finally {
                assertQuerySucceeds("DROP TABLE test_t2_1");
                assertQuerySucceeds("DROP TABLE test_t2_2");
            }
        }
    }

    @Test
    public void testConcurrentEvictionSelect()
            throws Throwable
    {
        int numTables = 3;
        synchronized (this) {
            try {
                Thread.sleep(processingDelay);
                // create the tables
                for (int i = 0; i < numTables; i++) {
                    assertQuerySucceeds("CREATE TABLE test_t3_" + i + " AS SELECT * FROM tpch.tiny.orders");
                    Thread.sleep(processingDelay);
                }
                QueryThread[] threads = new QueryThread[numTables];
                QueryThreadExceptionHandler[] threadExceptionHandlers = new QueryThreadExceptionHandler[numTables];
                // define the query threads
                for (int i = 0; i < numTables; i++) {
                    threads[i] = new QueryThread("SELECT COUNT(*) FROM test_t3_" + i + " LIMIT 100");
                    threadExceptionHandlers[i] = new QueryThreadExceptionHandler();
                    threads[i].setUncaughtExceptionHandler(threadExceptionHandlers[i]);
                }

                // start the query threads
                for (int i = 0; i < numTables; i++) {
                    threads[i].start();
                }

                // wait till all the query threads are finished
                for (int i = 0; i < numTables; i++) {
                    threads[i].join();
                }

                // catch any exceptions from the threads
                for (int i = 0; i < numTables; i++) {
                    if (threadExceptionHandlers[i].exception != null) {
                        throw threads[i].exception.getCause();
                    }
                }
            }
            finally {
                // delete tables
                for (int i = 0; i < numTables; i++) {
                    assertQuerySucceeds("DROP TABLE IF EXISTS test_t3_" + i);
                }
            }
        }
    }

    class QueryThreadExceptionHandler
            implements Thread.UncaughtExceptionHandler
    {
        Throwable exception;

        @Override
        public void uncaughtException(Thread th, Throwable ex)
        {
            this.exception = ex;
        }
    }

    class QueryThread
            extends Thread
    {
        String query;
        Exception exception;

        public QueryThread(@Language("SQL") String query)
        {
            this.query = query;
        }

        public void run()
        {
            try {
                assertQuerySucceeds(this.query);
            }
            catch (Exception ex) {
                this.exception = ex;
            }
        }
    }
}
