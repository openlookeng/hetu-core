/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestMemorySpilling
        extends AbstractTestQueryFramework
{
    private static final String maxDataPerNode = "9MB";
    private static final int processingDelay = 10000;
    private static final int numLoops = 3;

    public TestMemorySpilling()
    {
        super(() -> MemoryQueryRunner.createQueryRunner(1, ImmutableMap.of(), ImmutableMap.of("memory.max-data-per-node", maxDataPerNode), false));
    }

    @AfterMethod
    @BeforeMethod
    public void dropAllTables()
    {
        MaterializedResult tables = computeActual("SHOW TABLES");
        for (MaterializedRow row : tables.getMaterializedRows()) {
            assertQuerySucceeds("DROP TABLE IF EXISTS " + row.getField(0));
        }
    }

    @Test
    public void testEvictionCreate()
    {
        synchronized (this) {
            try {
                // create one orders table
                assertQuerySucceeds("CREATE TABLE test_t1_1 WITH (async_processing=false) AS SELECT * FROM tpch.tiny.orders");

                // without memory eviction this table creation would not succeed
                // if this query passes then the eviction is functioning well
                assertQuerySucceeds("CREATE TABLE test_t1_2 WITH (async_processing=false) AS SELECT * FROM tpch.tiny.orders");

                // creation of a table over total limit. even with eviction this should fail
                assertQueryFails("CREATE TABLE test_t1_3 WITH (async_processing=false) AS SELECT * FROM tpch.tiny.lineitem", "Memory limit \\[.+\\] for memory connector exceeded. Current: \\[.+\\]. Requested: \\[.+\\]");
            }
            finally {
                assertQuerySucceeds("DROP TABLE IF EXISTS test_t1_1");
                assertQuerySucceeds("DROP TABLE IF EXISTS test_t1_2");
                assertQuerySucceeds("DROP TABLE IF EXISTS test_t1_3");
            }
        }
    }

    @Test
    public void testEvictionSelect()
    {
        synchronized (this) {
            try {
                assertQuerySucceeds("CREATE TABLE test_t2_1 WITH (async_processing=false) AS SELECT * FROM tpch.tiny.orders");

                assertQuerySucceeds("CREATE TABLE test_t2_2 WITH (async_processing=false) AS SELECT * FROM tpch.tiny.orders");

                assertQuerySucceeds("SELECT COUNT(*) FROM test_t2_1");
                assertQuerySucceeds("SELECT COUNT(*) FROM test_t2_2");
            }
            finally {
                assertQuerySucceeds("DROP TABLE IF EXISTS test_t2_1");
                assertQuerySucceeds("DROP TABLE IF EXISTS test_t2_2");
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
                // create the tables
                for (int i = 0; i < numTables; i++) {
                    assertQuerySucceeds("CREATE TABLE test_t3_" + i + " WITH (async_processing=false) AS SELECT * FROM tpch.tiny.orders");
                }
                QueryThread[] threads = new QueryThread[numTables];
                QueryThreadExceptionHandler[] threadExceptionHandlers = new QueryThreadExceptionHandler[numTables];
                // define the query threads
                for (int i = 0; i < numTables; i++) {
                    threads[i] = new QueryThread("SELECT COUNT(*) FROM test_t3_" + i);
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
