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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.tests.AbstractTestDistributedQueries;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertTrue;

public class TestDCDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final TestingPrestoServer hetuServer;

    public TestDCDistributedQueries()
            throws Exception
    {
        this(new TestingPrestoServer(
                ImmutableMap.<String, String>builder().put("node-scheduler.include-coordinator", "true")
                        .put("hetu.data.center.split.count", "2").build()));
    }

    private TestDCDistributedQueries(TestingPrestoServer hetuServer)
    {
        super(() -> DataCenterQueryRunner.createDCQueryRunner(hetuServer, ImmutableMap.of(), TpchTable.getTables()));
        this.hetuServer = hetuServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        hetuServer.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    private boolean supportsBatchInsert()
    {
        return false;
    }

    @Override
    public void testAddColumn()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testAddColumn();
    }

    @Override
    public void testCreateTable()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testCreateTable();
    }

    @Override
    public void testCreateTableAsSelect()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testCreateTableAsSelect();
    }

    @Override
    public void testDropColumn()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testCreateTableAsSelect();
    }

    @Override
    public void testCommentTable()
    {
        // DC connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'",
                "This connector does not support setting table comments");
    }

    @Override
    public void testDelete()
    {
        // delete is not supported
    }

    @Override
    public void testLargeQuerySuccess()
    {
        // With Jacoco enabled, this test fails due to SOF
    }

    @Override
    public void testInsert()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testInsert();
    }

    @Override
    public void testQueryLoggingCount()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testQueryLoggingCount();
    }

    @Override
    public void testRenameColumn()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testRenameColumn();
    }

    @Override
    public void testRenameTable()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testRenameTable();
    }

    @Override
    public void testSymbolAliasing()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testSymbolAliasing();
    }

    @Override
    public void testWrittenStats()
    {
        // Batch insert through DC connector not supported
        skipTestUnless(supportsBatchInsert());

        super.testWrittenStats();
    }

    @Test
    public void testLargeIn()
    {
        // Does not support very large IN query, failed with 1000 values
        String longValues = range(0, 100).mapToObj(Integer::toString).collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String arrayValues = range(0, 100).mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery("SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery("SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    @Test
    public void testDeadLock()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        final class ThreadPerTaskExecutor
                implements Executor
        {
            @Override
            public void execute(Runnable runnable)
            {
                new Thread(runnable).start();
            }
        }

        Executor threadPerTaskExec = new ThreadPerTaskExecutor();
        CompletableFuture<Void> testMergeHLLCallableFuture = CompletableFuture.supplyAsync(() -> {
            testMergeHyperLogLog();
            return null;
        }, threadPerTaskExec);

        CompletableFuture<Void> testShowCatalogsCallableFuture = CompletableFuture.supplyAsync(() -> {
            testShowCatalogs();
            return null;
        }, threadPerTaskExec);

        CompletableFuture<Void> combined = CompletableFuture.allOf(testShowCatalogsCallableFuture, testMergeHLLCallableFuture);
        combined.get(6000, TimeUnit.SECONDS);

        assertTrue(testShowCatalogsCallableFuture.isDone());
        assertTrue(testMergeHLLCallableFuture.isDone());
    }
    // DataCenterConnector specific tests should normally go in TestDCIntegrationSmokeTest
}
