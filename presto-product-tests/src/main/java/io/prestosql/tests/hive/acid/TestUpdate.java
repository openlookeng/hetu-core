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
package io.prestosql.tests.hive.acid;

import io.prestosql.tempto.query.QueryExecutor;
import io.prestosql.tests.hive.HiveProductTest;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.TestGroups.UPDATE;
import static io.prestosql.tests.utils.QueryExecutors.connectToPresto;
import static org.testng.Assert.assertTrue;

public class TestUpdate
        extends HiveProductTest
{
    @Test(groups = {STORAGE_FORMATS, UPDATE})
    public void testConcurrentUpdate()
            throws InterruptedException, ExecutionException
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }
        String tableName = "test_acid_table_update";
        query("DROP TABLE IF EXISTS " + tableName);
        query("CREATE TABLE " + tableName + " (col INT, fcol INT, part_col INT) " +
                "WITH (partitioned_by=ARRAY['part_col'],transactional=true)");
        query("INSERT INTO " + tableName + " VALUES (1,1,1),(1,1,2)");
        //Start a multi statement and submit update to hold the lock.
        query("START TRANSACTION");
        query("UPDATE " + tableName + " SET fcol=2 WHERE part_col=1");
        //Submit the second update in a separate thread with separate client.
        CountDownLatch latch = new CountDownLatch(1);
        Callable secondUpdate = () -> {
            QueryExecutor alicePresto = connectToPresto("alice@presto");
            latch.countDown();
            alicePresto.executeQuery("UPDATE " + tableName + " SET fcol=2 WHERE part_col=2");
            return true;
        };
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            Future<Boolean> updateFuture = executorService.submit(secondUpdate);
            //Wait for second update to get submitted.
            latch.await(5, TimeUnit.SECONDS);
            Thread.sleep(500);
            query("COMMIT");
            assertTrue(updateFuture.get());
        }
        finally {
            executorService.shutdownNow();
        }
    }
}
