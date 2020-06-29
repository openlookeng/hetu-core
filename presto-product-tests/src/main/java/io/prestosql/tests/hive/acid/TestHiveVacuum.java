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

import com.google.inject.Inject;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import io.prestosql.tempto.query.QueryResult;
import io.prestosql.tests.hive.HiveProductTest;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.TestGroups.VACUUM;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static org.testng.Assert.assertTrue;

public class TestHiveVacuum
        extends HiveProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL, VACUUM}, dataProvider = "isTablePartitionedBucketed")
    public void testVacuumOperation(boolean isPartitioned, boolean isBucketed)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_acid_table_vacuum";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                (isBucketed ? "CLUSTERED BY (col) into 3 buckets " : "") +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='true') ");

        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));
            String partitionPath = "/user/hive/warehouse/test_acid_table_vacuum/" + (isPartitioned ? "part_col=2/" : "");
            String bucketFileName = (isBucketed ? "bucket_00000" : "bucket_00000");
            assertTrue(hdfsClient.exist(partitionPath + "delta_0000001_0000001_0000/" + bucketFileName));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            bucketFileName = (isBucketed ? "bucket_00002" : "bucket_00000");
            assertTrue(hdfsClient.exist(partitionPath + "delta_0000002_0000002_0000/" + bucketFileName));

            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(21, 1), row(22, 2));
            // test filtering
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // delete a row
            query("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));
            bucketFileName = (isBucketed ? "bucket_00002" : "bucket_00000");
            assertTrue(hdfsClient.exist(partitionPath + "delete_delta_0000003_0000003_0000/" + bucketFileName));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            query("UPDATE " + tableName + " SET fcol = 2 WHERE " + predicate);
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 2));

            bucketFileName = "bucket_00000";
            assertTrue(hdfsClient.exist(partitionPath + "delta_0000004_0000004_0000/" + bucketFileName));
            assertTrue(hdfsClient.exist(partitionPath + "delete_delta_0000004_0000004_0000/" + bucketFileName));

            QueryResult query = query("VACUUM TABLE " + tableName);

            assertThat(query).containsOnly(row(5));
            bucketFileName = "bucket_00000";
            assertTrue(hdfsClient.exist(partitionPath + "delta_0000001_0000004/" + bucketFileName));
            assertTrue(hdfsClient.exist(partitionPath + "delete_delta_0000001_0000004/" + bucketFileName));
            if (isBucketed) {
                bucketFileName = "bucket_00002";
                assertTrue(hdfsClient.exist(partitionPath + "delta_0000001_0000004/" + bucketFileName));
                assertTrue(hdfsClient.exist(partitionPath + "delete_delta_0000001_0000004/" + bucketFileName));
            }

            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 2));

            //Retry vacuum
            assertThat(query("VACUUM TABLE " + tableName)).containsOnly(row(0));

            //Full Vacuum
            assertThat(query("VACUUM TABLE " + tableName + " FULL")).containsOnly(row(1));
            bucketFileName = "bucket_00000";
            assertTrue(hdfsClient.exist(partitionPath + "base_0000004/" + bucketFileName));

            //Retry Full Vacuum
            assertThat(query("VACUUM TABLE " + tableName + " FULL")).containsOnly(row(0));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] isTablePartitionedBucketed()
    {
        return new Object[][]{
                {true, true},
                {true, false},
                {false, true},
                {false, false},
        };
    }
}
