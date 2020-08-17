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

package io.prestosql.tests;

import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.Requires;
import io.prestosql.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.CREATE_TABLE;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Requires(ImmutableNationTable.class)
public class CreateTableTests
        extends ProductTest
{
    public enum CarbonTaskOperation {
        CARBON_CREATE_TABLE,
        CARBON_DROP_TABLE
    }

    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = CREATE_TABLE)
    public void shouldCreateTableAsSelect()
    {
        String tableName = "create_table_as_select";
        query(format("DROP TABLE IF EXISTS %s", tableName));
        query(format("CREATE TABLE %s(nationkey, name) AS SELECT n_nationkey, n_name FROM nation", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName))).hasRowsCount(25);
    }

    @Test(groups = CREATE_TABLE)
    public void shouldCreateTableAsEmptySelect()
    {
        String tableName = "create_table_as_empty_select";
        query(format("DROP TABLE IF EXISTS %s", tableName));
        query(format("CREATE TABLE %s(nationkey, name) AS SELECT n_nationkey, n_name FROM nation WHERE 0 is NULL", tableName));
        assertThat(query(format("SELECT nationkey, name FROM %s", tableName))).hasRowsCount(0);
    }

    @Test(groups = CREATE_TABLE)
    public void testConcurrentlyCreateTable() throws SQLException
    {
        String tableName = "carbondata.default.demotable";
        query(format("DROP TABLE IF EXISTS %s", tableName));
        query(format("CREATE TABLE %s(a int)", tableName));

        int n = 8; // Number of threads
        TestCarbondataConcurrent[] threads = new TestCarbondataConcurrent[n];
        for (int i = 0; i < n; i++) {
            threads[i] = new TestCarbondataConcurrent(tableName, CarbonTaskOperation.CARBON_CREATE_TABLE);
            threads[i].start();
        }
        for (int i = 0; i < n; i++) {
            try {
                threads[i].join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //after simultaneous create Table, it should allow to insert, update
        query(format("INSERT INTO %s VALUES(11)", tableName));
        query(format("INSERT INTO %s VALUES(12)", tableName));
        query(format("INSERT INTO %s VALUES(13)", tableName));
        query(format("INSERT INTO %s VALUES(14)", tableName));
        query(format("UPDATE %s SET a=55 WHERE a=12", tableName));
        assertThat(query(format("SELECT a FROM %s", tableName))).hasRowsCount(4);

        String partitionPath = "/user/hive/warehouse/carbon.store/default/demotable";
        assertTrue(hdfsClient.exist(partitionPath));

        for (int i = 0; i < n; i++) {
            threads[i] = new TestCarbondataConcurrent(tableName, CarbonTaskOperation.CARBON_DROP_TABLE);
            threads[i].start();
        }
        for (int i = 0; i < n; i++) {
            try {
                threads[i].join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // after simultaneous drop there should not be table folder
        assertTrue(!(hdfsClient.exist(partitionPath)));
    }
}
