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
package io.hetu.core.plugin.iceberg;

import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.hetu.core.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.hetu.core.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.tests.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergV2
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private java.nio.file.Path tempDir;
    private File metastoreDir;

    public TestIcebergV2(QueryRunnerSupplier supplier)
    {
        super(() -> IcebergQueryRunner.builder().build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testSettingFormatVersion()
    {
        String tableName = "test_seting_format_version_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(1);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDefaultFormatVersion()
    {
        String tableName = "test_default_format_version_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testV2TableRead()
    {
        String tableName = "test_v2_table_read" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        updateTableToV2(tableName);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testUpgradeTableToV2FromTrino()
    {
        String tableName = "test_upgrade_table_to_v2_from_trino_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 1);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2");
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 2);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testUpdatingAllTableProperties()
    {
        String tableName = "test_updating_all_table_properties_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1, format = 'ORC') AS SELECT * FROM tpch.tiny.nation", 25);
        BaseTable table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 1);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("ORC"));
        assertTrue(table.spec().isUnpartitioned());

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2, partitioning = ARRAY['regionkey'], format = 'PARQUET'");
        table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 2);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("PARQUET"));
        assertTrue(table.spec().isPartitioned());
        List<PartitionField> partitionFields = table.spec().fields();
        assertThat(partitionFields).hasSize(1);
        assertEquals(partitionFields.get(0).name(), "regionkey");
        assertTrue(partitionFields.get(0).transform().isIdentity());
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testUnsettingAllTableProperties()
    {
        String tableName = "test_unsetting_all_table_properties_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1, format = 'PARQUET', partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);
        BaseTable table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 1);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("PARQUET"));
        assertTrue(table.spec().isPartitioned());
        List<PartitionField> partitionFields = table.spec().fields();
        assertThat(partitionFields).hasSize(1);
        assertEquals(partitionFields.get(0).name(), "regionkey");
        assertTrue(partitionFields.get(0).transform().isIdentity());

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = DEFAULT, format = DEFAULT, partitioning = DEFAULT");
        table = loadTable(tableName);
        assertEquals(table.operations().current().formatVersion(), 2);
        assertTrue(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT).equalsIgnoreCase("ORC"));
        assertTrue(table.spec().isUnpartitioned());
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testDeletingEntireFile()
    {
        String tableName = "test_deleting_entire_file_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation WITH NO DATA", 0);
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 1", "SELECT count(*) FROM nation WHERE regionkey = 1");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey != 1", "SELECT count(*) FROM nation WHERE regionkey != 1");

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey <= 2", "SELECT count(*) FROM nation WHERE regionkey <= 2");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey > 2");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
    }

    @Test
    public void testDeletingEntireFileFromPartitionedTable()
    {
        String tableName = "test_deleting_entire_file_from_partitioned_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT, b INT) WITH (partitioning = ARRAY['a'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (1, 3), (1, 5), (2, 1), (2, 3), (2, 5)", 6);
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2), (1, 4), (1, 6), (2, 2), (2, 4), (2, 6)", 6);

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(4);
        assertUpdate("DELETE FROM " + tableName + " WHERE b % 2 = 0", 6);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1), (1, 3), (1, 5), (2, 1), (2, 3), (2, 5)");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
    }

    @Test
    public void testDeletingEntireFileWithNonTupleDomainConstraint()
    {
        String tableName = "test_deleting_entire_file_with_non_tuple_domain_constraint" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation WITH NO DATA", 0);
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey = 1", "SELECT count(*) FROM nation WHERE regionkey = 1");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation WHERE regionkey != 1", "SELECT count(*) FROM nation WHERE regionkey != 1");

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(2);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey % 2 = 1", "SELECT count(*) FROM nation WHERE regionkey % 2 = 1");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey % 2 = 0");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(1);
    }

    @Test
    public void testDeletingEntirePartitionedTable()
    {
        String tableName = "test_deleting_entire_partitioned_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", 25);

        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(5);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey < 10", "SELECT count(*) FROM nation WHERE regionkey < 10");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(0);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey < 10");
        assertThat(this.loadTable(tableName).newScan().planFiles()).hasSize(0);
    }

    private Table updateTableToV2(String tableName)
    {
        BaseTable table = loadTable(tableName);
        TableOperations operations = table.operations();
        TableMetadata currentMetadata = operations.current();
        operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(2));

        return table;
    }

    private BaseTable loadTable(String tableName)
    {
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                CachingHiveMetastore.memoizeMetastore(metastore, 1000),
                hdfsEnvironment,
                new TestingTypeManager(),
                tableOperationsProvider,
                "test",
                false,
                false,
                false);
        return (BaseTable) loadIcebergTable(catalog, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));
    }
}
