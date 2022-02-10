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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CachedConnectorMetadata;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

/**
 * the tests below are based on the tests from TestJdbcMetadata
 *
 * @see TestJdbcMetadata
 */
@Test(singleThreaded = true)
public class TestCachingJdbcMetadata
{
    private static final Logger LOGGER = Logger.get(TestCachingJdbcMetadata.class);
    private TestingDatabase database;
    private CachedConnectorMetadata metadata;
    private JdbcMetadataConfig config;
    private ConnectorTableHandle tableHandle;
    private ConnectorSession session;
    private static final long CACHE_TTL = 500; // ms
    private ConnectorTableHandle cachingTableHandle;
    private SchemaTableName cachingSchemaTableName;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
            throws Exception
    {
        session = new TestingConnectorSession(ImmutableList.of())
        {
            @Override
            public Optional<String> getCatalog()
            {
                return Optional.of("mycatalog");
            }
        };
        database = new TestingDatabase();
        config = new JdbcMetadataConfig()
                .setMetadataCacheEnabled(true)
                .setMetadataCacheTtl(Duration.valueOf(CACHE_TTL + "ms"))
                .setMetadataCacheMaximumSize(1000);
        metadata = new CachedConnectorMetadata(new JdbcMetadata(database.getJdbcClient(), false), Duration.valueOf(CACHE_TTL + "ms"), 1000);
        tableHandle = metadata.getTableHandle(session, new SchemaTableName("example", "numbers"));
        cachingSchemaTableName = new SchemaTableName("cachingschema", "cachingtable");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();

        // expire cache between tests
        Thread.sleep(CACHE_TTL + 50);
    }

    @Test
    public void testListTables() throws SQLException, InterruptedException
    {
        // all schemas should return all tables
        assertEquals(ImmutableSet.copyOf(metadata.listTables(session, Optional.empty())), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("exa_ple", "table_with_float_col"),
                new SchemaTableName("exa_ple", "num_ers")));

        // add a new schema and table
        database.getConnection().createStatement().execute("CREATE SCHEMA cachingschema");
        database.getConnection().createStatement().execute("CREATE TABLE cachingschema.cachingtable(id varchar primary key)");

        // all schemas should still return all tables, including the new one since there is no caching when schema is not specified
        assertEquals(ImmutableSet.copyOf(metadata.listTables(session, Optional.empty())), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("exa_ple", "table_with_float_col"),
                new SchemaTableName("exa_ple", "num_ers"),
                new SchemaTableName("cachingschema", "cachingtable")));

        // get tables for cachingschema, since this is first time, it will now be cached
        assertEquals(ImmutableSet.copyOf(metadata.listTables(session, Optional.of("cachingschema"))), ImmutableSet.of(
                new SchemaTableName("cachingschema", "cachingtable")));

        // add cachingtable2
        database.getConnection().createStatement().execute("CREATE TABLE cachingschema.cachingtable2(id varchar primary key)");

        // cached tables should be returned, without cachingtable2
        assertEquals(ImmutableSet.copyOf(metadata.listTables(session, Optional.of("cachingschema"))), ImmutableSet.of(
                new SchemaTableName("cachingschema", "cachingtable")));

        // wait for cache to expire
        Thread.sleep(CACHE_TTL + 50);

        // should now also return cachingtable2
        assertEquals(ImmutableSet.copyOf(metadata.listTables(session, Optional.of("cachingschema"))), ImmutableSet.of(
                new SchemaTableName("cachingschema", "cachingtable"),
                new SchemaTableName("cachingschema", "cachingtable2")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(session, Optional.of("unknown"))), ImmutableSet.of());
    }

    @Test
    public void testGetTableHandle() throws Exception
    {
        // add cachingtable
        database.getConnection().createStatement().execute("CREATE SCHEMA cachingschema");
        database.getConnection().createStatement().execute("CREATE TABLE cachingschema.cachingtable(id varchar primary key)");

        // cache will be created and should get handle back
        assertEquals(metadata.getTableHandle(session, cachingSchemaTableName),
                cachingTableHandle);

        // now drop the table and get table handle again, this should still work bc of cache
        database.getConnection().createStatement().execute("DROP TABLE cachingschema.cachingtable");
        assertEquals(metadata.getTableHandle(session, cachingSchemaTableName),
                cachingTableHandle);

        // wait for cache to expire
        Thread.sleep(CACHE_TTL + 50);

        // table is dropped and cache is expired, should not get a handle
        assertNull(metadata.getTableHandle(session, cachingSchemaTableName));

        // other cases copied from TestJdbcMetadata
        assertEquals(metadata.getTableHandle(session, new SchemaTableName("example", "numbers")), tableHandle);
        assertNull(metadata.getTableHandle(session, new SchemaTableName("example", "unknown")));
        assertNull(metadata.getTableHandle(session, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(session, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void getTableMetadata() throws SQLException, InterruptedException
    {
        // add cachingtable
        database.getConnection().createStatement().execute("CREATE SCHEMA cachingschema");
        database.getConnection().createStatement().execute("CREATE TABLE cachingschema.cachingtable(id varchar primary key)");

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, metadata.getTableHandle(session, cachingSchemaTableName));
        assertEquals(tableMetadata.getTable(), cachingSchemaTableName);
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("id", VARCHAR, false, null, null, false, emptyMap())));

        database.getConnection().createStatement().execute("ALTER TABLE cachingschema.cachingtable ADD COLUMN value VARCHAR(50)");

        // wait for cache to expire
        Thread.sleep(CACHE_TTL + 50);

        assertEquals(metadata.getTableMetadata(session, metadata.getTableHandle(session, cachingSchemaTableName)).getColumns(), ImmutableList.of(
                new ColumnMetadata("id", VARCHAR, false, null, null, false, emptyMap()), // primary key is not null in H2
                new ColumnMetadata("value", createVarcharType(50))));

        // known table
        tableMetadata = metadata.getTableMetadata(session, tableHandle);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("example", "numbers"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("text", VARCHAR, false, null, null, false, emptyMap()), // primary key is not null in H2
                new ColumnMetadata("text_short", createVarcharType(32)),
                new ColumnMetadata("value", BIGINT)));

        // escaping name patterns
        ConnectorTableHandle specialTableHandle = metadata.getTableHandle(session, new SchemaTableName("exa_ple", "num_ers"));
        ConnectorTableMetadata specialTableMetadata = metadata.getTableMetadata(session, specialTableHandle);
        assertEquals(specialTableMetadata.getTable(), new SchemaTableName("exa_ple", "num_ers"));
        assertEquals(specialTableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("te_t", VARCHAR, false, null, null, false, emptyMap()), // primary key is not null in H2
                new ColumnMetadata("va%ue", BIGINT)));

        // unknown tables should produce null
        unknownTableMetadata(new JdbcTableHandle(new SchemaTableName("unknown", "unknown"), null, "unknown", "unknown"));
        unknownTableMetadata(new JdbcTableHandle(new SchemaTableName("example", "unknown"), null, "example", "unknown"));
        unknownTableMetadata(new JdbcTableHandle(new SchemaTableName("unknown", "numbers"), null, "unknown", "numbers"));
    }

    private void unknownTableMetadata(JdbcTableHandle tableHandle)
    {
        try {
            metadata.getTableMetadata(session, tableHandle);
            fail("Expected getTableMetadata of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException ignored) {
            LOGGER.error("unknownTableMetadata error : %s", ignored.getMessage());
        }
    }

    @Test
    public void testListTableColumns() throws SQLException, InterruptedException
    {
        // add cachingtable
        database.getConnection().createStatement().execute("CREATE SCHEMA cachingschema");
        database.getConnection().createStatement().execute("CREATE TABLE cachingschema.cachingtable(id varchar primary key)");

        // should only have one column
        SchemaTablePrefix tablePrefix = new SchemaTablePrefix("cachingschema", "cachingtable");
        assertEquals(metadata.listTableColumns(session, tablePrefix), ImmutableMap.of(cachingSchemaTableName, ImmutableList.of(
                new ColumnMetadata("id", VARCHAR, false, null, null, false, emptyMap()))));

        // add a new column
        database.getConnection().createStatement().execute("ALTER TABLE cachingschema.cachingtable ADD COLUMN value VARCHAR(50)");

        // should still only have one column bc it is cached
        assertEquals(metadata.listTableColumns(session, tablePrefix), ImmutableMap.of(cachingSchemaTableName, ImmutableList.of(
                new ColumnMetadata("id", VARCHAR, false, null, null, false, emptyMap()))));

        // wait for cache to expire
        Thread.sleep(CACHE_TTL + 50);

        // should now see the new column
        assertEquals(metadata.listTableColumns(session, tablePrefix), ImmutableMap.of(cachingSchemaTableName, ImmutableList.of(
                new ColumnMetadata("id", VARCHAR, false, null, null, false, emptyMap()),
                new ColumnMetadata("value", createVarcharType(50)))));
    }

    @Test
    public void testGetColumnHandles() throws InterruptedException, SQLException
    {
        // add cachingtable
        database.getConnection().createStatement().execute("CREATE SCHEMA cachingschema");
        database.getConnection().createStatement().execute("CREATE TABLE cachingschema.cachingtable(id varchar primary key)");

        // should only have one column
        cachingTableHandle = metadata.getTableHandle(session, cachingSchemaTableName);
        assertEquals(metadata.getColumnHandles(session, cachingTableHandle), ImmutableMap.of(
                "id", new JdbcColumnHandle("ID", JDBC_VARCHAR, VARCHAR, true)));

        // add a new column
        database.getConnection().createStatement().execute("ALTER TABLE cachingschema.cachingtable ADD COLUMN value VARCHAR(50)");

        // should still only have one column bc it is cached
        assertEquals(metadata.getColumnHandles(session, cachingTableHandle), ImmutableMap.of(
                "id", new JdbcColumnHandle("ID", JDBC_VARCHAR, VARCHAR, true)));

        // wait for cache to expire
        Thread.sleep(CACHE_TTL + 50);

        // should now see the new column
        assertEquals(metadata.getColumnHandles(session, cachingTableHandle), ImmutableMap.of(
                "id", new JdbcColumnHandle("ID", JDBC_VARCHAR, VARCHAR, true),
                "value", new JdbcColumnHandle("VALUE", JDBC_VARCHAR, createVarcharType(50), true)));

        // copied from TestJdbcMetadata
        // known table
        assertEquals(metadata.getColumnHandles(session, tableHandle), ImmutableMap.of(
                "text", new JdbcColumnHandle("TEXT", JDBC_VARCHAR, VARCHAR, true),
                "text_short", new JdbcColumnHandle("TEXT_SHORT", JDBC_VARCHAR, createVarcharType(32), true),
                "value", new JdbcColumnHandle("VALUE", JDBC_BIGINT, BIGINT, true)));

        // unknown table
        unknownTableColumnHandle(new JdbcTableHandle(new SchemaTableName("unknown", "unknown"), "unknown", "unknown", "unknown"));
        unknownTableColumnHandle(new JdbcTableHandle(new SchemaTableName("example", "unknown"), null, "example", "unknown"));
    }

    private void unknownTableColumnHandle(JdbcTableHandle tableHandle)
    {
        try {
            metadata.getColumnHandles(session, tableHandle);
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException ignored) {
            LOGGER.error("unknownTableColumnHandle error : %s", ignored.getMessage());
        }
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(
                metadata.getColumnMetadata(session, tableHandle, new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR, true)),
                new ColumnMetadata("text", VARCHAR));
    }

    @Test
    public void testDropTableTable()
    {
        try {
            metadata.dropTable(session, tableHandle);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), PERMISSION_DENIED.toErrorCode());
        }

        metadata = new CachedConnectorMetadata(new JdbcMetadata(database.getJdbcClient(), true), Duration.valueOf(CACHE_TTL + "ms"), 1000);
        metadata.dropTable(session, tableHandle);

        try {
            metadata.getTableMetadata(session, tableHandle);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }
}
