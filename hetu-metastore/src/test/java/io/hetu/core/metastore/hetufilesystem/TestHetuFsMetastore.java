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

package io.hetu.core.metastore.hetufilesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.ColumnEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.metastore.model.TableEntityType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.hetu.core.metastore.MetaStoreConstants.LOCAL;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.CATALOG_NOT_EMPTY;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.prestosql.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHetuFsMetastore
{
    private static final Logger LOG = Logger.get(TestHetuFsMetastore.class);
    private HetuMetastore metastore;
    private HetuFileSystemClient client;
    private CatalogEntity defaultCatalog;
    private DatabaseEntity defaultDatabase;
    private String path = Resources.getResource("").getPath() + File.separator + "metastore";
    private final String user = "user";
    private final String password = "testpass";
    private final String owner = "user1";
    private final String viewData = "select * from table";
    private final String typeInt = "bigint";
    private final String typeVarchar = "array(varchar(32))";

    private boolean testResult = true;

    /**
     * setUp
     *
     * @throws Exception Exception
     */
    @BeforeClass
    public void setUp()
    {
        try {
            Map<String, String> config = new ImmutableMap.Builder<String, String>()
                    .put("hetu.metastore.hetufilesystem.path", path)
                    .put("hetu.metastore.hetufilesystem.profile-name", "local-config-catalog")
                    .build();

            LocalConfig localConfig = new LocalConfig(null);
            client = new HetuLocalFileSystemClient(localConfig, Paths.get(path));

            if (!client.exists(Paths.get(path))) {
                client.createDirectories(Paths.get(path));
            }
            client.deleteRecursively(Paths.get(path));

            String type = LOCAL;
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new HetuFsMetastoreModule(client, type));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            metastore = injector.getInstance(HetuMetastore.class);
        }
        catch (Exception ex) {
            throwIfUnchecked(ex);
            throw new PrestoException(HETU_METASTORE_CODE,
                    "init hetu metastore module failed.");
        }
        defaultCatalog = CatalogEntity.builder()
                .setCatalogName("hetu")
                .setOwner("hetu1").build();
        metastore.createCatalog(defaultCatalog);

        defaultDatabase = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db1").build();
        metastore.createDatabase(defaultDatabase);
    }

    /**
     * tearDown
     *
     * @throws Exception Exception
     */
    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        try {
            client.deleteRecursively(Paths.get(path));
        }
        catch (IOException e) {
            LOG.info("Error message: " + e.getStackTrace());
        }
    }

    /**
     * test create catalog
     */
    @Test
    public void testCreateCatalog()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();

        CatalogEntity vdm = CatalogEntity.builder()
                .setCatalogName("catalog1")
                .setOwner("root1")
                .setComment(Optional.of("Hetu VDM"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(vdm);

        CatalogEntity vdm1 = CatalogEntity.builder()
                .setCatalogName("catalog2")
                .setOwner("root2")
                .setComment(Optional.of("Hetu VDM"))
                .setParameters(emptyMap())
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(vdm1);

        try {
            metastore.createCatalog(vdm);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), ALREADY_EXISTS.toErrorCode());
        }

        metastore.dropCatalog(vdm.getName());
        metastore.dropCatalog(vdm1.getName());
    }

    /**
     * test create catalog parallel
     */
    @Test
    public void testCreateCatalogParallel()
            throws InterruptedException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();
        CatalogEntity catalog = CatalogEntity.builder()
                .setCatalogName("catalog100")
                .setOwner("root1")
                .setComment(Optional.of("create catalog parallel"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();

        testResult = true;
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                try {
                    metastore.createCatalogIfNotExist(catalog);
                }
                catch (PrestoException e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(testResult);
        metastore.dropCatalog(catalog.getName());
    }

    /**
     * test drop dropCatalog
     */
    @Test
    public void testDropCatalog()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive1.metastore", "129.0.0.1")
                .put("hive1.config", "core-site.xml")
                .build();

        CatalogEntity hive = CatalogEntity.builder()
                .setCatalogName("hive")
                .setOwner("root3")
                .setComment(Optional.of("hive database source"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();

        metastore.createCatalog(hive);

        // drop not exist catalog
        try {
            metastore.dropCatalog("noExistCatalog");
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }

        // drop catalog not empty
        DatabaseEntity db1 = DatabaseEntity.builder()
                .setCatalogName(hive.getName())
                .setDatabaseName("db1")
                .setOwner("root4")
                .build();
        metastore.createDatabase(db1);
        try {
            metastore.dropCatalog(hive.getName());
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), CATALOG_NOT_EMPTY.toErrorCode());
        }

        metastore.dropDatabase(db1.getCatalogName(), db1.getName());
        metastore.dropCatalog(hive.getName());
    }

    /**
     * test get catalog
     */
    @Test
    public void testGetCatalog()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "126.0.0.1")
                .put("hive.config", "hdfs-site.xml")
                .build();

        CatalogEntity catalog1 = CatalogEntity.builder()
                .setCatalogName("hive2")
                .setOwner("root5")
                .setComment(Optional.of("hive database source"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();

        metastore.createCatalog(catalog1);
        Optional<CatalogEntity> catalogInfo = metastore.getCatalog(catalog1.getName());
        assertTrue(catalogInfo.isPresent());
        assertEquals(catalog1.getParameters(), catalogInfo.get().getParameters());
        assertEquals(catalogInfo.get(), catalog1);

        Optional<CatalogEntity> catalogInfo1 = metastore.getCatalog("mysql");
        assertFalse(catalogInfo1.isPresent());
    }

    /**
     * test get All catalogs
     */
    @Test
    public void testGetAllCatalogs()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "130.0.0.1")
                .put("hive.config", "hdfs-site.xml")
                .build();

        CatalogEntity catalog2 = CatalogEntity.builder()
                .setCatalogName("catalog2")
                .setOwner("root6")
                .setComment(Optional.of("mysql database source"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog2);

        CatalogEntity catalog3 = CatalogEntity.builder()
                .setCatalogName("catalog3")
                .setOwner("root7")
                .setComment(Optional.of("mysql1 database source"))
                .setParameters(emptyMap())
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog3);

        CatalogEntity catalog4 = CatalogEntity.builder()
                .setCatalogName("catalog4")
                .setOwner("root8")
                .setComment(Optional.of("oracle database source"))
                .setParameters(emptyMap())
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog4);

        final int catalogNumer = 3;
        assertTrue(metastore.getCatalogs().size() > catalogNumer);
        List<CatalogEntity> catalogs = new ArrayList<>();
        metastore.getCatalogs().stream().forEach(catalog -> {
            String name = catalog.getName();
            if (name.equals(catalog2.getName())
                    || name.equals(catalog3.getName())
                    || name.equals(catalog4.getName())) {
                catalogs.add(catalog);
            }
        });
        Collections.sort(catalogs, new Comparator<CatalogEntity>()
        {
            @Override
            public int compare(CatalogEntity o1, CatalogEntity o2)
            {
                return o1.getName().compareTo(o2.getName());
            }
        });
        assertEquals(ImmutableList.copyOf(catalogs), ImmutableList.of(catalog2, catalog3, catalog4));
    }

    /**
     * test alter catalog
     */
    @Test
    public void testAlterCatalog()
    {
        String oldCatalogName = "catalog5";
        CatalogEntity catalog5 = CatalogEntity.builder()
                .setCatalogName(oldCatalogName)
                .setOwner("root8")
                .setComment(Optional.of("mysql3 database source"))
                .setParameters(emptyMap())
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog5);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("url11", "117.0.0.1")
                .put("user1", "testcreate")
                .build();
        CatalogEntity catalog6 = CatalogEntity.builder()
                .setCatalogName("catalog6")
                .setOwner("root9")
                .setComment(Optional.of("mysql5 database source"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        try {
            metastore.alterCatalog(oldCatalogName, catalog6);
            fail("Cannot alter a catalog's name");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HETU_METASTORE_CODE.toErrorCode());
        }

        try {
            metastore.alterCatalog(catalog6.getName(), catalog6);
            fail(format("Catalog '%s' does not exist:", catalog6.getName()));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }

        catalog6.setName(oldCatalogName);
        metastore.alterCatalog(oldCatalogName, catalog6);
        Optional<CatalogEntity> catalog7 = metastore.getCatalog(catalog6.getName());
        assertTrue(catalog7.isPresent());
        assertCatalogEquals(catalog7.get(), catalog6);
        assertEquals(catalog7.get().getCreateTime(), catalog5.getCreateTime());
    }

    /**
     * test alter catalog parallel
     */
    @Test
    public void testAlterCatalogParallel()
            throws InterruptedException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();
        CatalogEntity catalog = CatalogEntity.builder()
                .setCatalogName("catalog200")
                .setOwner("root1")
                .setComment(Optional.of("create catalog"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog);

        CatalogEntity newCatalog = CatalogEntity.builder()
                .setCatalogName("catalog200")
                .setOwner("root9")
                .setComment(Optional.of("alter catalog parallel"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();

        testResult = true;
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                try {
                    metastore.alterCatalog(catalog.getName(), newCatalog);
                }
                catch (PrestoException e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(testResult);
        metastore.dropCatalog(catalog.getName());
    }

    /**
     * test create database
     */
    @Test
    public void testCreateDatabase()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();

        DatabaseEntity db10 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db10")
                .setOwner("root9")
                .setComment(Optional.of("Hetu create database."))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(db10);

        // database exist
        try {
            metastore.createDatabase(db10);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), ALREADY_EXISTS.toErrorCode());
        }

        // catalog not exist
        try {
            db10.setCatalogName("notexist");
            metastore.createDatabase(db10);
            fail(format("Catalog '%s' does not exist:", db10.getCatalogName()));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }

    /**
     * test create database parallel
     */
    @Test
    public void testCreateDatabaseParallel()
            throws InterruptedException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();

        DatabaseEntity database = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("database100")
                .setOwner("root9")
                .setComment(Optional.of("Hetu create database."))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();

        testResult = true;
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                try {
                    metastore.createDatabaseIfNotExist(database);
                }
                catch (PrestoException e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(testResult);
        metastore.dropDatabase(database.getCatalogName(), database.getName());
    }

    /**
     * test drop database
     */
    @Test
    public void testDropDatabase()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("url", "127.0.0.1")
                .put("user", "test")
                .build();

        DatabaseEntity db20 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db20")
                .setOwner("root11")
                .setComment(Optional.of("Hetu schema"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();

        metastore.createDatabase(db20);
        metastore.dropDatabase(db20.getCatalogName(), db20.getName());

        String vschemaName3 = "vschema30";

        DatabaseEntity db30 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db20")
                .setOwner("root10")
                .setComment(Optional.of("Hetu schema"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(db30);

        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                viewData,
                Optional.of(vschemaName3),
                Optional.of(vschemaName3),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn(vschemaName3, parseTypeSignature(typeInt)),
                        new ConnectorViewDefinition.ViewColumn(vschemaName3, parseTypeSignature(typeVarchar))),
                Optional.of(owner),
                false);

        TableEntity table = TableEntity.builder()
                .setCatalogName(db30.getCatalogName())
                .setDatabaseName(db30.getName())
                .setTableName("view1")
                .setOwner("root10")
                .setComment("Hetu View")
                .setViewOriginalText(Optional.of(definition.toString()))
                .setTableType(TableEntityType.VIRTUAL_VIEW.toString())
                .build();
        metastore.createTable(table);

        try {
            metastore.dropDatabase(db30.getCatalogName(), db30.getName());
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), SCHEMA_NOT_EMPTY.toErrorCode());
        }
    }

    /**
     * test get database
     */
    @Test
    public void testGetDatabase()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("url", "127.0.0.1")
                .put(user, "test")
                .build();

        String dbName405 = "db405";
        DatabaseEntity db405 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(dbName405)
                .setOwner(dbName405)
                .setComment(Optional.of("Hetu database40"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(db405);
        Optional<DatabaseEntity> db = metastore.getDatabase(defaultCatalog.getName(), db405.getName());
        assertTrue(db.isPresent());
        assertEquals(db.get(), db405);
    }

    /**
     * test get all databases
     */
    @Test
    public void testAllDatabases()
    {
        Map<String, String> props1 = ImmutableMap.<String, String>builder()
                .put("location", "hdfs://db40")
                .put("partition", "p1")
                .build();

        DatabaseEntity db40 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db40")
                .setOwner("root40")
                .setComment(Optional.of("Hetu database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(props1)
                .build();
        metastore.createDatabase(db40);

        Map<String, String> props2 = ImmutableMap.<String, String>builder()
                .put("location2", "hdfs://db401")
                .put("partition2", "p2")
                .build();
        DatabaseEntity db401 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db401")
                .setOwner("root401")
                .setComment(Optional.of("Hetu database 401"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(props2)
                .build();
        metastore.createDatabase(db401);

        ImmutableList.Builder<DatabaseEntity> databasesBuilder = ImmutableList.builder();
        metastore.getAllDatabases(defaultCatalog.getName()).stream().forEach(db -> {
            String name = db.getName();
            if (name.equals(db40.getName())
                    || name.equals(db401.getName())) {
                databasesBuilder.add(db);
            }
        });

        ImmutableList<DatabaseEntity> databases = databasesBuilder.build();
        assertEquals(databases.size(), 2);
        ImmutableList<DatabaseEntity> checkDatabases = ImmutableList.of(db40, db401);
        assertTrue(checkDatabases.contains(databases.get(0)));
        assertTrue(checkDatabases.contains(databases.get(1)));
    }

    /**
     * test alter database
     */
    @Test
    public void testAlterDatabase()
    {
        DatabaseEntity db50 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db50")
                .setOwner("root50")
                .setComment(Optional.of("Hetu database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(emptyMap())
                .build();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("location", "hdfs://db001")
                .build();

        TableEntity table = TableEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db50")
                .setTableName("table50")
                .setTableType("view")
                .setOwner("root50")
                .setComment("Hetu table")
                .setCreateTime(System.currentTimeMillis())
                .setParameters(emptyMap())
                .build();

        metastore.createDatabase(db50);
        metastore.createTable(table);

        Optional<TableEntity> checkTable = metastore.getTable(defaultCatalog.getName(), db50.getName(), "table50");
        assertTrue(checkTable.isPresent());
        assertTrue(checkTable.get().equals(table));

        // alter database properties and database name
        DatabaseEntity newDb = DatabaseEntity.builder(db50)
                .setDatabaseName("newdb50")
                .setParameters(properties)
                .build();
        metastore.alterDatabase(defaultCatalog.getName(), db50.getName(), newDb);
        Optional<DatabaseEntity> originalDb = metastore.getDatabase(defaultCatalog.getName(), db50.getName());
        assertFalse(originalDb.isPresent());
        Optional<DatabaseEntity> checkDb = metastore.getDatabase(defaultCatalog.getName(), newDb.getName());
        assertTrue(checkDb.isPresent());
        assertTrue(checkDb.get().equals(newDb));

        // can show all view.
        TableEntity newTable = TableEntity.builder(table).setDatabaseName(newDb.getName()).build();
        checkTable = metastore.getTable(defaultCatalog.getName(), newDb.getName(), "table50");
        assertTrue(checkTable.isPresent());
        assertTrue(checkTable.get().equals(newTable));

        DatabaseEntity alterDb = DatabaseEntity.builder(db50).setCatalogName("hetu1").build();
        try {
            metastore.alterDatabase(db50.getCatalogName(), db50.getName(), alterDb);
            fail(format("alter database cannot cross catalog[%s,%s]", db50.getCatalogName(), alterDb.getName()));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HETU_METASTORE_CODE.toErrorCode());
        }
    }

    /**
     * test alter database parallel
     */
    @Test
    public void testAlterDatabaseParallel()
            throws InterruptedException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();

        DatabaseEntity database = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("database200")
                .setOwner("root9")
                .setComment(Optional.of("Hetu create database."))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(database);

        DatabaseEntity newDatabase = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("database200")
                .setOwner("root10")
                .setComment(Optional.of("alter database parallel"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();

        testResult = true;
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                try {
                    metastore.alterDatabase(database.getCatalogName(), database.getName(), newDatabase);
                }
                catch (PrestoException e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(testResult);
        metastore.dropDatabase(database.getCatalogName(), database.getName());
    }

    /**
     * test get table
     */
    @Test
    public void testGetTable()
    {
        String dbName7 = "db70";
        DatabaseEntity db70 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(dbName7)
                .build();
        metastore.createDatabase(db70);

        String tableName = "table1";
        SchemaTableName viewName = new SchemaTableName(db70.getName(), tableName);
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                viewData,
                Optional.of(dbName7),
                Optional.of(dbName7),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn(dbName7, parseTypeSignature(typeInt)),
                        new ConnectorViewDefinition.ViewColumn(dbName7 + tableName,
                                parseTypeSignature(typeVarchar))),
                Optional.of(owner),
                false);

        TableEntity table = TableEntity.builder()
                .setCatalogName(db70.getCatalogName())
                .setDatabaseName(db70.getName())
                .setTableName(viewName.getTableName())
                .setOwner(dbName7)
                .setTableType(TableEntityType.VIRTUAL_VIEW.toString())
                .setColumns(definition.getColumns().stream().map(column ->
                        new ColumnEntity(column.getName(),
                                column.getType().toString(), "View column", emptyMap()))
                        .collect(toList()))
                .build();

        metastore.createTable(table);
        // table exist
        Optional<TableEntity> tableEntity = metastore.getTable(table.getCatalogName(),
                table.getDatabaseName(), table.getName());
        assertTrue(tableEntity.isPresent());
        assertEquals(tableEntity.get(), table);

        // table not exist
        assertEquals(metastore.getTable(db70.getCatalogName(), dbName7, "table2"), Optional.empty());
    }

    /**
     * test get all table
     */
    @Test
    public void testGetAllTables()
    {
        String dbName8 = "db80";
        DatabaseEntity db80 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(dbName8)
                .build();
        metastore.createDatabase(db80);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bucket", "yes")
                .put("index", "0")
                .build();
        String tableName5 = "table5";
        TableEntity table = TableEntity.builder()
                .setCatalogName(db80.getCatalogName())
                .setDatabaseName(dbName8)
                .setTableName(tableName5)
                .setOwner(dbName8)
                .setTableType(TableEntityType.TABLE.toString()).setColumns(
                        ImmutableList.of(new ColumnEntity(dbName8, parseTypeSignature(typeInt).toString(),
                                        "dbName8 column", emptyMap()),
                                new ColumnEntity(dbName8 + tableName5, parseTypeSignature(typeVarchar).toString(),
                                        "table1 column", properties)))
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createTable(table);

        TableEntity table1 = TableEntity.builder()
                .setCatalogName(db80.getCatalogName())
                .setDatabaseName(dbName8)
                .setTableName(dbName8 + tableName5)
                .setOwner(dbName8)
                .setTableType(TableEntityType.TABLE.toString()).setColumns(
                        ImmutableList.of(new ColumnEntity(dbName8, parseTypeSignature(typeInt).toString(),
                                        "dbName8 column", emptyMap()),
                                new ColumnEntity(dbName8 + tableName5, parseTypeSignature(typeVarchar).toString(),
                                        "table column", properties)))
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createTable(table1);

        assertEquals(ImmutableSet.copyOf(metastore.getAllTables(db80.getCatalogName(), dbName8)), ImmutableSet.of(
                table, table1));
    }

    /**
     * testCreateView
     */
    @Test
    public void testCreateTable()
    {
        String dbName9 = "db90";
        DatabaseEntity db90 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(dbName9)
                .build();
        metastore.createDatabase(db90);
        SchemaTableName viewName1 = new SchemaTableName(dbName9, "testView");

        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                viewData,
                Optional.of(dbName9),
                Optional.of(dbName9),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn(dbName9, parseTypeSignature(typeInt)),
                        new ConnectorViewDefinition.ViewColumn(dbName9 + 1, parseTypeSignature(typeVarchar))),
                Optional.of(owner),
                false);

        Map<String, String> props1 = ImmutableMap.<String, String>builder()
                .put("rowNumber", "128")
                .put("columnNumber", "2")
                .put("totalFile", "128")
                .build();

        TableEntity mv = TableEntity.builder()
                .setCatalogName(db90.getCatalogName())
                .setDatabaseName(dbName9)
                .setTableName(viewName1.getTableName())
                .setTableType(TableEntityType.MATERIALIZED_VIEW.toString())
                .setViewOriginalText(Optional.of(definition.toString()))
                .setColumns(ImmutableList.of(new ColumnEntity(dbName9, parseTypeSignature(typeInt).toString(),
                                "dbName9 column", emptyMap()),
                        new ColumnEntity(dbName9 + 1, parseTypeSignature(typeVarchar).toString(),
                                "table column", emptyMap())))
                .setParameters(props1)
                .build();

        metastore.createTable(mv);
        // table exist
        try {
            metastore.createTable(mv);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), ALREADY_EXISTS.toErrorCode());
        }

        // database not exist
        try {
            mv.setDatabaseName("dbName9");
            metastore.createTable(mv);
            fail(format("Database '%s.%s' does not exist:", mv.getCatalogName(), mv.getDatabaseName()));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }

    /**
     * test create table parallel
     */
    @Test
    public void testCreateTableParallel()
            throws InterruptedException
    {
        String tableName = "table100";
        SchemaTableName schemaTableName = new SchemaTableName(defaultDatabase.getName(), tableName);
        TableEntity tableEntity = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(schemaTableName.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();

        testResult = true;
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                try {
                    metastore.createTableIfNotExist(tableEntity);
                }
                catch (PrestoException e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(testResult);
        metastore.dropTable(tableEntity.getCatalogName(), tableEntity.getDatabaseName(), tableName);
    }

    /**
     * test drop table
     */
    @Test
    public void testDropTable()
    {
        String dbName11 = "db110";

        DatabaseEntity db11 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(dbName11)
                .build();
        metastore.createDatabase(db11);

        // for create table in dbName11
        SchemaTableName tableName1 = new SchemaTableName(dbName11, "testview");
        TableEntity table = TableEntity.builder()
                .setCatalogName(db11.getCatalogName())
                .setDatabaseName(db11.getName())
                .setTableName(tableName1.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(table);
        metastore.dropTable(table.getCatalogName(), table.getDatabaseName(), table.getName());

        try {
            metastore.dropTable(table.getCatalogName(), table.getDatabaseName(), table.getName());
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }

        assertEquals(metastore.getTable(table.getCatalogName(), table.getDatabaseName(), table.getName()),
                Optional.empty());
    }

    /**
     * test alter table
     */
    @Test
    public void testAlterTable()
    {
        String dbName13 = "db130";

        DatabaseEntity db13 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(dbName13)
                .build();
        metastore.createDatabase(db13);

        // for create table in dbName11
        String table130 = "table130";
        TableEntity table = TableEntity.builder()
                .setCatalogName(db13.getCatalogName())
                .setDatabaseName(db13.getName())
                .setTableName(table130)
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(table);

        String newTableName = "new130";
        TableEntity newTable = TableEntity.builder(table)
                .setTableName(newTableName)
                .setOwner(owner)
                .setComment("alter table")
                .build();

        // alter table
        metastore.alterTable(table.getCatalogName(), table.getDatabaseName(), table.getName(), newTable);
        try {
            metastore.alterTable(table.getCatalogName(), table.getDatabaseName(), table.getName(), newTable);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }

        Optional<TableEntity> newTable1 = metastore.getTable(newTable.getCatalogName(),
                newTable.getDatabaseName(), newTable.getName());
        assertTrue(newTable1.isPresent());
        assertTableEquals(newTable1.get(), newTable);
    }

    /**
     * test alter table parallel
     */
    @Test
    public void testAlterTableParallel()
            throws InterruptedException
    {
        String tableName = "table200";
        TableEntity tableEntity1 = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(tableName)
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(tableEntity1);

        TableEntity tableEntity2 = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(tableName)
                .setTableType(TableEntityType.TABLE.toString())
                .setComment("alter table parallel")
                .build();

        testResult = true;
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                try {
                    metastore.alterTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName, tableEntity2);
                }
                catch (PrestoException e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(testResult);
        metastore.dropTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName);
    }

    /**
     * test alter view
     */
    @Test
    public void testAlterView()
    {
        String dbName15 = "db150";

        DatabaseEntity db15 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(dbName15)
                .build();
        metastore.createDatabase(db15);

        // alter view
        String table131 = "table131";
        TableEntity view = TableEntity.builder()
                .setCatalogName(db15.getCatalogName())
                .setDatabaseName(db15.getName())
                .setTableName(table131)
                .setTableType(TableEntityType.VIRTUAL_VIEW.toString())
                .setViewOriginalText(Optional.of(viewData))
                .setColumns(ImmutableList.of(new ColumnEntity(table131, parseTypeSignature(typeInt).toString(),
                        "table131 column", emptyMap())))
                .build();
        metastore.createTable(view);

        String newViewName = "new131";
        TableEntity newView = TableEntity.builder(view)
                .setTableName(newViewName)
                .setOwner(owner)
                .setComment("alter view")
                .setViewOriginalText(Optional.of("select * from t1"))
                .setColumns(ImmutableList.of(new ColumnEntity(newViewName, parseTypeSignature(typeVarchar).toString(),
                        "new131 column", emptyMap())))
                .build();

        metastore.alterTable(view.getCatalogName(), view.getDatabaseName(), table131, newView);
        Optional<TableEntity> newView1 = metastore.getTable(newView.getCatalogName(),
                newView.getDatabaseName(), newView.getName());
        assertTrue(newView1.isPresent());
        assertTableEquals(newView1.get(), newView);
        assertEquals(newView1.get().getCreateTime(), view.getCreateTime());
    }

    private void assertCatalogEquals(CatalogEntity actual, CatalogEntity expected)
    {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOwner(), expected.getOwner());
        assertEquals(actual.getComment(), expected.getComment());
        assertEquals(actual.getParameters(), expected.getParameters());
    }

    private void assertTableEquals(TableEntity actual, TableEntity expected)
    {
        assertEquals(actual.getCatalogName(), expected.getCatalogName());
        assertEquals(actual.getDatabaseName(), expected.getDatabaseName());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOwner(), expected.getOwner());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getViewOriginalText(), expected.getViewOriginalText());
        assertEquals(actual.getComment(), expected.getComment());
        assertEquals(actual.getParameters(), expected.getParameters());
        assertEquals(actual.getColumns(), expected.getColumns());
    }

    @Test
    public void testAlterCatalogParametersParallel()
            throws InterruptedException
    {
        String catalogName = "catalog2000";
        metastore.createCatalog(CatalogEntity.builder()
                .setCatalogName(catalogName)
                .build());

        int num = 100;
        Thread[] threads = new Thread[num];
        for (int i = 0; i < num; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    metastore.alterCatalogParameter(catalogName, String.valueOf(finalI), String.valueOf(finalI));
                }
                catch (Exception e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        Map<String, String> res = metastore.getCatalog(catalogName).get().getParameters();
        assertEquals(num, res.size());

        metastore.dropCatalog(catalogName);
    }

    @Test
    public void testAlterDatabaseParametersParallel()
            throws InterruptedException
    {
        String databaseName = "database2000";
        metastore.createDatabase(DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(databaseName)
                .build());

        int num = 100;
        Thread[] threads = new Thread[num];
        for (int i = 0; i < num; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    metastore.alterDatabaseParameter(defaultCatalog.getName(), databaseName, String.valueOf(finalI), String.valueOf(finalI));
                }
                catch (Exception e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        Map<String, String> res = metastore.getDatabase(defaultCatalog.getName(), databaseName).get().getParameters();
        assertEquals(num, res.size());

        metastore.dropDatabase(defaultCatalog.getName(), databaseName);
    }

    @Test
    public void testAlterTableParametersParallel()
            throws InterruptedException
    {
        String tableName = "table2000";
        TableEntity tableEntity = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(tableName)
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(tableEntity);

        int num = 100;
        Thread[] threads = new Thread[num];
        for (int i = 0; i < num; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    metastore.alterTableParameter(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName, String.valueOf(finalI), String.valueOf(finalI));
                }
                catch (Exception e) {
                    testResult = false;
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        Map<String, String> res = metastore.getTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName).get().getParameters();
        assertEquals(num, res.size());

        metastore.dropTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName);
    }
}
