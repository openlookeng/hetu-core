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
package io.hetu.core.metastore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.metastore.hetufilesystem.HetuFsMetastoreModule;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.metastore.HetuCache;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.metastore.model.TableEntityType;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.hetu.core.metastore.MetaStoreConstants.LOCAL;
import static io.prestosql.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHetuMetastoreCacheLocal
{
    private HetuMetastore metastore;
    private HetuFileSystemClient client;
    private CatalogEntity defaultCatalog;
    private DatabaseEntity defaultDatabase;
    private String path = Resources.getResource("").getPath() + File.separator + "metastoreCache";

    private static final String TESTING_HOST = "127.0.0.1";
    private static final String TESTING_PORT = "8090";

    private String expected;
    private String actual;
    private ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

    private HetuCache<String, Optional<CatalogEntity>> catalogCache;
    private HetuCache<String, List<CatalogEntity>> catalogsCache;
    private HetuCache<String, Optional<DatabaseEntity>> databaseCache;
    private HetuCache<String, List<DatabaseEntity>> databasesCache;
    private HetuCache<String, Optional<TableEntity>> tableCache;
    private HetuCache<String, List<TableEntity>> tablesCache;

    /**
     * setUp
     *
     * @throws Exception Exception
     */
    @BeforeClass
    public void setUp()
            throws Throwable
    {
        try {
            Map<String, String> config = new ImmutableMap.Builder<String, String>()
                    .put("hetu.metastore.hetufilesystem.path", path)
                    .put("hetu.metastore.hetufilesystem.profile-name", "local-config-catalog")
                    .put("hetu.metastore.cache.size", "10000")
                    .put("hetu.metastore.cache.ttl", "10h")
                    .build();

            LocalConfig localConfig = new LocalConfig(null);
            client = new HetuLocalFileSystemClient(localConfig, Paths.get(path));

            if (!client.exists(Paths.get(path))) {
                client.createDirectories(Paths.get(path));
            }
            client.deleteRecursively(Paths.get(path));

            StateStore stateStore = createMockStateStore();

            String type = LOCAL;

            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new HetuFsMetastoreModule(client, stateStore, type));

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

        // create default catalog and database
        defaultCatalog = CatalogEntity.builder()
                .setCatalogName("hetu1")
                .setOwner("hetu1").build();
        metastore.createCatalog(defaultCatalog);

        defaultDatabase = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db1").build();
        metastore.createDatabase(defaultDatabase);

        // get metastore catalog cache
        Field catalogCacheField = metastore.getClass().getSuperclass().getDeclaredField("catalogCache");
        catalogCacheField.setAccessible(true);
        catalogCache = (HetuCache<String, Optional<CatalogEntity>>) catalogCacheField.get(metastore);

        Field catalogsCacheField = metastore.getClass().getSuperclass().getDeclaredField("catalogsCache");
        catalogsCacheField.setAccessible(true);
        catalogsCache = (HetuCache<String, List<CatalogEntity>>) catalogsCacheField.get(metastore);

        // get metastore database cache
        Field databaseCacheField = metastore.getClass().getSuperclass().getDeclaredField("databaseCache");
        databaseCacheField.setAccessible(true);
        databaseCache = (HetuCache<String, Optional<DatabaseEntity>>) databaseCacheField.get(metastore);

        Field databasesCacheField = metastore.getClass().getSuperclass().getDeclaredField("databasesCache");
        databasesCacheField.setAccessible(true);
        databasesCache = (HetuCache<String, List<DatabaseEntity>>) databasesCacheField.get(metastore);

        // get metastore table cache
        Field tableCacheField = metastore.getClass().getSuperclass().getDeclaredField("tableCache");
        tableCacheField.setAccessible(true);
        tableCache = (HetuCache<String, Optional<TableEntity>>) tableCacheField.get(metastore);

        Field tablesCacheField = metastore.getClass().getSuperclass().getDeclaredField("tablesCache");
        tablesCacheField.setAccessible(true);
        tablesCache = (HetuCache<String, List<TableEntity>>) tablesCacheField.get(metastore);
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
            e.printStackTrace();
        }
    }

    /**
     * test create catalog with metastore cache
     */
    @Test
    public void testCreateCatalog()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();
        CatalogEntity catalogEntity = CatalogEntity.builder()
                .setCatalogName("catalog1")
                .setOwner("root1")
                .setComment(Optional.of("Hetu create catalog"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalogEntity);

        assertEquals(catalogsCache.getIfPresent(""), null);

        metastore.dropCatalog(catalogEntity.getName());
    }

    /**
     * test drop dropCatalog with metastore cache
     */
    @Test
    public void testDropCatalog()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();
        CatalogEntity catalogEntity = CatalogEntity.builder()
                .setCatalogName("catalog2")
                .setOwner("root2")
                .setComment(Optional.of("Hetu create catalog"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalogEntity);
        metastore.dropCatalog(catalogEntity.getName());

        assertEquals(catalogCache.getIfPresent("catalog2"), null);
        assertEquals(catalogsCache.getIfPresent(""), null);
    }

    /**
     * test get catalog with metastore cache
     */
    @Test
    public void testGetCatalog()
            throws JsonProcessingException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();
        CatalogEntity catalogEntity = CatalogEntity.builder()
                .setCatalogName("catalog3")
                .setOwner("root3")
                .setComment(Optional.of("Hetu create catalog"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalogEntity);

        Optional<CatalogEntity> catalogInfo = metastore.getCatalog("catalog3");
        assertTrue(catalogInfo.isPresent());
        actual = mapper.writeValueAsString(catalogInfo.get());
        expected = mapper.writeValueAsString(catalogEntity);

        assertEquals(actual, expected);

        assertEquals(catalogInfo.get(), catalogEntity);

        metastore.dropCatalog(catalogEntity.getName());
    }

    /**
     * test get All catalogs with metastore cache
     */
    @Test
    public void testGetAllCatalogs()
            throws JsonProcessingException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();
        CatalogEntity catalog1 = CatalogEntity.builder()
                .setCatalogName("catalog4")
                .setOwner("root4")
                .setComment(Optional.of("Hetu create catalog"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog1);

        CatalogEntity catalog2 = CatalogEntity.builder()
                .setCatalogName("catalog5")
                .setOwner("root5")
                .setComment(Optional.of("Hetu create catalog"))
                .setParameters(emptyMap())
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog2);

        List<CatalogEntity> catalogEntities = metastore.getCatalogs();

        actual = mapper.writeValueAsString(catalogsCache.getIfPresent(""));
        expected = mapper.writeValueAsString(catalogEntities);

        assertEquals(actual, expected);

        metastore.dropCatalog(catalog1.getName());
        metastore.dropCatalog(catalog2.getName());
    }

    /**
     * test alter catalog with metastore cache
     */
    @Test
    public void testAlterCatalog()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder().build();
        CatalogEntity catalog1 = CatalogEntity.builder()
                .setCatalogName("catalog6")
                .setOwner("root6")
                .setComment(Optional.of("Hetu create catalog"))
                .setParameters(properties)
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.createCatalog(catalog1);

        CatalogEntity catalog2 = CatalogEntity.builder()
                .setCatalogName("catalog6")
                .setOwner("hive")
                .setComment(Optional.of("Hetu alter catalog"))
                .setParameters(emptyMap())
                .setCreateTime(System.currentTimeMillis())
                .build();
        metastore.alterCatalog("catalog6", catalog2);

        assertEquals(catalogsCache.getIfPresent(""), null);

        metastore.dropCatalog(catalog2.getName());
    }

    /**
     * test create database with metastore cache
     */
    @Test
    public void testCreateDatabase()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();
        DatabaseEntity databaseEntity = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db2")
                .setOwner("root7")
                .setComment(Optional.of("Hetu create database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(databaseEntity);

        assertEquals(databasesCache.getIfPresent(defaultCatalog.getName()), null);

        metastore.dropDatabase(defaultCatalog.getName(), "db2");
    }

    /**
     * test drop database with metastore cache
     */
    @Test
    public void testDropDatabase()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();
        DatabaseEntity databaseEntity = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db3")
                .setOwner("root8")
                .setComment(Optional.of("Hetu create database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(databaseEntity);
        metastore.dropDatabase(defaultCatalog.getName(), "db3");

        String key = defaultCatalog.getName() + "." + "db3";
        assertEquals(databaseCache.getIfPresent(key), null);
        assertEquals(databasesCache.getIfPresent(defaultCatalog.getName()), null);
    }

    /**
     * test get database with metastore cache
     */
    @Test
    public void testGetDatabase()
            throws JsonProcessingException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();
        DatabaseEntity databaseEntity = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db4")
                .setOwner("root9")
                .setComment(Optional.of("Hetu create database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(databaseEntity);

        Optional<DatabaseEntity> databaseEntity2 = metastore.getDatabase(defaultCatalog.getName(), "db4");

        assertTrue(databaseEntity2.isPresent());

        actual = mapper.writeValueAsString(databaseEntity2.get());
        expected = mapper.writeValueAsString(databaseEntity);

        assertEquals(actual, expected);

        metastore.dropDatabase(defaultCatalog.getName(), "db4");
    }

    /**
     * test get all databases with metastore cache
     */
    @Test
    public void testAllDatabases()
            throws JsonProcessingException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();
        DatabaseEntity databaseEntity1 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db5")
                .setOwner("root10")
                .setComment(Optional.of("Hetu create database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(databaseEntity1);

        DatabaseEntity databaseEntity2 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName("db6")
                .setOwner("root11")
                .setComment(Optional.of("Hetu create database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(databaseEntity2);

        List<DatabaseEntity> databaseEntities = metastore.getAllDatabases(defaultCatalog.getName());

        actual = mapper.writeValueAsString(databasesCache.getIfPresent(defaultCatalog.getName()));
        expected = mapper.writeValueAsString(databaseEntities);

        assertEquals(actual, expected);

        metastore.dropDatabase(defaultCatalog.getName(), "db5");
        metastore.dropDatabase(defaultCatalog.getName(), "db6");
    }

    /**
     * test alter database with metastore cache
     */
    @Test
    public void testAlterDatabase()
    {
        String oldDatabaseName = "db7";
        String newDatabaseName = "db8";
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("desc", "vschema")
                .build();
        DatabaseEntity databaseEntity1 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(oldDatabaseName)
                .setOwner("root12")
                .setComment(Optional.of("Hetu create database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();
        metastore.createDatabase(databaseEntity1);

        DatabaseEntity databaseEntity2 = DatabaseEntity.builder()
                .setCatalogName(defaultCatalog.getName())
                .setDatabaseName(newDatabaseName)
                .setOwner("root13")
                .setComment(Optional.of("Hetu create database"))
                .setCreateTime(System.currentTimeMillis())
                .setParameters(properties)
                .build();

        metastore.alterDatabase(defaultCatalog.getName(), oldDatabaseName, databaseEntity2);

        String key = defaultCatalog.getName() + "." + oldDatabaseName;
        assertEquals(databaseCache.getIfPresent(key), null);
        assertEquals(databasesCache.getIfPresent(defaultCatalog.getName()), null);

        metastore.dropDatabase(defaultCatalog.getName(), newDatabaseName);
    }

    /**
     * testCreateView with metastore cache
     */
    @Test
    public void testCreateTable()
    {
        String tableName = "table1";
        SchemaTableName schemaTableName = new SchemaTableName(defaultDatabase.getName(), tableName);
        TableEntity tableEntity = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(schemaTableName.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(tableEntity);

        String tablesKey = defaultDatabase.getCatalogName() + "." + defaultDatabase.getName();
        assertEquals(tablesCache.getIfPresent(tablesKey), null);

        metastore.dropTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName);
    }

    /**
     * test drop table with metastore cache
     */
    @Test
    public void testDropTable()
    {
        String tableName = "table2";
        SchemaTableName schemaTableName = new SchemaTableName(defaultDatabase.getName(), tableName);
        TableEntity tableEntity = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(schemaTableName.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(tableEntity);
        metastore.dropTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName);

        String tablesKey = defaultDatabase.getCatalogName() + "." + defaultDatabase.getName();
        String tableKey = defaultDatabase.getCatalogName() + "." + defaultDatabase.getName() + "." + tableName;
        assertEquals(tableCache.getIfPresent(tableKey), null);
        assertEquals(databasesCache.getIfPresent(tablesKey), null);
    }

    /**
     * test get table with metastore cache
     */
    @Test
    public void testGetTable()
            throws JsonProcessingException
    {
        String tableName = "table3";
        SchemaTableName schemaTableName = new SchemaTableName(defaultDatabase.getName(), tableName);
        TableEntity tableEntity = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(schemaTableName.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(tableEntity);

        Optional<TableEntity> tableEntity2 = metastore.getTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName);

        String tableKey = defaultDatabase.getCatalogName() + "." + defaultDatabase.getName() + "." + tableName;

        actual = mapper.writeValueAsString(tableCache.getIfPresent(tableKey).get());
        expected = mapper.writeValueAsString(tableEntity2.get());

        assertEquals(actual, expected);

        metastore.dropTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName);
    }

    /**
     * test get all table with metastore cache
     */
    @Test
    public void testGetAllTables()
            throws JsonProcessingException
    {
        String tableName = "table4";
        SchemaTableName schemaTableName = new SchemaTableName(defaultDatabase.getName(), tableName);
        TableEntity tableEntity = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(schemaTableName.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(tableEntity);

        List<TableEntity> tableEntities = metastore.getAllTables(defaultDatabase.getCatalogName(), defaultDatabase.getName());

        String tablesKey = defaultDatabase.getCatalogName() + "." + defaultDatabase.getName();

        actual = mapper.writeValueAsString(tablesCache.getIfPresent(tablesKey));
        expected = mapper.writeValueAsString(tableEntities);

        assertEquals(actual, expected);

        metastore.dropTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName);
    }

    /**
     * test alter table with metastore cache
     */
    @Test
    public void testAlterTable()
    {
        String tableName1 = "table5";
        SchemaTableName schemaTableName1 = new SchemaTableName(defaultDatabase.getName(), tableName1);
        TableEntity tableEntity1 = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(schemaTableName1.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();
        metastore.createTable(tableEntity1);

        String tableName2 = "table6";
        SchemaTableName schemaTableName2 = new SchemaTableName(defaultDatabase.getName(), tableName2);
        TableEntity tableEntity2 = TableEntity.builder()
                .setCatalogName(defaultDatabase.getCatalogName())
                .setDatabaseName(defaultDatabase.getName())
                .setTableName(schemaTableName2.getTableName())
                .setTableType(TableEntityType.TABLE.toString())
                .build();

        metastore.alterTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName1, tableEntity2);

        String tablesKey = defaultDatabase.getCatalogName() + "." + defaultDatabase.getName();
        String tableKey = defaultDatabase.getCatalogName() + "." + defaultDatabase.getName() + "." + tableName1;
        assertEquals(tablesCache.getIfPresent(tablesKey), null);
        assertEquals(tableCache.getIfPresent(tableKey), null);

        metastore.dropTable(defaultDatabase.getCatalogName(), defaultDatabase.getName(), tableName2);
    }

    private StateStore createMockStateStore()
    {
        StateStoreProvider mockStateStoreProvider = mock(LocalStateStoreProvider.class);
        StateStore mockStateStore = mock(StateStore.class);
        when(mockStateStoreProvider.getStateStore()).thenReturn(mockStateStore);
        StateMap<String, String> mockDiscoveryInfo = mock(StateMap.class);
        Map<String, String> mockDiscoveryMap = new HashMap<>();
        mockDiscoveryMap.put(TESTING_HOST, TESTING_PORT);
        when(mockDiscoveryInfo.getAll()).thenReturn(mockDiscoveryMap);
        when(mockStateStore.getStateCollection(StateStoreConstants.DISCOVERY_SERVICE_COLLECTION_NAME)).thenReturn(mockDiscoveryInfo);

        return mockStateStoreProvider.getStateStore();
    }
}
