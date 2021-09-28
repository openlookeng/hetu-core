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

package io.hetu.core.cube.startree.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.hetu.core.cube.startree.StarTreeProvider;
import io.hetu.core.cube.startree.tree.AggregateColumn;
import io.hetu.core.cube.startree.tree.DimensionColumn;
import io.hetu.core.cube.startree.tree.StarTreeMetadata;
import io.hetu.core.cube.startree.tree.StarTreeMetadataBuilder;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatus;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.sql.analyzer.FeaturesConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static io.hetu.core.cube.startree.util.Constants.CUBE_CATALOG;
import static io.hetu.core.cube.startree.util.Constants.CUBE_DATABASE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestStarTreeMetaStore
{
    private CubeMetaStore cubeMetadataService;
    private MockMetaStore metaStore;
    private CubeMetadata cubeMetadata1;
    private CubeMetadata cubeMetadata2;

    @BeforeClass
    public void setup()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setCubeMetadataCacheTtl(Duration.valueOf("5m"));
        featuresConfig.setCubeMetadataCacheSize(1000);
        Properties properties = new Properties();
        properties.setProperty("cache-ttl", Long.toString(featuresConfig.getCubeMetadataCacheTtl().toMillis()));
        properties.setProperty("cache-size", Long.toString(featuresConfig.getCubeMetadataCacheSize()));
        metaStore = new MockMetaStore();
        cubeMetadataService = new StarTreeProvider().getCubeMetaStore(metaStore, properties);
        cubeMetadata1 = new StarTreeMetadata("star1",
                "a",
                1000,
                ImmutableList.of(
                        new AggregateColumn("sum_cost", "SUM", "cost", false),
                        new DimensionColumn("value", "value")),
                ImmutableList.of(ImmutableSet.of("value")),
                null,
                10000,
                CubeStatus.READY);
        cubeMetadata2 = new StarTreeMetadata("star2",
                "a",
                1000,
                ImmutableList.of(
                        new AggregateColumn("sum_cost", "SUM", "cost", false),
                        new DimensionColumn("value", "value")),
                ImmutableList.of(ImmutableSet.of("value")),
                null,
                10000,
                CubeStatus.READY);
    }

    @Test
    public void testPersistWithOneTableOnEmptyMetadata()
    {
        cubeMetadataService.persist(cubeMetadata1);

        assertFalse(metaStore.getCatalogs().isEmpty());
        assertFalse(metaStore.getAllDatabases(CUBE_CATALOG).isEmpty());
        assertTrue(metaStore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeMetadata1.getCubeName()).isPresent());
    }

    @Test
    public void testPersistWithNonEmptyMetadata()
    {
        cubeMetadataService.persist(cubeMetadata1);
        cubeMetadataService.persist(cubeMetadata2);
        assertFalse(metaStore.getCatalogs().isEmpty());
        assertFalse(metaStore.getAllDatabases(CUBE_CATALOG).isEmpty());
        assertTrue(metaStore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeMetadata1.getCubeName()).isPresent());
        assertTrue(metaStore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeMetadata2.getCubeName()).isPresent());
    }

    @Test
    public void testRemoveCube()
    {
        cubeMetadataService.persist(cubeMetadata1);
        cubeMetadataService.persist(cubeMetadata2);

        cubeMetadataService.removeCube(cubeMetadata2);

        assertFalse(metaStore.getCatalogs().isEmpty());
        assertFalse(metaStore.getAllDatabases(CUBE_CATALOG).isEmpty());
        assertTrue(metaStore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeMetadata1.getCubeName()).isPresent());
        assertFalse(metaStore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeMetadata2.getCubeName()).isPresent());
    }

    @Test
    public void testGetMetaDataListEmpty()
    {
        assertFalse(cubeMetadataService.getMetadataList("a").isEmpty());
    }

    @Test
    public void testGetMetaDataListWithTables()
    {
        cubeMetadataService.persist(cubeMetadata1);
        cubeMetadataService.persist(cubeMetadata2);

        assertFalse(cubeMetadataService.getMetadataList("a").isEmpty());
        cubeMetadataService.getMetadataList("a").forEach(cube -> {
            assertEquals(cube.getSourceTableName(), "a");
            assertEquals(cube.getDimensions(), Collections.singleton("value"));
        });
        assertEquals(cubeMetadataService.getMetadataList("a").size(), 2);
    }

    @Test
    public void testGetMetaDataByCubeName()
    {
        cubeMetadataService.persist(cubeMetadata1);
        cubeMetadataService.persist(cubeMetadata2);

        CubeMetadata found = cubeMetadataService.getMetadataFromCubeName("star1").get();
        assertEquals(found, cubeMetadata1);
        assertNotEquals(found, cubeMetadata2);
    }

    @Test
    public void testGetAllCubes()
    {
        cubeMetadataService.persist(cubeMetadata1);
        cubeMetadataService.persist(cubeMetadata2);
        List<CubeMetadata> result = cubeMetadataService.getAllCubes();
        assertEquals(result.size(), 2);
        assertTrue(result.containsAll(ImmutableList.of(cubeMetadata1, cubeMetadata2)));
    }

    @Test
    public void testUpdateMetadata()
    {
        cubeMetadataService.persist(cubeMetadata1);
        StarTreeMetadata starTreeMetadata = (StarTreeMetadata) cubeMetadata1;
        StarTreeMetadataBuilder builder = new StarTreeMetadataBuilder(starTreeMetadata);
        builder.setCubeLastUpdatedTime(System.currentTimeMillis());
        CubeMetadata updated = builder.build();
        assertNotEquals(cubeMetadata1, updated);
    }

    private static class MockMetaStore
            implements HetuMetastore
    {
        private final List<CatalogEntity> catalogs = new ArrayList<>();
        private final List<DatabaseEntity> databases = new ArrayList<>();
        private final List<TableEntity> tables = new ArrayList<>();

        @Override
        public void createCatalog(CatalogEntity catalog)
        {
            catalogs.add(catalog);
        }

        @Override
        public void createCatalogIfNotExist(CatalogEntity catalog)
        {
            if (!catalogs.contains(catalog)) {
                createCatalog(catalog);
            }
        }

        @Override
        public void alterCatalog(String catalogName, CatalogEntity newCatalog)
        {
        }

        @Override
        public void dropCatalog(String catalogName)
        {
        }

        @Override
        public Optional<CatalogEntity> getCatalog(String catalogName)
        {
            if (catalogs.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(catalogs.get(0));
        }

        @Override
        public List<CatalogEntity> getCatalogs()
        {
            return catalogs;
        }

        @Override
        public void createDatabase(DatabaseEntity database)
        {
            databases.add(database);
        }

        @Override
        public void createDatabaseIfNotExist(DatabaseEntity database)
        {
            if (!databases.contains(database)) {
                createDatabase(database);
            }
        }

        @Override
        public void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
        {
        }

        @Override
        public void dropDatabase(String catalogName, String databaseName)
        {
        }

        @Override
        public Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
        {
            if (databases.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(databases.get(0));
        }

        @Override
        public List<DatabaseEntity> getAllDatabases(String catalogName)
        {
            return databases;
        }

        @Override
        public void createTable(TableEntity table)
        {
            tables.add(table);
        }

        @Override
        public void createTableIfNotExist(TableEntity table)
        {
            if (!tables.contains(table)) {
                createTable(table);
            }
        }

        @Override
        public void dropTable(String catalogName, String databaseName, String tableName)
        {
            tables.removeIf(table -> table.getName().equals(tableName));
        }

        @Override
        public void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
        {
        }

        @Override
        public Optional<TableEntity> getTable(String catalogName, String databaseName, String table)
        {
            return tables.stream().filter(t -> t.getName().equals(table)).findFirst();
        }

        @Override
        public List<TableEntity> getAllTables(String catalogName, String databaseName)
        {
            return tables;
        }

        @Override
        public void alterCatalogParameter(String catalogName, String key, String value) {}

        @Override
        public void alterDatabaseParameter(String catalogName, String databaseName, String key, String value) {}

        @Override
        public void alterTableParameter(String catalogName, String databaseName, String tableName, String key, String value) {}
    }
}
