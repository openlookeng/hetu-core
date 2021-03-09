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

package io.hetu.core.cube.startree.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.hetu.core.cube.startree.tree.AggregateColumn;
import io.hetu.core.cube.startree.tree.DimensionColumn;
import io.hetu.core.cube.startree.tree.StarTreeColumn;
import io.hetu.core.cube.startree.tree.StarTreeMetadata;
import io.hetu.core.cube.startree.tree.StarTreeMetadataBuilder;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeMetadataBuilder;
import io.hetu.core.spi.cube.CubeStatus;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.ColumnEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.metastore.model.TableEntityType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.hetu.core.cube.startree.tree.StarTreeMetadata.COLUMN_DELIMITER;
import static io.hetu.core.cube.startree.tree.StarTreeMetadata.GROUP_DELIMITER;
import static io.hetu.core.cube.startree.util.Constants.CUBE_CATALOG;
import static io.hetu.core.cube.startree.util.Constants.CUBE_DATABASE;
import static java.util.Objects.requireNonNull;

public class StarTreeMetaStore
        implements CubeMetaStore
{
    public static final String ORIGINAL_TABLE_NAME = "originalTableName";
    public static final String ORIGINAL_COLUMN = "originalColumn";
    public static final String STAR_TABLE_NAME = "starTableName";
    public static final String GROUPING_STRING = "groupingString";
    public static final String PREDICATE_STRING = "predicateString";
    public static final String CUBE_STATUS = "cubeStatus";
    private final HetuMetastore metastore;
    private final LoadingCache<String, List<CubeMetadata>> cubeCache;

    public StarTreeMetaStore(HetuMetastore metastore, long cacheTtl, long cacheSize)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.cubeCache = Caffeine.newBuilder()
                .expireAfterAccess(cacheTtl, TimeUnit.MILLISECONDS)
                .maximumSize(cacheSize)
                .build(this::loadMetadata);
    }

    private List<CubeMetadata> convertTablesToMetadata(List<TableEntity> tableEntities)
    {
        List<CubeMetadata> cubeMetadataList = new ArrayList<>();
        tableEntities.forEach(table -> {
            List<ColumnEntity> cols = table.getColumns();
            StarTreeMetadataBuilder builder = new StarTreeMetadataBuilder(table.getParameters().get(STAR_TABLE_NAME),
                    table.getParameters().get(ORIGINAL_TABLE_NAME));
            cols.forEach(col -> {
                if (col.getType().equals("aggregate")) {
                    builder.addAggregationColumn(col.getName(), col.getParameters().get("aggregateFunction"), col.getParameters().get(ORIGINAL_COLUMN), Boolean.parseBoolean(col.getParameters().get("distinct")));
                }
                else if (col.getType().equals("dimension")) {
                    builder.addDimensionColumn(col.getName(), col.getParameters().get(ORIGINAL_COLUMN));
                }
            });
            String groupingString = table.getParameters().get(GROUPING_STRING);
            if (groupingString != null) {
                for (String columns : groupingString.split(GROUP_DELIMITER)) {
                    Set<String> group;
                    if (columns.equals("")) {
                        group = new HashSet<>();
                    }
                    else {
                        group = new HashSet<>(Arrays.asList(columns.split(COLUMN_DELIMITER)));
                    }
                    builder.addGroup(group);
                }
            }
            builder.withPredicate(table.getParameters().get(PREDICATE_STRING));
            builder.setCubeStatus(CubeStatus.forValue(Integer.parseInt(table.getParameters().get(CUBE_STATUS))));
            cubeMetadataList.add(builder.build(table.getCreateTime()));
        });
        return cubeMetadataList;
    }

    @Override
    public Optional<CubeMetadata> getMetadataFromCubeName(String cubeName)
    {
        String cubeUnderscoreName = cubeName.replace(".", "_");
        Optional<TableEntity> tableEntity = metastore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeUnderscoreName);
        return tableEntity.map(entity -> convertTablesToMetadata(Collections.singletonList(entity)).get(0));
    }

    private List<CubeMetadata> loadMetadata(String tableName)
    {
        List<TableEntity> tables = metastore.getAllTables(CUBE_CATALOG, CUBE_DATABASE);
        List<TableEntity> matchingTables = new ArrayList<>();
        tables.forEach(table -> {
            if (table.getParameters().get(ORIGINAL_TABLE_NAME).equals(tableName)) {
                matchingTables.add(table);
            }
        });

        return convertTablesToMetadata(matchingTables);
    }

    @Override
    public List<CubeMetadata> getMetadataList(String tableName)
    {
        return cubeCache.get(tableName);
    }

    @Override
    public void removeCube(CubeMetadata cubeMetadata)
    {
        metastore.dropTable(CUBE_CATALOG, CUBE_DATABASE, cubeMetadata.getCubeTableName().replace(".", "_"));
        cubeCache.invalidate(cubeMetadata.getOriginalTableName());
    }

    @Override
    public void persist(CubeMetadata cubeMetadata)
    {
        CatalogEntity catalogEntity = catalogEntity();
        if (!metastore.getCatalog(catalogEntity.getName()).isPresent()) {
            metastore.createCatalog(catalogEntity);
        }

        DatabaseEntity databaseEntity = databaseEntity();
        if (!metastore.getDatabase(catalogEntity.getName(), databaseEntity.getName()).isPresent()) {
            metastore.createDatabase(databaseEntity);
        }

        String cubeNameDelimited = cubeMetadata.getCubeTableName().replace(".", "_");
        TableEntity table = getTableEntity((StarTreeMetadata) cubeMetadata);
        if (metastore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeNameDelimited).isPresent()) {
            //update flow
            metastore.alterTable(CUBE_CATALOG, CUBE_DATABASE, cubeNameDelimited, table);
        }
        else {
            //create flow
            metastore.createTable(table);
        }
        cubeCache.invalidate(cubeMetadata.getOriginalTableName());
    }

    private CatalogEntity catalogEntity()
    {
        return CatalogEntity.builder()
                .setCatalogName(CUBE_CATALOG)
                .setOwner("root")
                .setComment(Optional.of("Hetu Star-tree Cubes"))
                .setCreateTime(System.currentTimeMillis())
                .build();
    }

    private DatabaseEntity databaseEntity()
    {
        return DatabaseEntity.builder()
                .setCatalogName(CUBE_CATALOG)
                .setDatabaseName(CUBE_DATABASE)
                .setOwner("root")
                .setComment(Optional.of("Hetu Star-tree database"))
                .setCreateTime(System.currentTimeMillis())
                .build();
    }

    private TableEntity getTableEntity(StarTreeMetadata starTreeMetadata)
    {
        String cubeNameDelimited = starTreeMetadata.getCubeTableName().replace(".", "_");
        List<ColumnEntity> columns = new ArrayList<>();
        starTreeMetadata.getColumns().forEach(col -> {
            ColumnEntity newCol = new ColumnEntity();
            if (col.getType() == StarTreeColumn.ColumnType.AGGREGATE) {
                AggregateColumn aggCol = (AggregateColumn) col;
                Map<String, String> params = new HashMap<>();
                params.put("aggregateFunction", aggCol.getAggregateFunction());
                params.put(ORIGINAL_COLUMN, aggCol.getOriginalColumn());
                params.put("distinct", Boolean.toString(aggCol.isDistinct()));
                newCol.setName(aggCol.getName());
                newCol.setType("aggregate");
                newCol.setParameters(params);
            }
            else if (col.getType() == StarTreeColumn.ColumnType.DIMENSION) {
                DimensionColumn dimCol = (DimensionColumn) col;
                Map<String, String> params = new HashMap<>();
                params.put(ORIGINAL_COLUMN, dimCol.getOriginalColumn());
                newCol.setName(dimCol.getName());
                newCol.setType("dimension");
                newCol.setParameters(params);
            }
            columns.add(newCol);
        });
        Map<String, String> parameters = new HashMap<>();
        parameters.put(ORIGINAL_TABLE_NAME, starTreeMetadata.getOriginalTableName());
        parameters.put(STAR_TABLE_NAME, starTreeMetadata.getCubeTableName());
        parameters.put(GROUPING_STRING, starTreeMetadata.getGroupString());
        parameters.put(PREDICATE_STRING, starTreeMetadata.getPredicateString());
        parameters.put(CUBE_STATUS, String.valueOf(starTreeMetadata.getCubeStatus().getValue()));
        return TableEntity.builder()
                .setCatalogName(CUBE_CATALOG)
                .setDatabaseName(CUBE_DATABASE)
                .setTableType(TableEntityType.TABLE.toString())
                .setTableName(cubeNameDelimited)
                .setColumns(columns)
                .setParameters(parameters)
                .setCreateTime(starTreeMetadata.getLastUpdated())
                .build();
    }

    @Override
    public List<CubeMetadata> getAllCubes()
    {
        return convertTablesToMetadata(metastore.getAllTables(CUBE_CATALOG, CUBE_DATABASE));
    }

    @Override
    public CubeMetadataBuilder getBuilder(String cubeName, String originalTableName)
    {
        return new StarTreeMetadataBuilder(cubeName, originalTableName);
    }

    @Override
    public CubeMetadataBuilder getBuilder(CubeMetadata existingMetadata)
    {
        return new StarTreeMetadataBuilder((StarTreeMetadata) existingMetadata);
    }
}
