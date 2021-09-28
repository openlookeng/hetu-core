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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Sets;
import io.hetu.core.cube.startree.tree.AggregateColumn;
import io.hetu.core.cube.startree.tree.DimensionColumn;
import io.hetu.core.cube.startree.tree.StarTreeColumn;
import io.hetu.core.cube.startree.tree.StarTreeMetadata;
import io.hetu.core.cube.startree.tree.StarTreeMetadataBuilder;
import io.hetu.core.spi.cube.CubeFilter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.hetu.core.cube.startree.tree.StarTreeMetadata.COLUMN_DELIMITER;
import static io.hetu.core.cube.startree.util.Constants.CUBE_CATALOG;
import static io.hetu.core.cube.startree.util.Constants.CUBE_DATABASE;
import static java.util.Objects.requireNonNull;

public class StarTreeMetaStore
        implements CubeMetaStore
{
    public static final String SOURCE_TABLE_NAME = "sourceTableName";
    public static final String ORIGINAL_COLUMN = "originalColumn";
    public static final String STAR_TABLE_NAME = "starTableName";
    public static final String GROUPING_STRING = "groupingString";
    public static final String SOURCE_FILTER_STRING = "sourceFilterString";
    public static final String PREDICATE_STRING = "predicateString";
    public static final String CUBE_STATUS = "cubeStatus";
    public static final String SOURCE_TABLE_LAST_UPDATED_TIME = "sourceLastUpdatedTime";
    public static final String CUBE_LAST_UPDATED_TIME = "cubeLastUpdatedTime";

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
                    table.getParameters().get(SOURCE_TABLE_NAME));
            cols.forEach(col -> {
                if (col.getType().equals("aggregate")) {
                    builder.addAggregationColumn(col.getName(), col.getParameters().get("aggregateFunction"), col.getParameters().get(ORIGINAL_COLUMN), Boolean.parseBoolean(col.getParameters().get("distinct")));
                }
                else if (col.getType().equals("dimension")) {
                    builder.addDimensionColumn(col.getName(), col.getParameters().get(ORIGINAL_COLUMN));
                }
            });
            String groupingString = table.getParameters().get(GROUPING_STRING);
            //Create empty set to support Empty Group
            builder.addGroup((groupingString == null || groupingString.isEmpty()) ? new HashSet<>() : Sets.newHashSet(groupingString.split(COLUMN_DELIMITER)));
            String sourceTablePredicate = table.getParameters().get(SOURCE_FILTER_STRING);
            String cubePredicate = table.getParameters().get(PREDICATE_STRING);
            if (sourceTablePredicate != null || cubePredicate != null) {
                builder.withCubeFilter(new CubeFilter(sourceTablePredicate, cubePredicate));
            }
            builder.setCubeStatus(CubeStatus.forValue(Integer.parseInt(table.getParameters().get(CUBE_STATUS))));
            builder.setTableLastUpdatedTime(Long.parseLong(table.getParameters().get(SOURCE_TABLE_LAST_UPDATED_TIME)));
            builder.setCubeLastUpdatedTime(Long.parseLong(table.getParameters().get(CUBE_LAST_UPDATED_TIME)));
            cubeMetadataList.add(builder.build());
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
            if (table.getParameters().get(SOURCE_TABLE_NAME).equals(tableName)) {
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
        metastore.dropTable(CUBE_CATALOG, CUBE_DATABASE, cubeMetadata.getCubeName().replace(".", "_"));
        cubeCache.invalidate(cubeMetadata.getSourceTableName());
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

        String cubeNameDelimited = cubeMetadata.getCubeName().replace(".", "_");
        TableEntity table = getTableEntity((StarTreeMetadata) cubeMetadata);
        if (metastore.getTable(CUBE_CATALOG, CUBE_DATABASE, cubeNameDelimited).isPresent()) {
            //update flow
            metastore.alterTable(CUBE_CATALOG, CUBE_DATABASE, cubeNameDelimited, table);
        }
        else {
            //create flow
            metastore.createTable(table);
        }
        cubeCache.invalidate(cubeMetadata.getSourceTableName());
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
        String cubeNameDelimited = starTreeMetadata.getCubeName().replace(".", "_");
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
        parameters.put(SOURCE_TABLE_NAME, starTreeMetadata.getSourceTableName());
        parameters.put(STAR_TABLE_NAME, starTreeMetadata.getCubeName());
        parameters.put(GROUPING_STRING, String.join(COLUMN_DELIMITER, starTreeMetadata.getGroup()));
        parameters.put(SOURCE_FILTER_STRING, starTreeMetadata.getCubeFilter() == null ? null : starTreeMetadata.getCubeFilter().getSourceTablePredicate());
        parameters.put(PREDICATE_STRING, starTreeMetadata.getCubeFilter() == null ? null : starTreeMetadata.getCubeFilter().getCubePredicate());
        parameters.put(CUBE_STATUS, String.valueOf(starTreeMetadata.getCubeStatus().getValue()));
        parameters.put(CUBE_LAST_UPDATED_TIME, String.valueOf(starTreeMetadata.getLastUpdatedTime()));
        parameters.put(SOURCE_TABLE_LAST_UPDATED_TIME, String.valueOf(starTreeMetadata.getSourceTableLastUpdatedTime()));

        return TableEntity.builder()
                .setCatalogName(CUBE_CATALOG)
                .setDatabaseName(CUBE_DATABASE)
                .setTableType(TableEntityType.TABLE.toString())
                .setTableName(cubeNameDelimited)
                .setColumns(columns)
                .setParameters(parameters)
                .build();
    }

    @Override
    public List<CubeMetadata> getAllCubes()
    {
        return convertTablesToMetadata(metastore.getAllTables(CUBE_CATALOG, CUBE_DATABASE));
    }

    @Override
    public CubeMetadataBuilder getBuilder(String cubeName, String sourceTableName)
    {
        return new StarTreeMetadataBuilder(cubeName, sourceTableName);
    }

    @Override
    public CubeMetadataBuilder getBuilder(CubeMetadata existingMetadata)
    {
        return new StarTreeMetadataBuilder((StarTreeMetadata) existingMetadata);
    }
}
