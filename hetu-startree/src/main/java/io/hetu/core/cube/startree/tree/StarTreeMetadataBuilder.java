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

package io.hetu.core.cube.startree.tree;

import com.google.common.collect.ImmutableSet;
import io.hetu.core.spi.cube.CubeFilter;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeMetadataBuilder;
import io.hetu.core.spi.cube.CubeStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StarTreeMetadataBuilder
        implements CubeMetadataBuilder
{
    private final String starTableName;
    private final String sourceTableName;
    private final List<StarTreeColumn> columns = new ArrayList<>();
    private final List<Set<String>> groups = new ArrayList<>();
    private CubeFilter cubeFilter;
    private CubeStatus cubeStatus;
    private long tableLastUpdatedTime;
    private long cubeLastUpdatedTime;

    public StarTreeMetadataBuilder(String starTableName, String sourceTableName)
    {
        this.starTableName = starTableName;
        this.sourceTableName = sourceTableName;
    }

    public StarTreeMetadataBuilder(StarTreeMetadata starTreeMetadata)
    {
        this.starTableName = starTreeMetadata.getCubeName();
        this.sourceTableName = starTreeMetadata.getSourceTableName();
        this.columns.addAll(starTreeMetadata.getColumns());
        this.groups.add(starTreeMetadata.getGroup());
        this.cubeFilter = starTreeMetadata.getCubeFilter();
        this.tableLastUpdatedTime = starTreeMetadata.getSourceTableLastUpdatedTime();
        this.cubeLastUpdatedTime = starTreeMetadata.getLastUpdatedTime();
        this.cubeStatus = starTreeMetadata.getCubeStatus();
    }

    @Override
    public void setCubeStatus(CubeStatus cubeStatus)
    {
        this.cubeStatus = cubeStatus;
    }

    @Override
    public void setTableLastUpdatedTime(long tableLastUpdatedTime)
    {
        this.tableLastUpdatedTime = tableLastUpdatedTime;
    }

    @Override
    public void setCubeLastUpdatedTime(long cubeLastUpdatedTime)
    {
        this.cubeLastUpdatedTime = cubeLastUpdatedTime;
    }

    @Override
    public void addDimensionColumn(String name, String originalColumn)
    {
        columns.add(new DimensionColumn(name, originalColumn));
    }

    @Override
    public void addAggregationColumn(String name, String aggregationFunction, String originalColumn, boolean distinct)
    {
        columns.add(new AggregateColumn(name, aggregationFunction, originalColumn, distinct));
    }

    @Override
    public void addGroup(Set<String> group)
    {
        this.groups.add(ImmutableSet.copyOf(group));
    }

    @Override
    public void withCubeFilter(CubeFilter cubeFilter)
    {
        this.cubeFilter = cubeFilter;
    }

    @Override
    public CubeMetadata build()
    {
        return new StarTreeMetadata(
                starTableName,
                sourceTableName,
                tableLastUpdatedTime,
                columns,
                groups,
                cubeFilter,
                cubeLastUpdatedTime,
                cubeStatus);
    }
}
