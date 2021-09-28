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
package io.prestosql.utils;

import io.prestosql.spi.dynamicfilter.DynamicFilter.DataType;
import io.prestosql.spi.dynamicfilter.DynamicFilter.Type;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.SemiJoinNode;

import java.util.List;

import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.BLOOM_FILTER;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.HASHSET;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.LOCAL;

/**
 * DynamicFilterUtils contains global Dynamic Filter configurations and helper functions
 *
 * @since 2020-04-02
 */
public class DynamicFilterUtils
{
    public static final String FILTERPREFIX = "filter-";
    public static final String PARTIALPREFIX = "partial-";
    public static final String TASKSPREFIX = "tasks-";
    public static final String MERGED_DYNAMIC_FILTERS = "merged-dynamic-filters";
    public static final double BLOOM_FILTER_EXPECTED_FPP = 0.25F;

    private DynamicFilterUtils()
    {
    }

    public static String createKey(String prefix, String key)
    {
        return prefix + "-" + key;
    }

    public static String createKey(String prefix, String filterKey, String queryId)
    {
        return prefix + filterKey + "-" + queryId;
    }

    /**
     * Util function to find all FilterNodes in the same stage as the JoinNode
     *
     * @param node the current visiting JoinNode
     * @return FilterNodes within the same stage and is above a TableScanNode
     */
    public static List<FilterNode> findFilterNodeInStage(JoinNode node)
    {
        List<FilterNode> filterNodes = PlanNodeSearcher
                .searchFrom(node.getLeft())
                .where(DynamicFilterUtils::isFilterAboveTableScan)
                .findAll();
        return filterNodes;
    }

    public static List<FilterNode> findFilterNodeInStage(SemiJoinNode node)
    {
        List<FilterNode> filterNodes = PlanNodeSearcher
                .searchFrom(node.getFilteringSource())
                .where(DynamicFilterUtils::isFilterAboveTableScan)
                .findAll();
        return filterNodes;
    }

    public static boolean isFilterAboveTableScan(PlanNode node)
    {
        if (node instanceof FilterNode) {
            return ((FilterNode) node).getSource() instanceof TableScanNode;
        }
        return false;
    }

    public static DataType getDynamicFilterDataType(Type type, DynamicFilterDataType dataType)
    {
        if (type == LOCAL || dataType == DynamicFilterDataType.HASHSET) {
            return HASHSET;
        }
        else {
            return BLOOM_FILTER;
        }
    }
}
