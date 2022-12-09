/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.elasticsearch.optimization;

import org.elasticsearch.search.aggregations.Aggregations;

import java.util.Map;

public class ElasticsearchAggregationsCompositeResult
{
    private Aggregations aggregations;

    private Map<String, Object> groupbyKeyValueMap;

    public ElasticsearchAggregationsCompositeResult(Aggregations aggregations, Map<String, Object> groupbyKeyValueMap)
    {
        this.aggregations = aggregations;
        this.groupbyKeyValueMap = groupbyKeyValueMap;
    }

    public Aggregations getAggregations()
    {
        return aggregations;
    }

    public Map<String, Object> getGroupbyKeyValueMap()
    {
        return groupbyKeyValueMap;
    }
}
