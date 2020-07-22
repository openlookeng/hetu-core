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
package io.prestosql.heuristicindex;

import io.prestosql.metadata.Split;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.Operator;
import io.prestosql.utils.Predicate;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.utils.SplitUtils.getSplitKey;

public class SplitFilterFactory
{
    public static IndexCache indexCache;

    public SplitFilterFactory(IndexCache indexCache)
    {
        SplitFilterFactory.indexCache = indexCache;
    }

    /**
     * <pre>
     * Retrieves the corresponding indices for the given predicate and splits.
     * This method uses Cache to improve index loading performance, see {@link IndexCache}
     * </pre>
     *
     * @param predicate Query predicate value
     * @param splits    A list of split of data
     * @return A SplitFilter object containing all available indices for the passed in splits and predicate.
     */
    public SplitFilter getFilter(Predicate predicate, List<Split> splits)
    {
        Map<String, List<IndexMetadata>> indices = new ConcurrentHashMap<>();
        splits.stream().parallel().forEach(split -> {
            String splitKey = getSplitKey(split);
            if (!indices.containsKey(splitKey)) {
                List<IndexMetadata> allIndices = indexCache.getIndices(predicate.getTableName(), predicate.getColumnName(), split);
                List<IndexMetadata> matchingIndices = new LinkedList<>();

                for (IndexMetadata i : allIndices) {
                    if (i.getIndex().supports(Operator.fromValue(predicate.getOperator().getValue()))) {
                        matchingIndices.add(i);
                    }
                }

                if (!matchingIndices.isEmpty()) {
                    indices.put(splitKey, matchingIndices);
                }
            }
        });
        return new SplitFilter(indices, predicate.getOperator());
    }
}
