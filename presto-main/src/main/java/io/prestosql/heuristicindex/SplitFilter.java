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

import io.airlift.log.Logger;
import io.prestosql.metadata.Split;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.Operator;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.utils.RangeUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.prestosql.utils.SplitUtils.getSplitKey;

public class SplitFilter
        implements Filter<Split, Object>
{
    private static final Logger LOG = Logger.get(SplitFilter.class);
    private static final List<String> INDEX_ORDER = new ArrayList<String>(2)
    {
        {
            add("MINMAX");
            add("BLOOM");
        }
    };

    private final Map<String, List<IndexMetadata>> indices;
    private ComparisonExpression.Operator operator;

    public SplitFilter(Map<String, List<IndexMetadata>> indices, ComparisonExpression.Operator operator)
    {
        this.indices = indices;
        this.operator = operator;
    }

    @Override
    public List<Split> filter(List<Split> splits, Object value)
    {
        Set<Split> validSplits = Collections.synchronizedSet(new HashSet<>());

        splits.parallelStream().forEach(split -> {
            List<IndexMetadata> splitIndices = indices.get(getSplitKey(split));

            if (splitIndices == null || splitIndices.size() == 0) {
                validSplits.add(split);
                return;
            }

            // Group each type of index together and make sure they are sorted in ascending order
            // with respect to their SplitStart
            Map<String, List<IndexMetadata>> indexGroupMap = new HashMap<>();
            for (IndexMetadata splitIndex : splitIndices) {
                List<IndexMetadata> indexGroup = indexGroupMap.get(splitIndex.getIndex().getId());
                if (indexGroup == null) {
                    indexGroup = new ArrayList<>();
                    indexGroupMap.put(splitIndex.getIndex().getId(), indexGroup);
                }

                insert(indexGroup, splitIndex);
            }

            List<String> sortedIndexTypeKeys = new LinkedList<>(indexGroupMap.keySet());
            sortedIndexTypeKeys.sort(Comparator.comparingInt(e -> INDEX_ORDER.contains(e) ? INDEX_ORDER.indexOf(e) : Integer.MAX_VALUE));

            for (String indexTypeKey : sortedIndexTypeKeys) {
                List<IndexMetadata> validIndices = indexGroupMap.get(indexTypeKey);
                if (validIndices != null) {
                    validIndices = RangeUtil.subArray(validIndices, split.getConnectorSplit().getStartIndex(), split.getConnectorSplit().getEndIndex());
                    // If any index groups returns false, then the value is definitely not in split.
                    // But if they return true, it still could just be false-positive result
                    // So we'll go through all the indices and only include the split if all indices can match the result
                    if (!isMatched(validIndices, value)) {
                        LOG.debug("Split %s is filtered by %s", split.toString(), indexTypeKey);
                        return;
                    }
                }
            }
            // At this point all indices shows "match" with the predicate value, so we cannot filter out the split
            // We'll add the current split as a valid split
            validSplits.add(split);
        });

        return new ArrayList<>(validSplits);
    }

    private boolean isMatched(List<IndexMetadata> indices, Object value)
    {
        for (IndexMetadata indexMetadata : indices) {
            // if there was no valid split index data, then we the whole indices are incomplete/invalid
            if (indexMetadata == null || indexMetadata.getIndex() == null) {
                return true;
            }

            // Since each split might be composed of several splitIndices,
            // if one splitIndex returns true, that means the value can still be found in the split
            // Only when all the splitIndices return false, can we conclude the split does not contain the value
            // that we are searching for
            try {
                if (indexMetadata.getIndex().matches(value, Operator.fromValue(operator.getValue()))) {
                    return true;
                }
            }
            catch (IllegalArgumentException e) {
                // unable to apply the operator with the given value
                // return true since we don't want this split filtered out
                return true;
            }
        }
        return false;
    }

    /**
     * Performs list insertion that guarantees SplitStart are sorted in ascending order
     * Cannot assure order when two SplitStarts are the same
     *
     * @param list List to be inserted element obj
     * @param obj  SplitIndexMetadata to be inserted to the list
     */
    private void insert(List<IndexMetadata> list, IndexMetadata obj)
    {
        int listSize = list.size();
        // If there's no element, just insert it
        if (listSize == 0) {
            list.add(obj);
            return;
        }

        long splitStart = obj.getSplitStart();
        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).getSplitStart() <= splitStart) {
                list.add(i + 1, obj);
                return;
            }
        }
    }
}
