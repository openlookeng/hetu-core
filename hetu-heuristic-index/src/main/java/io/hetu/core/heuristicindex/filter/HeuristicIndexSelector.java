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

package io.hetu.core.heuristicindex.filter;

import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.relation.RowExpression;

import java.util.List;

public class HeuristicIndexSelector
{
    private HeuristicIndexSelector()
    {
    }

    public static List<IndexMetadata> select(RowExpression expression, List<IndexMetadata> candidates)
    {
        return candidates;
    }

    public static IndexMetadata pickOne(RowExpression exception, List<IndexMetadata> candidates)
    {
        return candidates.get(0);
    }
}
