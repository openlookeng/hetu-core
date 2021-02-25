/*
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
package io.prestosql.sql.planner;

import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.SortItem;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class OrderingSchemeUtils
{
    private OrderingSchemeUtils() {}

    public static OrderingScheme fromOrderBy(OrderBy orderBy)
    {
        return new OrderingScheme(
                orderBy.getSortItems().stream()
                        .map(SortItem::getSortKey)
                        .map(SymbolUtils::from)
                        .collect(toImmutableList()),
                orderBy.getSortItems().stream()
                        .collect(toImmutableMap(sortItem -> SymbolUtils.from(sortItem.getSortKey()), OrderingSchemeUtils::sortItemToSortOrder)));
    }

    public static SortOrder sortItemToSortOrder(SortItem sortItem)
    {
        if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            return SortOrder.ASC_NULLS_LAST;
        }

        if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
            return SortOrder.DESC_NULLS_FIRST;
        }
        return SortOrder.DESC_NULLS_LAST;
    }
}
