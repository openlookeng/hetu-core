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

package io.prestosql.spi.heuristicindex;

import java.util.Iterator;

public interface IndexFilter
{
    /**
     * Apply the filter on a given expression to check if the index matches the expression.
     *
     * @param expression the expression used to filter the result. e.g. col_a = 10 AND col_b in ("a", "b")
     * @return if ANY index in this filter matches the expression.
     */
    public boolean matches(Object expression);

    /**
     * Apply the filter on a given expression to find the matching positions.
     * <p>
     * Note that not all indices support this method. Some indices don't preserve enough
     * information to get detailed matching positions and therefore only support {@code matches()} method.
     * (e.g. MinMaxIndex, BloomIndex).
     *
     * @param expression the expression used to lookup the result. e.g. col_a = 10 OR col_b < 5
     * @param <I> type to mark the positions. e.g. Integer for row number
     * @return the matching positions according to the indices saved.
     * @throws IndexLookUpException when lookUp() runs into issues and can't determine if a value is there. It is recommended that the caller perform no filtering in this case.
     */
    <I extends Comparable<I>> Iterator<I> lookUp(Object expression)
            throws IndexLookUpException;
}
