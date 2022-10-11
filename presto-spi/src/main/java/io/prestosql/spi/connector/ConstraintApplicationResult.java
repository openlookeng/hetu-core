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
package io.prestosql.spi.connector;

import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConstraintApplicationResult<T>
{
    private final T handle;
    private final TupleDomain<ColumnHandle> remainingFilter;

    private final Optional<ConnectorExpression> remainingExpression;
    private final boolean precalculateStatistics;

    public ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        this.remainingExpression = Optional.empty();
        this.precalculateStatistics = false;
    }

    public ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter, boolean precalculateStatistics)
    {
        this(handle, remainingFilter, Optional.empty(), precalculateStatistics);
    }

    private ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter, Optional<ConnectorExpression> remainingExpression, boolean precalculateStatistics)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        this.precalculateStatistics = precalculateStatistics;
    }

    public T getHandle()
    {
        return handle;
    }

    public TupleDomain<ColumnHandle> getRemainingFilter()
    {
        return remainingFilter;
    }
}
