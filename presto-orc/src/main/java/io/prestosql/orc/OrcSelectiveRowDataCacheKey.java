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
package io.prestosql.orc;

import java.util.Objects;

public class OrcSelectiveRowDataCacheKey
        extends OrcRowDataCacheKey
{
    private OrcPredicate predicate;

    public OrcPredicate getPredicate()
    {
        return predicate;
    }

    public void setPredicate(OrcPredicate predicate)
    {
        this.predicate = predicate;
    }

    @Override
    public String toString()
    {
        return "OrcSelectiveRowDataCacheKey{" + super.toString() +
                ", Predicate=" + predicate.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OrcSelectiveRowDataCacheKey that = (OrcSelectiveRowDataCacheKey) o;
        return getStripeOffset() == that.getStripeOffset()
                && getRowGroupOffset() == that.getRowGroupOffset()
                && Objects.equals(getOrcDataSourceId(), that.getOrcDataSourceId())
                && Objects.equals(getColumnId(), that.getColumnId())
                && Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getOrcDataSourceId(),
                getStripeOffset(), getRowGroupOffset(),
                getColumnId(), predicate);
    }
}
