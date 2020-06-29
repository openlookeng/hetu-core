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
package io.prestosql.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;

public class CachePredicate
{
    private final TupleDomain<ColumnMetadata> columnMetadataTupleDomain;
    private final String cachePredicateString; // For storing human readable predicate input as it cannot be extracted from a TupleDomain

    @JsonCreator
    public CachePredicate(
            @JsonProperty("columnMetadataTupleDomain") TupleDomain<ColumnMetadata> columnMetadataTupleDomain,
            @JsonProperty("cachePredicateString") String cachePredicateString)
    {
        this.columnMetadataTupleDomain = columnMetadataTupleDomain;
        this.cachePredicateString = cachePredicateString;
    }

    @JsonProperty
    public TupleDomain<ColumnMetadata> getColumnMetadataTupleDomain()
    {
        return columnMetadataTupleDomain;
    }

    @JsonProperty
    public String getCachePredicateString()
    {
        return cachePredicateString;
    }

    @Override
    public String toString()
    {
        return "CachePredicate{" +
                "columnMetadataTupleDomain=" + columnMetadataTupleDomain +
                ", cachePredicateString='" + cachePredicateString + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CachePredicate)) {
            return false;
        }
        CachePredicate that = (CachePredicate) o;
        return Objects.equals(columnMetadataTupleDomain, that.columnMetadataTupleDomain) &&
                Objects.equals(cachePredicateString, that.cachePredicateString);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnMetadataTupleDomain, cachePredicateString);
    }
}
