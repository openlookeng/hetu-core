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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

public class CachePredicateTest
{
    private final ColumnMetadata columnMetadataA = new ColumnMetadata("a", BIGINT);
    private final TupleDomain tupleDomainA = TupleDomain.withColumnDomains(
            ImmutableMap.of(columnMetadataA, Domain.singleValue(BIGINT, 23L)));

    private final ColumnMetadata columnMetadataB = new ColumnMetadata("b", BIGINT);
    private final TupleDomain tupleDomainB = TupleDomain.withColumnDomains(
            ImmutableMap.of(columnMetadataB, Domain.singleValue(BIGINT, 88L)));

    @Test
    public void testGetColumnMetadataTupleDomain()
    {
        CachePredicate cachePredicate = new CachePredicate(tupleDomainA, "a = 23");
        assertEquals(cachePredicate.getColumnMetadataTupleDomain(), tupleDomainA);
    }

    @Test
    public void testGetCachePredicateString()
    {
        CachePredicate cachePredicate = new CachePredicate(tupleDomainA, "a = 23");
        assertEquals(cachePredicate.getCachePredicateString(), "a = 23");
    }

    @Test
    public void testEquals()
    {
        CachePredicate cachePredicate1 = new CachePredicate(tupleDomainA, "a = 23");
        CachePredicate cachePredicate2 = new CachePredicate(tupleDomainB, "b = 88");
        CachePredicate cachePredicate3 = new CachePredicate(tupleDomainA, "a = 23");

        assertNotEquals(cachePredicate1, cachePredicate2);
        assertEquals(cachePredicate1, cachePredicate3);
        assertFalse(cachePredicate1.equals("Just a string!"));
        assertEquals(2, ImmutableSet.of(cachePredicate1, cachePredicate2, cachePredicate3).size());
    }
}
