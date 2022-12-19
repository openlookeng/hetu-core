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

import com.google.common.collect.ImmutableList;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.IntegerStatistics;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterOrcPredicate
{
    @Mock
    private ColumnHandle mockColumnHandle;

    private HashSetDynamicFilter hashSetDynamicFilterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hashSetDynamicFilterUnderTest = new HashSetDynamicFilter("filterId", mockColumnHandle, new HashSet<>(),
                DynamicFilter.Type.LOCAL);
    }

    @Test
    public void testDFPredicateMatch() throws Exception
    {
        when(mockColumnHandle.getTypeName()).thenReturn("bigint");
        hashSetDynamicFilterUnderTest.getSetValues().add(10L);
        hashSetDynamicFilterUnderTest.getSetValues().add(20L);
        hashSetDynamicFilterUnderTest.setMinMax();

        DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder builder = DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder.builder();
        List<DynamicFilter> dynamicFilters = new ArrayList<>();
        dynamicFilters.add(hashSetDynamicFilterUnderTest);
        builder.addColumn(new OrcColumnId(0), dynamicFilters);
        Optional<OrcPredicate> orcPredicate = builder.build();

        ColumnMetadata<ColumnStatistics> matchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null)));

        boolean matchResult = orcPredicate.get().matches(1, matchingStatisticsByColumnIndex);
        assertTrue(matchResult);
    }

    @Test
    public void testDFPredicateNotMatch() throws Exception
    {
        when(mockColumnHandle.getTypeName()).thenReturn("bigint");
        hashSetDynamicFilterUnderTest.getSetValues().add(10L);
        hashSetDynamicFilterUnderTest.getSetValues().add(20L);
        hashSetDynamicFilterUnderTest.setMinMax();

        DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder builder = DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder.builder();
        List<DynamicFilter> dynamicFilters = new ArrayList<>();
        dynamicFilters.add(hashSetDynamicFilterUnderTest);
        builder.addColumn(new OrcColumnId(0), dynamicFilters);
        Optional<OrcPredicate> orcPredicate = builder.build();

        ColumnMetadata<ColumnStatistics> matchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(100L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null)));

        boolean matchResult = orcPredicate.get().matches(1, matchingStatisticsByColumnIndex);
        assertFalse(matchResult);
    }

    @Test
    public void testDFPredicateMatchWithOutFilterMinMaxStats() throws Exception
    {
        when(mockColumnHandle.getTypeName()).thenReturn("bigint");
        hashSetDynamicFilterUnderTest.getSetValues().add(10L);
        hashSetDynamicFilterUnderTest.getSetValues().add(20L);

        DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder builder = DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder.builder();
        List<DynamicFilter> dynamicFilters = new ArrayList<>();
        dynamicFilters.add(hashSetDynamicFilterUnderTest);
        builder.addColumn(new OrcColumnId(0), dynamicFilters);
        Optional<OrcPredicate> orcPredicate = builder.build();

        ColumnMetadata<ColumnStatistics> matchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null)));

        boolean matchResult = orcPredicate.get().matches(1, matchingStatisticsByColumnIndex);
        assertTrue(matchResult);
    }

    @Test
    public void testDFPredicateMatch_NO_DF_Present() throws Exception
    {
        when(mockColumnHandle.getTypeName()).thenReturn("bigint");
        hashSetDynamicFilterUnderTest.getSetValues().add(10L);
        hashSetDynamicFilterUnderTest.getSetValues().add(20L);

        DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder builder = DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder.builder();
        Optional<OrcPredicate> orcPredicate = builder.build();

        assertTrue(!orcPredicate.isPresent());
    }

    @Test
    public void testDFPredicateMatch_No_Column_Stats() throws Exception
    {
        when(mockColumnHandle.getTypeName()).thenReturn("bigint");
        hashSetDynamicFilterUnderTest.getSetValues().add(10L);
        hashSetDynamicFilterUnderTest.getSetValues().add(20L);
        hashSetDynamicFilterUnderTest.setMinMax();

        DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder builder = DynamicFilterOrcPredicate.DynamicFilterOrcPredicateBuilder.builder();
        List<DynamicFilter> dynamicFilters = new ArrayList<>();
        dynamicFilters.add(hashSetDynamicFilterUnderTest);
        builder.addColumn(new OrcColumnId(0), dynamicFilters);
        Optional<OrcPredicate> orcPredicate = builder.build();

        ColumnMetadata<ColumnStatistics> matchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(100L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null)));

        boolean matchResult = orcPredicate.get().matches(0, matchingStatisticsByColumnIndex);
        assertTrue(matchResult);
    }
}
