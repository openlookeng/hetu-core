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
package io.prestosql.parquet.predicate;

import io.prestosql.parquet.DictionaryPage;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSourceId;
import io.prestosql.parquet.ParquetEncoding;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TupleDomainParquetPredicateTest
{
    @Mock
    private DateTimeZone mockTimeZone;

    private TupleDomainParquetPredicate tupleDomainParquetPredicateUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        tupleDomainParquetPredicateUnderTest = new TupleDomainParquetPredicate(
                TupleDomain.withColumnDomains(new HashMap<>()),
                Arrays.asList(new RichColumnDescriptor(new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                        new PrimitiveType(
                                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"))),
                mockTimeZone);
    }

    @Test
    public void testMatches1() throws Exception
    {
        // Setup
        final Map<ColumnDescriptor, Statistics<?>> statistics = new HashMap<>();
        final ParquetDataSourceId id = new ParquetDataSourceId("id");

        // Run the test
        final boolean result = tupleDomainParquetPredicateUnderTest.matches(0L, statistics, id, false);
    }

    @Test
    public void testMatches1_ThrowsParquetCorruptionException()
    {
        // Setup
        final Map<ColumnDescriptor, Statistics<?>> statistics = new HashMap<>();
        final ParquetDataSourceId id = new ParquetDataSourceId("id");

        // Run the test
        assertThrows(
                ParquetCorruptionException.class,
                () -> tupleDomainParquetPredicateUnderTest.matches(0L, statistics, id, false));
    }

    @Test
    public void testMatches2()
    {
        // Setup
        final Map<ColumnDescriptor, DictionaryDescriptor> dictionaries = new HashMap<>();

        // Run the test
        final boolean result = tupleDomainParquetPredicateUnderTest.matches(dictionaries);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testMatches3() throws Exception
    {
        // Setup
        final Map<ColumnDescriptor, Statistics<?>> statistics = new HashMap<>();
        final ParquetDataSourceId id = new ParquetDataSourceId("id");

        // Run the test
        final boolean result = tupleDomainParquetPredicateUnderTest.matches(0L, statistics, id);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testMatches3_ThrowsParquetCorruptionException()
    {
        // Setup
        final Map<ColumnDescriptor, Statistics<?>> statistics = new HashMap<>();
        final ParquetDataSourceId id = new ParquetDataSourceId("id");

        // Run the test
        assertThrows(
                ParquetCorruptionException.class,
                () -> tupleDomainParquetPredicateUnderTest.matches(0L, statistics, id));
    }

    @Test
    public void testMatches4() throws Exception
    {
        // Setup
        final ColumnIndexStore columnIndexStore = null;
        final ParquetDataSourceId id = new ParquetDataSourceId("id");

        // Run the test
        final boolean result = tupleDomainParquetPredicateUnderTest.matches(0L, columnIndexStore, id);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testMatches4_ThrowsParquetCorruptionException()
    {
        // Setup
        final ColumnIndexStore columnIndexStore = null;
        final ParquetDataSourceId id = new ParquetDataSourceId("id");

        // Run the test
        assertThrows(ParquetCorruptionException.class,
                () -> tupleDomainParquetPredicateUnderTest.matches(0L, columnIndexStore, id));
    }

    @Test
    public void testMatches5()
    {
        // Setup
        final DictionaryDescriptor dictionary = new DictionaryDescriptor(
                new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                Optional.of(new DictionaryPage(null, 0, 0, ParquetEncoding.PLAIN)));

        // Run the test
        final boolean result = tupleDomainParquetPredicateUnderTest.matches(dictionary);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testToParquetFilter()
    {
        // Setup
        final DateTimeZone timeZone = DateTimeZone.forID("id");

        // Run the test
        final Optional<FilterPredicate> result = tupleDomainParquetPredicateUnderTest.toParquetFilter(timeZone);

        // Verify the results
    }

    @Test
    public void testGetDomain1() throws Exception
    {
        // Setup
        final io.prestosql.spi.type.Type type = null;
        final Statistics<?> statistics = Statistics.createStats(null);
        final ParquetDataSourceId id = new ParquetDataSourceId("id");
        final Domain expectedResult = Domain.union(Arrays.asList());

        // Run the test
        final Domain result = TupleDomainParquetPredicate.getDomain(type, 0L, statistics, id, "column", false);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDomain1_ThrowsParquetCorruptionException()
    {
        // Setup
        final io.prestosql.spi.type.Type type = null;
        final Statistics<?> statistics = Statistics.createStats(null);
        final ParquetDataSourceId id = new ParquetDataSourceId("id");

        // Run the test
        assertThrows(
                ParquetCorruptionException.class,
                () -> TupleDomainParquetPredicate.getDomain(type, 0L, statistics, id, "column", false));
    }

    @Test
    public void testGetDomain2()
    {
        // Setup
        final io.prestosql.spi.type.Type type = null;
        final DictionaryDescriptor dictionaryDescriptor = new DictionaryDescriptor(
                new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                Optional.of(new DictionaryPage(null, 0, 0, ParquetEncoding.PLAIN)));
        final Domain expectedResult = Domain.union(Arrays.asList());

        // Run the test
        final Domain result = TupleDomainParquetPredicate.getDomain(type, dictionaryDescriptor);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDomain3() throws Exception
    {
        // Setup
        final ColumnDescriptor column = new ColumnDescriptor(new String[]{"path"},
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0);
        final io.prestosql.spi.type.Type type = null;
        final Statistics<?> statistics = Statistics.createStats(null);
        final ParquetDataSourceId id = new ParquetDataSourceId("id");
        final DateTimeZone timeZone = DateTimeZone.forID("id");
        final Domain expectedResult = Domain.union(Arrays.asList());

        // Run the test
        final Domain result = TupleDomainParquetPredicate.getDomain(column, type, 0L, statistics, id, timeZone);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDomain3_ThrowsParquetCorruptionException()
    {
        // Setup
        final ColumnDescriptor column = new ColumnDescriptor(new String[]{"path"},
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0);
        final io.prestosql.spi.type.Type type = null;
        final Statistics<?> statistics = Statistics.createStats(null);
        final ParquetDataSourceId id = new ParquetDataSourceId("id");
        final DateTimeZone timeZone = DateTimeZone.forID("id");

        // Run the test
        assertThrows(ParquetCorruptionException.class,
                () -> TupleDomainParquetPredicate.getDomain(column, type, 0L, statistics, id, timeZone));
    }

    @Test
    public void testGetDomain4() throws Exception
    {
        // Setup
        final io.prestosql.spi.type.Type type = null;
        final ColumnIndex columnIndex = null;
        final ParquetDataSourceId id = new ParquetDataSourceId("id");
        final RichColumnDescriptor descriptor = new RichColumnDescriptor(
                new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"));
        final DateTimeZone timeZone = DateTimeZone.forID("id");
        final Domain expectedResult = Domain.union(Arrays.asList());

        // Run the test
        final Domain result = TupleDomainParquetPredicate.getDomain(type, 0L, columnIndex, id, descriptor, timeZone);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDomain4_ThrowsParquetCorruptionException()
    {
        // Setup
        final io.prestosql.spi.type.Type type = null;
        final ColumnIndex columnIndex = null;
        final ParquetDataSourceId id = new ParquetDataSourceId("id");
        final RichColumnDescriptor descriptor = new RichColumnDescriptor(
                new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"));
        final DateTimeZone timeZone = DateTimeZone.forID("id");

        // Run the test
        assertThrows(
                ParquetCorruptionException.class,
                () -> TupleDomainParquetPredicate.getDomain(type, 0L, columnIndex, id, descriptor, timeZone));
    }

    @Test
    public void testAsLong()
    {
        assertEquals(0L, TupleDomainParquetPredicate.asLong("value"));
    }
}
