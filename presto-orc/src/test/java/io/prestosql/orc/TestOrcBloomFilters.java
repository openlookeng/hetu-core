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
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcMetadataReader;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.HashableBloomFilter;
import io.prestosql.orc.metadata.statistics.IntegerStatistics;
import io.prestosql.orc.proto.OrcProto;
import io.prestosql.orc.protobuf.CodedInputStream;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import org.apache.orc.util.Murmur3;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.orc.TupleDomainOrcPredicate.checkInBloomFilter;
import static io.prestosql.orc.TupleDomainOrcPredicate.extractDiscreteValues;
import static io.prestosql.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestOrcBloomFilters
{
    private static final byte[] TEST_STRING = "ORC_STRING".getBytes(UTF_8);
    private static final byte[] TEST_STRING_NOT_WRITTEN = "ORC_STRING_not".getBytes(UTF_8);
    private static final int TEST_INTEGER = 12345;
    private static final String COLUMN_0 = "bigint_0";
    private static final String COLUMN_1 = "bigint_1";

    private static final Map<Object, Type> TEST_VALUES = ImmutableMap.<Object, Type>builder()
            .put(wrappedBuffer(TEST_STRING), VARCHAR)
            .put(wrappedBuffer(new byte[] {12, 34, 56}), VARBINARY)
            .put(4312L, BIGINT)
            .put(123, INTEGER)
            .put(789, SMALLINT)
            .put(77, TINYINT)
            .put(901, DATE)
            .put(987654L, TIMESTAMP)
            .put(234.567, DOUBLE)
            .put((long) floatToIntBits(987.654f), REAL)
            .build();

    private static byte[] serializeBloomFilterToIndex(OrcProto.BloomFilter bloomFilter, OrcProto.BloomFilterIndex bloomFilterIndex)
            throws IOException
    {
        assertTrue(bloomFilter.isInitialized());

        OrcProto.BloomFilterIndex.Builder builder = bloomFilterIndex.toBuilder();
        builder.addBloomFilter(bloomFilter);

        OrcProto.BloomFilterIndex index = builder.build();
        assertTrue(index.isInitialized());
        assertEquals(index.getBloomFilterCount(), 1);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        index.writeTo(os);
        os.flush();
        return os.toByteArray();
    }

    private static OrcProto.BloomFilter toOrcBloomFilter(HashableBloomFilter bloomFilter)
    {
        OrcProto.BloomFilter.Builder builder = OrcProto.BloomFilter.newBuilder();
        builder.addAllBitset(Longs.asList(bloomFilter.getBitSet()));
        builder.setNumHashFunctions(bloomFilter.getNumHashFunctions());
        return builder.build();
    }

    private static HashableBloomFilter toBloomFilter(OrcProto.BloomFilter orcBloomFilter)
    {
        return new HashableBloomFilter(Longs.toArray(orcBloomFilter.getBitsetList()), orcBloomFilter.getNumHashFunctions());
    }

    private static byte[] randomBytes(int length)
    {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    @Test
    public void testHiveBloomFilterSerde()
    {
        HashableBloomFilter bloomFilter = new HashableBloomFilter(1_000_000L, 0.05);

        // String
        bloomFilter.add(TEST_STRING);
        assertTrue(bloomFilter.test(TEST_STRING));
        assertTrue(bloomFilter.test(wrappedBuffer(TEST_STRING)));
        assertFalse(bloomFilter.test(TEST_STRING_NOT_WRITTEN));
        assertFalse(bloomFilter.test(wrappedBuffer(TEST_STRING_NOT_WRITTEN)));

        // Integer
        bloomFilter.addLong(TEST_INTEGER);
        assertTrue(bloomFilter.test(TEST_INTEGER));
        assertFalse(bloomFilter.test(TEST_INTEGER + 1));

        // Re-construct
        HashableBloomFilter newBloomFilter = new HashableBloomFilter(bloomFilter.getBitSet(), bloomFilter.getNumHashFunctions());

        // String
        assertTrue(newBloomFilter.test(TEST_STRING));
        assertTrue(newBloomFilter.test(wrappedBuffer(TEST_STRING)));
        assertFalse(newBloomFilter.test(TEST_STRING_NOT_WRITTEN));
        assertFalse(newBloomFilter.test(wrappedBuffer(TEST_STRING_NOT_WRITTEN)));

        // Integer
        assertTrue(newBloomFilter.test(TEST_INTEGER));
        assertFalse(newBloomFilter.test(TEST_INTEGER + 1));
    }

    @Test
    public void testOrcHiveBloomFilterSerde()
            throws Exception
    {
        HashableBloomFilter bloomFilterWrite = new HashableBloomFilter(1000L, 0.05);

        bloomFilterWrite.add(TEST_STRING);
        assertTrue(bloomFilterWrite.test(TEST_STRING));
        assertTrue(bloomFilterWrite.test(wrappedBuffer(TEST_STRING)));

        OrcProto.BloomFilter.Builder bloomFilterBuilder = OrcProto.BloomFilter.newBuilder();
        bloomFilterBuilder.addAllBitset(Longs.asList(bloomFilterWrite.getBitSet()));
        bloomFilterBuilder.setNumHashFunctions(bloomFilterWrite.getNumHashFunctions());

        OrcProto.BloomFilter bloomFilter = bloomFilterBuilder.build();
        OrcProto.BloomFilterIndex bloomFilterIndex = OrcProto.BloomFilterIndex.getDefaultInstance();
        byte[] bytes = serializeBloomFilterToIndex(bloomFilter, bloomFilterIndex);

        // Read through method
        InputStream inputStream = new ByteArrayInputStream(bytes);
        OrcMetadataReader metadataReader = new OrcMetadataReader();
        List<HashableBloomFilter> bloomFilters = metadataReader.readBloomFilterIndexes(inputStream);

        assertEquals(bloomFilters.size(), 1);

        assertTrue(bloomFilters.get(0).test(TEST_STRING));
        assertTrue(bloomFilters.get(0).test(wrappedBuffer(TEST_STRING)));
        assertFalse(bloomFilters.get(0).test(TEST_STRING_NOT_WRITTEN));
        assertFalse(bloomFilters.get(0).test(wrappedBuffer(TEST_STRING_NOT_WRITTEN)));

        assertEquals(bloomFilterWrite.getNumBits(), bloomFilters.get(0).getNumBits());
        assertEquals(bloomFilterWrite.getNumHashFunctions(), bloomFilters.get(0).getNumHashFunctions());

        // Validate bit set
        assertTrue(Arrays.equals(bloomFilters.get(0).getBitSet(), bloomFilterWrite.getBitSet()));

        // Read directly: allows better inspection of the bit sets (helped to fix a lot of bugs)
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        OrcProto.BloomFilterIndex deserializedBloomFilterIndex = OrcProto.BloomFilterIndex.parseFrom(input);
        List<OrcProto.BloomFilter> bloomFilterList = deserializedBloomFilterIndex.getBloomFilterList();
        assertEquals(bloomFilterList.size(), 1);

        OrcProto.BloomFilter bloomFilterRead = bloomFilterList.get(0);

        // Validate contents of ORC bloom filter bit set
        assertTrue(Arrays.equals(Longs.toArray(bloomFilterRead.getBitsetList()), bloomFilterWrite.getBitSet()));

        // hash functions
        assertEquals(bloomFilterWrite.getNumHashFunctions(), bloomFilterRead.getNumHashFunctions());

        // bit size
        assertEquals(bloomFilterWrite.getBitSet().length, bloomFilterRead.getBitsetCount());
    }

    @Test
    public void testBloomFilterPredicateValuesExisting()
    {
        HashableBloomFilter bloomFilter = new HashableBloomFilter(TEST_VALUES.size() * 10, 0.01);

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            Object o = testValue.getKey();
            if (o instanceof Long) {
                if (testValue.getValue() instanceof RealType) {
                    bloomFilter.add(intBitsToFloat(((Number) o).intValue()));
                }
                else {
                    bloomFilter.addLong((Long) o);
                }
            }
            else if (o instanceof Integer) {
                bloomFilter.addLong((Integer) o);
            }
            else if (o instanceof String) {
                bloomFilter.add(((String) o).getBytes(UTF_8));
            }
            else if (o instanceof BigDecimal) {
                bloomFilter.add(o.toString().getBytes(UTF_8));
            }
            else if (o instanceof Slice) {
                bloomFilter.add(((Slice) o).getBytes());
            }
            else if (o instanceof Timestamp) {
                bloomFilter.addLong(((Timestamp) o).getTime());
            }
            else if (o instanceof Double) {
                bloomFilter.add((Double) o);
            }
            else {
                fail("Unsupported type " + o.getClass());
            }
        }

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            boolean matched = checkInBloomFilter(bloomFilter, testValue.getKey(), testValue.getValue());
            assertTrue(matched, "type " + testValue.getClass());
        }
    }

    @Test
    public void testBloomFilterPredicateValuesNonExisting()
    {
        HashableBloomFilter bloomFilter = new HashableBloomFilter(TEST_VALUES.size() * 10, 0.01);

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            boolean matched = checkInBloomFilter(bloomFilter, testValue.getKey(), testValue.getValue());
            assertFalse(matched, "type " + testValue.getKey().getClass());
        }
    }

    @Test
    public void testExtractValuesFromSingleDomain()
    {
        Map<Type, Object> testValues = ImmutableMap.<Type, Object>builder()
                .put(BOOLEAN, true)
                .put(INTEGER, 1234L)
                .put(SMALLINT, 789L)
                .put(TINYINT, 77L)
                .put(DATE, 901L)
                .put(TIMESTAMP, 987654L)
                .put(BIGINT, 4321L)
                .put(DOUBLE, 0.123)
                .put(REAL, (long) (floatToIntBits(0.456f)))
                .put(VARCHAR, wrappedBuffer(TEST_STRING))
                .build();

        for (Map.Entry<Type, Object> testValue : testValues.entrySet()) {
            Domain predicateDomain = Domain.singleValue(testValue.getKey(), testValue.getValue());
            Optional<Collection<Object>> discreteValues = extractDiscreteValues(predicateDomain.getValues());
            assertTrue(discreteValues.isPresent());
            Collection<Object> objects = discreteValues.get();
            assertEquals(objects.size(), 1);
            assertEquals(objects.iterator().next(), testValue.getValue());
        }
    }

    @Test
    // simulate query on a 2 columns where 1 is used as part of the where, with and without bloom filter
    public void testMatches()
    {
        TupleDomainOrcPredicate predicate = TupleDomainOrcPredicate.builder()
                .setBloomFiltersEnabled(true)
                .addColumn(ROOT_COLUMN, Domain.singleValue(BIGINT, 1234L))
                .build();
        TupleDomainOrcPredicate emptyPredicate = TupleDomainOrcPredicate.builder().build();

        // assemble a matching and a non-matching bloom filter
        HashableBloomFilter bloomFilter = new HashableBloomFilter(1000, 0.01);
        OrcProto.BloomFilter emptyOrcBloomFilter = toOrcBloomFilter(bloomFilter);
        bloomFilter.addLong(1234);
        OrcProto.BloomFilter orcBloomFilter = toOrcBloomFilter(bloomFilter);

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
                toBloomFilter(orcBloomFilter))));

        ColumnMetadata<ColumnStatistics> nonMatchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                toBloomFilter(emptyOrcBloomFilter))));

        ColumnMetadata<ColumnStatistics> withoutBloomFilterStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
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

        assertTrue(predicate.matches(1L, matchingStatisticsByColumnIndex));
        assertTrue(predicate.matches(1L, withoutBloomFilterStatisticsByColumnIndex));
        assertFalse(predicate.matches(1L, nonMatchingStatisticsByColumnIndex));
        assertTrue(emptyPredicate.matches(1L, matchingStatisticsByColumnIndex));
    }

    @Test
    public void testBloomFilterCompatibility()
    {
        for (int n = 0; n < 200; n++) {
            double fpp = ThreadLocalRandom.current().nextDouble(0.01, 0.10);
            int size = ThreadLocalRandom.current().nextInt(100, 10000);
            int entries = ThreadLocalRandom.current().nextInt(size / 2, size);

            HashableBloomFilter actual = new HashableBloomFilter(size, fpp);
            org.apache.orc.util.BloomFilter expected = new org.apache.orc.util.BloomFilter(size, fpp);

            assertFalse(actual.test((Slice) null));
            assertFalse(expected.test(null));

            byte[][] binaryValue = new byte[entries][];
            long[] longValue = new long[entries];
            double[] doubleValue = new double[entries];
            float[] floatValue = new float[entries];

            for (int i = 0; i < entries; i++) {
                binaryValue[i] = randomBytes(ThreadLocalRandom.current().nextInt(100));
                longValue[i] = ThreadLocalRandom.current().nextLong();
                doubleValue[i] = ThreadLocalRandom.current().nextDouble();
                floatValue[i] = ThreadLocalRandom.current().nextFloat();
            }

            for (int i = 0; i < entries; i++) {
                assertFalse(actual.test(binaryValue[i]));
                assertFalse(actual.test(wrappedBuffer(binaryValue[i])));
                assertFalse(actual.test(longValue[i]));
                assertFalse(actual.test(doubleValue[i]));
                assertFalse(actual.test(floatValue[i]));

                assertFalse(expected.test(binaryValue[i]));
                assertFalse(expected.testLong(longValue[i]));
                assertFalse(expected.testDouble(doubleValue[i]));
                assertFalse(expected.testDouble(floatValue[i]));
            }

            for (int i = 0; i < entries; i++) {
                actual.add(binaryValue[i]);
                actual.addLong(longValue[i]);
                actual.add(doubleValue[i]);
                actual.add(floatValue[i]);

                expected.add(binaryValue[i]);
                expected.addLong(longValue[i]);
                expected.addDouble(doubleValue[i]);
                expected.addDouble(floatValue[i]);
            }

            for (int i = 0; i < entries; i++) {
                assertTrue(actual.test(binaryValue[i]));
                assertTrue(actual.test(wrappedBuffer(binaryValue[i])));
                assertTrue(actual.test(longValue[i]));
                assertTrue(actual.test(doubleValue[i]));
                assertTrue(actual.test(floatValue[i]));

                assertTrue(expected.test(binaryValue[i]));
                assertTrue(expected.testLong(longValue[i]));
                assertTrue(expected.testDouble(doubleValue[i]));
                assertTrue(expected.testDouble(floatValue[i]));
            }

            actual.add((Slice) null);
            expected.add(null);

            assertTrue(actual.test((Slice) null));
            assertTrue(actual.test((Slice) null));
            assertTrue(expected.test(null));

            assertEquals(actual.getBitSet(), expected.getBitSet());
        }
    }

    public void testHashCompatibility()
    {
        for (int length = 0; length < 1000; length++) {
            for (int i = 0; i < 100; i++) {
                byte[] bytes = randomBytes(length);
                assertEquals(HashableBloomFilter.OrcMurmur3.hash64(bytes), Murmur3.hash64(bytes));
            }
        }
    }
}
