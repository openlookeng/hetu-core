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

import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.prestosql.orc.TupleDomainFilter.BigintRange;
import io.prestosql.orc.TupleDomainFilter.BooleanValue;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.SqlTimestamp;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.prestosql.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.prestosql.orc.OrcTester.mapType;
import static io.prestosql.orc.OrcTester.quickSelectiveOrcTester;
import static io.prestosql.orc.TupleDomainFilter.IS_NOT_NULL;
import static io.prestosql.orc.TupleDomainFilter.IS_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestSelectiveOrcReader
{
    private final OrcTester tester = quickSelectiveOrcTester();

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_20 = DecimalType.createDecimalType(20, 2);
    private static final CharType CHAR_10 = createCharType(10);

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, BooleanValue.of(true, true)));
        tester.testRoundTrip(BOOLEAN, ImmutableList.of(true), filters);
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234), BigintRange.of(10, 100, false));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        BigintRange filters1 = BigintRange.of(10, 100, false);
        BigintRange filters2 = BigintRange.of(200, 300, false);
        testRoundTripNumeric(intsBetween(0, 100), TupleDomainFilter.BigintMultiRange.of(ImmutableList.of(filters1, filters2), true));
        long[] values = new long[]{1, 2, 3, 4};
        TupleDomainFilter.BigintValues filter3 = TupleDomainFilter.BigintValues.of(values, true);
        testRoundTripNumeric(intsBetween(0, 100), filter3);
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = IntStream.range(0, 31_234).boxed().collect(toList());
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values, BigintRange.of(4, 14, false));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values, TupleDomainFilter filter)
            throws Exception
    {
        List<Long> longValues = ImmutableList.copyOf(values).stream()
                .map(Number::longValue)
                .collect(toList());

        List<Integer> intValues = longValues.stream()
                .map(Long::intValue) // truncate values to int range
                .collect(toList());

        List<Short> shortValues = longValues.stream()
                .map(Long::shortValue) // truncate values to short range
                .collect(toList());

        List<SqlDate> dateValues = longValues.stream()
                .map(Long::intValue)
                .map(SqlDate::new)
                .collect(toList());

        List<SqlTimestamp> timestamps = longValues.stream()
                .map(timestamp -> sqlTimestampOf(timestamp & Integer.MAX_VALUE, SESSION))
                .collect(toList());

        tester.testRoundTrip(BIGINT, longValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(INTEGER, intValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(SMALLINT, shortValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(DATE, dateValues, ImmutableList.of(ImmutableMap.of(0, filter)));

        tester.testRoundTrip(TIMESTAMP, timestamps, ImmutableList.of(ImmutableMap.of(0, filter)));

        List<Integer> reversedIntValues = new ArrayList<>(intValues);
        Collections.reverse(reversedIntValues);

        List<SqlDate> reversedDateValues = new ArrayList<>(dateValues);
        Collections.reverse(reversedDateValues);
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                while (true) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }

                    T next = delegate.next();
                    position++;
                    if (position <= n) {
                        return next;
                    }
                    position = 0;
                }
            }
        };
    }

    private static TupleDomainFilter stringBetween(boolean nullAllowed, String upper, String lower)
    {
        return TupleDomainFilter.BytesRange.of(lower.getBytes(), false, upper.getBytes(), false, nullAllowed);
    }

    private static TupleDomainFilter stringEquals(boolean nullAllowed, String value)
    {
        return TupleDomainFilter.BytesRange.of(value.getBytes(), false, value.getBytes(), false, nullAllowed);
    }

    private static String toCharValue(Object value, int minLength)
    {
        return Strings.padEnd(value.toString(), minLength, ' ');
    }

    @Test
    public void testVarchars()
            throws Exception
    {
        // dictionary
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, stringEquals(false, "apple")));
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), 1)),
                filters);

        // direct
        filters = ImmutableList.of(
                ImmutableMap.of(0, stringBetween(false, "14", "10")));
        tester.testRoundTrip(VARCHAR,
                intsBetween(0, 1).stream().map(Object::toString).collect(toList()),
                filters);

        //stripe dictionary
        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123"))));

        //empty sequence
        filters = ImmutableList.of(
                ImmutableMap.of(0, stringEquals(false, "")));
        tester.testRoundTrip(VARCHAR, nCopies(1, ""), filters);
    }

    @Test
    public void testChars()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, IS_NULL));
        tester.testRoundTrip(createCharType(2), newArrayList(limit(cycle(ImmutableList.of("aa")), 1)), filters);

        filters = ImmutableList.of(
                ImmutableMap.of(0, stringEquals(false, "a")));
        tester.testRoundTrip(createCharType(1), newArrayList(limit(cycle(ImmutableList.of("a")), 1)),
                filters);

        // char with padding
        tester.testRoundTrip(
                CHAR_10,
                intsBetween(0, 1).stream()
                        .map(i -> toCharValue(i, 10))
                        .collect(toList()));

        // char with 0 truncated length
        tester.testRoundTrip(CHAR_10, newArrayList(limit(cycle(toCharValue("", 10)), 1)));
        filters = ImmutableList.of(
                ImmutableMap.of(0, stringEquals(false, "a")));
        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"))),
                filters);
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_4, decimalSequence("-3000", "1", 60_00, 4, 2));
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, TupleDomainFilter.BigintRange.of(10, 20, true)));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_2, decimalSequence("-30", "1", 60, 2, 1), filters);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnknowType()
            throws Exception
    {
        tester.testRoundTrip(mapType(INTEGER, INTEGER), createList(1, i -> createMap(1)));
    }

    private static <T> List<T> createList(int size, Function<Integer, T> createElement)
    {
        return IntStream.range(0, size).mapToObj(createElement::apply).collect(toList());
    }

    private static Map<Integer, Integer> createMap(int seed)
    {
        int mapSize = Math.abs(seed) % 7 + 1;
        return IntStream.range(0, mapSize).boxed().collect(toImmutableMap(Function.identity(), i -> i + seed));
    }

    private static List<SqlDecimal> decimalSequence(String start, String step, int items, int precision, int scale)
    {
        BigInteger decimalStep = new BigInteger(step);
        List<SqlDecimal> values = new ArrayList<>();
        BigInteger nextValue = new BigInteger(start);
        for (int i = 0; i < items; i++) {
            values.add(new SqlDecimal(nextValue, precision, scale));
            nextValue = nextValue.add(decimalStep);
        }
        return values;
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, TupleDomainFilter.DoubleRange.of(0, false, false, 1_000, false, false, false)),
                ImmutableMap.of(0, IS_NULL),
                ImmutableMap.of(0, IS_NOT_NULL));

        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 20), filters);
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, TupleDomainFilter.DoubleRange.of(0, false, false, 1_000, false, false, false)),
                ImmutableMap.of(0, IS_NULL),
                ImmutableMap.of(0, IS_NOT_NULL));

        tester.testRoundTrip(DOUBLE, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), filters);

        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, 1.0), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0), filters);
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), filters);
    }

    private static List<Double> doubleSequence(double start, double step, int items)
    {
        return IntStream.range(0, items)
                .mapToDouble(i -> start + i * step)
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }
}
