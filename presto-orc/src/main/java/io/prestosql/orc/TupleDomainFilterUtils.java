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

import io.airlift.slice.Slice;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.prestosql.orc.TupleDomainFilter.ALWAYS_FALSE;
import static io.prestosql.orc.TupleDomainFilter.IS_NOT_NULL;
import static io.prestosql.orc.TupleDomainFilter.IS_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.isVarbinaryType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public class TupleDomainFilterUtils
{
    private TupleDomainFilterUtils() {}

    public static TupleDomainFilter toFilter(Domain domain)
    {
        ValueSet values = domain.getValues();
        checkArgument(values instanceof SortedRangeSet, "Unexpected domain type: " + values.getClass().getSimpleName());

        List<Range> ranges = ((SortedRangeSet) values).getOrderedRanges();
        boolean nullAllowed = domain.isNullAllowed();

        if (ranges.isEmpty() && nullAllowed) {
            return IS_NULL;
        }

        Type type = domain.getType();
        if (ranges.size() == 1) {
            return createRangeFilter(type, ranges.get(0), nullAllowed);
        }

        if (type == BOOLEAN) {
            return createBooleanFilter(ranges, nullAllowed);
        }

        List<TupleDomainFilter> rangeFilters = ranges.stream()
                .map(range -> createRangeFilter(type, range, false))
                .filter(not(ALWAYS_FALSE::equals))
                .collect(toImmutableList());
        if (rangeFilters.isEmpty()) {
            return nullAllowed ? IS_NULL : ALWAYS_FALSE;
        }

        if (rangeFilters.get(0) instanceof TupleDomainFilter.BigintRange) {
            List<TupleDomainFilter.BigintRange> bigintRanges = rangeFilters.stream()
                    .map(TupleDomainFilter.BigintRange.class::cast)
                    .collect(toImmutableList());

            // predicates like
            //  a=2 or a=10
            //  a in (2,10)
            // will get converted to BigIntValues so that filter will be evaluated on multiple possible values.
            if (bigintRanges.stream().allMatch(TupleDomainFilter.BigintRange::isSingleValue)) {
                return TupleDomainFilter.BigintValues.of(
                        bigintRanges.stream()
                                .mapToLong(TupleDomainFilter.BigintRange::getLower)
                                .toArray(),
                        nullAllowed);
            }

            // Predicates like
            // id not in (1,5,10) will result in multiple ranges as below
            // -9223372036854775808L to 0,
            // 2 to 4,
            // 6 to 10
            // 11 to 9223372036854775807L.
            // So this is treatd as multiple range and filter will be consider to qualify if it falls
            // in any of these ranges.
            return TupleDomainFilter.BigintMultiRange.of(bigintRanges, nullAllowed);
        }

        return getMultiValuesTDF(rangeFilters, nullAllowed);
    }

    private static TupleDomainFilter getMultiValuesTDF(List<TupleDomainFilter> rangeFilters, boolean nullAllowed)
    {
        Set values = new HashSet<>();
        if (rangeFilters.stream().allMatch(TupleDomainFilter::isSingleValue)) {
            if (rangeFilters.get(0) instanceof TupleDomainFilter.BytesRange) {
                values = rangeFilters.stream()
                        .map(x -> new TupleDomainFilter.ExtendedByte(((TupleDomainFilter.BytesRange) x).getLower()))
                        .collect(Collectors.toSet());
            }
            else if (rangeFilters.get(0) instanceof TupleDomainFilter.FloatRange) {
                values = rangeFilters.stream()
                        .map(x -> ((TupleDomainFilter.FloatRange) x).getLower())
                        .collect(Collectors.toSet());
            }
            else if (rangeFilters.get(0) instanceof TupleDomainFilter.DoubleRange) {
                values = rangeFilters.stream()
                        .map(x -> ((TupleDomainFilter.DoubleRange) x).getLower())
                        .collect(Collectors.toSet());
            }
            else if (rangeFilters.get(0) instanceof TupleDomainFilter.LongDecimalRange) {
                values = rangeFilters.stream()
                        .map(x -> new TupleDomainFilter.LongDecimalValue(((TupleDomainFilter.LongDecimalRange) x).getLowerLow(),
                        ((TupleDomainFilter.LongDecimalRange) x).getLowerHigh()))
                        .collect(Collectors.toSet());
            }

            return TupleDomainFilter.MultiValues.of(values, nullAllowed);
        }

        return TupleDomainFilter.MultiRange.of(rangeFilters, nullAllowed);
    }

    private static TupleDomainFilter createBooleanFilter(List<Range> ranges, boolean nullAllowed)
    {
        boolean includesTrue = false;
        boolean includesFalse = false;
        for (Range range : ranges) {
            if (range.includes(Marker.exactly(BOOLEAN, true))) {
                includesTrue = true;
            }
            if (range.includes(Marker.exactly(BOOLEAN, false))) {
                includesFalse = true;
            }
        }

        if (includesTrue && includesFalse) {
            checkArgument(!nullAllowed, "Unexpected range of ALL values");
            return IS_NOT_NULL;
        }

        return TupleDomainFilter.BooleanValue.of(includesTrue, nullAllowed);
    }

    private static TupleDomainFilter createRangeFilter(Type type, Range range, boolean nullAllowed)
    {
        if (range.isAll()) {
            checkArgument(!nullAllowed, "Unexpected range of ALL values");
            return IS_NOT_NULL;
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == TIMESTAMP || type == DATE) {
            return bigintRangeToFilter(range, nullAllowed);
        }
        if (type == BOOLEAN) {
            checkArgument(range.isSingleValue(), "Unexpected range of boolean values");
            return TupleDomainFilter.BooleanValue.of(((Boolean) range.getSingleValue()).booleanValue(), nullAllowed);
        }

        if (type instanceof DecimalType) {
            if (((DecimalType) type).isShort()) {
                return bigintRangeToFilter(range, nullAllowed);
            }
            return longDecimalRangeToFilter(range, nullAllowed);
        }

        if (isVarcharType(type) || type instanceof CharType || isVarbinaryType(type)) {
            return varcharRangeToFilter(range, nullAllowed);
        }

        if (type == DOUBLE) {
            return doubleRangeToFilter(range, nullAllowed);
        }

        if (type == REAL) {
            return floatRangeToFilter(range, nullAllowed);
        }

        throw new UnsupportedOperationException("Unsupported type: " + type.getDisplayName());
    }

    private static TupleDomainFilter doubleRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        double lowerDouble = low.isLowerUnbounded() ? Double.MIN_VALUE : (double) low.getValue();
        double upperDouble = high.isUpperUnbounded() ? Double.MAX_VALUE : (double) high.getValue();
        if (!low.isLowerUnbounded() && Double.isNaN(lowerDouble)) {
            return ALWAYS_FALSE;
        }
        if (!high.isUpperUnbounded() && Double.isNaN(upperDouble)) {
            return ALWAYS_FALSE;
        }
        return TupleDomainFilter.DoubleRange.of(
                lowerDouble,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperDouble,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static TupleDomainFilter bigintRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        long lowerLong = low.isLowerUnbounded() ? Long.MIN_VALUE : (long) low.getValue();
        long upperLong = high.isUpperUnbounded() ? Long.MAX_VALUE : (long) high.getValue();
        if (!high.isUpperUnbounded() && high.getBound() == Marker.Bound.BELOW) {
            --upperLong;
        }
        if (!low.isLowerUnbounded() && low.getBound() == Marker.Bound.ABOVE) {
            ++lowerLong;
        }
        if (upperLong < lowerLong) {
            return ALWAYS_FALSE;
        }
        return TupleDomainFilter.BigintRange.of(lowerLong, upperLong, nullAllowed);
    }

    private static TupleDomainFilter varcharRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        Slice lowerValue = low.isLowerUnbounded() ? null : (Slice) low.getValue();
        Slice upperValue = high.isUpperUnbounded() ? null : (Slice) high.getValue();
        return TupleDomainFilter.BytesRange.of(lowerValue == null ? null : lowerValue.getBytes(),
                low.getBound() == Marker.Bound.ABOVE,
                upperValue == null ? null : upperValue.getBytes(),
                high.getBound() == Marker.Bound.BELOW, nullAllowed);
    }

    private static TupleDomainFilter longDecimalRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        return TupleDomainFilter.LongDecimalRange.of(
                low.isLowerUnbounded() ? Long.MIN_VALUE : ((Slice) low.getValue()).getLong(0),
                low.isLowerUnbounded() ? Long.MIN_VALUE : ((Slice) low.getValue()).getLong(SIZE_OF_LONG),
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                high.isUpperUnbounded() ? Long.MAX_VALUE : ((Slice) high.getValue()).getLong(0),
                high.isUpperUnbounded() ? Long.MAX_VALUE : ((Slice) high.getValue()).getLong(SIZE_OF_LONG),
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }

    private static TupleDomainFilter floatRangeToFilter(Range range, boolean nullAllowed)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        float lowerFloat = low.isLowerUnbounded() ? Float.MIN_VALUE : intBitsToFloat(toIntExact((long) low.getValue()));
        float upperFloat = high.isUpperUnbounded() ? Float.MAX_VALUE : intBitsToFloat(toIntExact((long) high.getValue()));
        if (!low.isLowerUnbounded() && Float.isNaN(lowerFloat)) {
            return ALWAYS_FALSE;
        }
        if (!high.isUpperUnbounded() && Float.isNaN(upperFloat)) {
            return ALWAYS_FALSE;
        }

        return TupleDomainFilter.FloatRange.of(
                lowerFloat,
                low.isLowerUnbounded(),
                low.getBound() == Marker.Bound.ABOVE,
                upperFloat,
                high.isUpperUnbounded(),
                high.getBound() == Marker.Bound.BELOW,
                nullAllowed);
    }
}
