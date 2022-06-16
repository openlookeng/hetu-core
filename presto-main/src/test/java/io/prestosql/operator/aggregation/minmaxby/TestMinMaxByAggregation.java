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
package io.prestosql.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.UnknownType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.block.BlockAssertions.createArrayBigintBlock;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createIntsBlock;
import static io.prestosql.block.BlockAssertions.createLongDecimalsBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createShortDecimalsBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertNotNull;

public class TestMinMaxByAggregation
{
    private static final Metadata METADATA = createTestMetadataManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> orderableTypes = getTypes().stream()
                .filter(Type::isOrderable)
                .collect(toImmutableSet());

        for (Type keyType : orderableTypes) {
            for (Type valueType : getTypes()) {
                if (StateCompiler.getSupportedFieldTypes().contains(valueType.getJavaType())) {
                    assertNotNull(METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, valueType.getTypeSignature(), valueType.getTypeSignature(), keyType.getTypeSignature())));
                    assertNotNull(METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, valueType.getTypeSignature(), valueType.getTypeSignature(), keyType.getTypeSignature())));
                }
            }
        }
    }

    private static List<Type> getTypes()
    {
        return new ImmutableList.Builder<Type>()
                .addAll(METADATA.getFunctionAndTypeManager().getTypes())
                .add(VARCHAR)
                .add(DecimalType.createDecimalType(1))
                .add(RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR, DOUBLE)))
                .build();
    }

    @Test
    public void testMinUnknown()
    {
        InternalAggregationFunction unknownKey = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(UnknownType.NAME), parseTypeSignature(UnknownType.NAME), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                unknownKey,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                unknownKey,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMaxUnknown()
    {
        InternalAggregationFunction unknownKey = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(UnknownType.NAME), parseTypeSignature(UnknownType.NAME), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                unknownKey,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                unknownKey,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMinNull()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                1.0,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                function,
                10.0,
                createDoublesBlock(10.0, 9.0, 8.0, 11.0),
                createDoublesBlock(1.0, null, 2.0, null));
    }

    @Test
    public void testMaxNull()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                null,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                function,
                10.0,
                createDoublesBlock(8.0, 9.0, 10.0, 11.0),
                createDoublesBlock(-2.0, null, -1.0, null));
    }

    @Test
    public void testMinDoubleDouble()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                function,
                3.0,
                createDoublesBlock(3.0, 2.0, 5.0, 3.0),
                createDoublesBlock(1.0, 1.5, 2.0, 4.0));
    }

    @Test
    public void testMaxDoubleDouble()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                function,
                2.0,
                createDoublesBlock(3.0, 2.0, null),
                createDoublesBlock(1.0, 1.5, null));
    }

    @Test
    public void testMinDoubleVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                "z",
                createStringsBlock("z", "a", "x", "b"),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0));

        assertAggregation(
                function,
                "a",
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0));
    }

    @Test
    public void testMaxDoubleVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                "a",
                createStringsBlock("z", "a", null),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                function,
                "hi",
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0));
    }

    @Test
    public void testMinLongLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                function,
                ImmutableList.of(8L, 9L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L))),
                createLongsBlock(1L, 2L, 2L, 3L));

        assertAggregation(
                function,
                ImmutableList.of(2L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L), ImmutableList.of(2L))),
                createLongsBlock(0L, 1L, 2L, -1L));
    }

    @Test
    public void testMinLongArrayLong()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature("array(bigint)")));
        assertAggregation(
                function,
                3L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                function,
                -1L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongArrayLong()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature("array(bigint)")));
        assertAggregation(
                function,
                1L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                function,
                2L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(-8L, 9L), ImmutableList.of(-6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                function,
                ImmutableList.of(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(1L, 2L), null)),
                createLongsBlock(1L, 2L, null));

        assertAggregation(
                function,
                ImmutableList.of(2L, 3L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(2L, 3L), null, asList(1L, 2L))),
                createLongsBlock(0L, 1L, null, -1L));
    }

    @Test
    public void testMinLongDecimalDecimal()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("decimal(19,1)"), parseTypeSignature("decimal(19,1)"), parseTypeSignature("decimal(19,1)")));
        assertAggregation(
                function,
                SqlDecimal.of("2.2"),
                createLongDecimalsBlock("1.1", "2.2", "3.3"),
                createLongDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxLongDecimalDecimal()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("decimal(19,1)"), parseTypeSignature("decimal(19,1)"), parseTypeSignature("decimal(19,1)")));
        assertAggregation(
                function,
                SqlDecimal.of("3.3"),
                createLongDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createLongDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinShortDecimalDecimal()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("decimal(10,1)"), parseTypeSignature("decimal(10,1)"), parseTypeSignature("decimal(10,1)")));
        assertAggregation(
                function,
                SqlDecimal.of("2.2"),
                createShortDecimalsBlock("1.1", "2.2", "3.3"),
                createShortDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxShortDecimalDecimal()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("decimal(10,1)"), parseTypeSignature("decimal(10,1)"), parseTypeSignature("decimal(10,1)")));
        assertAggregation(
                function,
                SqlDecimal.of("3.3"),
                createShortDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createShortDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinBooleanVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                function,
                "b",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinIntegerVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.INTEGER)));
        assertAggregation(
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMaxIntegerVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.INTEGER)));
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMinBooleanLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinLongVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMaxLongVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMinDoubleLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, 3.0));

        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMaxDoubleLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMinSliceLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                function,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMaxSliceLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMinLongArrayLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)")));
        assertAggregation(
                function,
                asList(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArrayLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)")));
        assertAggregation(
                function,
                asList(3L, 3L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinLongArraySlice()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature("array(bigint)")));
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArraySlice()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature("array(bigint)")));
        assertAggregation(
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinUnknownSlice()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(UnknownType.NAME)));
        assertAggregation(
                function,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownSlice()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(UnknownType.NAME)));
        assertAggregation(
                function,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMinUnknownLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("min_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(UnknownType.NAME)));
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                new Signature(QualifiedObjectName.valueOfDefaultFunction("max_by"), AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)"), parseTypeSignature(UnknownType.NAME)));
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }
}
