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
package io.hetu.core.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.AbstractType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.TIME_MICROS;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_TZ_MICROS;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.type.UuidType.UUID;

public class ExpressionConverterTest
{
    @Test
    public void testToIcebergExpression()
    {
        Map<AbstractType, Object> map = new HashMap<>();
        map.put(BIGINT, 1);
        map.put(INTEGER, 2L);
        map.put(DOUBLE, 3d);
        map.put(REAL, 4);
        map.put(BOOLEAN, true);
        map.put(DATE, new IntArrayBlock(2, Optional.ofNullable(new boolean[] {true, true}), new int[] {1, 2}));
        map.put(TIME_MICROS, 5L);
        map.put(TIMESTAMP_MICROS, 6L);
        map.put(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.INSTANCE_SIZE);
        map.put(VARCHAR, new VariableWidthBlock(0, EMPTY_SLICE, new int[1], Optional.empty()));
        map.put(VARBINARY, EMPTY_SLICE);
        map.put(UUID, java.util.UUID.fromString("00000000-0000-0000-0000-000000000000"));
        map.put(DecimalType.createDecimalType(10, 2), 10L);

        map.forEach((k, v) -> {
            try {
                IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                        new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                        k,
                        ImmutableList.of(),
                        k,
                        Optional.empty());
                TupleDomain<IcebergColumnHandle> domainOfZero = TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(k, v)));
                ExpressionConverter.toIcebergExpression(domainOfZero);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
