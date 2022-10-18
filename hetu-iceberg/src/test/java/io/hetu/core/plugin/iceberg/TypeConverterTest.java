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

import io.hetu.core.plugin.iceberg.util.IcebergTestUtil;
import io.prestosql.queryeditorui.output.persistors.FlatFilePersistor;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.spi.type.testing.TestingTypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.hetu.core.plugin.iceberg.TypeConverter.toIcebergType;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.type.UuidType.UUID;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TypeConverterTest
{
    private static TestingTypeManager typeManager = new TestingTypeManager();

    private static final Logger LOG = LoggerFactory.getLogger(FlatFilePersistor.class);

    @Test
    public void testToTrinoType()
    {
        // Run the test
        TypeConverter.toTrinoType(new Types.BooleanType(), typeManager);
        TypeConverter.toTrinoType(new Types.BinaryType(), typeManager);
        TypeConverter.toTrinoType(Types.FixedType.ofLength(10), typeManager);
        TypeConverter.toTrinoType(new Types.DateType(), typeManager);
        TypeConverter.toTrinoType(Types.DecimalType.of(1, 1), typeManager);
        TypeConverter.toTrinoType(new Types.DoubleType(), typeManager);
        TypeConverter.toTrinoType(new Types.LongType(), typeManager);
        TypeConverter.toTrinoType(new Types.FloatType(), typeManager);
        TypeConverter.toTrinoType(new Types.IntegerType(), typeManager);
        TypeConverter.toTrinoType(Types.TimeType.get(), typeManager);
        TypeConverter.toTrinoType(Types.TimestampType.withoutZone(), typeManager);
        TypeConverter.toTrinoType(new Types.StringType(), typeManager);
        TypeConverter.toTrinoType(new Types.UUIDType(), typeManager);
    }

    @Test
    public void testToTrinoType12()
    {
        TypeConverter.toTrinoType(Types.ListType.ofOptional(1, new Types.BooleanType()), typeManager);
    }

    @Test
    public void testToTrinoType2()
    {
        Types.IntegerType type = new Types.IntegerType();
        TypeConverter.toTrinoType(type, typeManager);
    }

    @Test
    public void testToTrinoType3()
    {
        Types.NestedField name = Types.NestedField.optional(10, "aa", () -> Type.TypeID.UUID);
        Types.StructType of = Types.StructType.of(name);
        TypeConverter.toTrinoType(of, typeManager);
    }

    @Test
    public void testToTrinoType4()
    {
        TypeConverter.toTrinoType(Types.MapType.ofOptional(10, 10, () -> Type.TypeID.LONG, () -> Type.TypeID.LONG), typeManager);
    }

    @Test
    public void testToIcebergType2()
    {
        toIcebergType(VarbinaryType.VARBINARY);
        toIcebergType(DateType.DATE);
        toIcebergType(TIME);
    }

    @Test
    public void testToIcebergType()
    {
        toIcebergType(BooleanType.BOOLEAN);
        toIcebergType(IntegerType.INTEGER);
        toIcebergType(BigintType.BIGINT);
        toIcebergType(RealType.REAL);
        toIcebergType(DoubleType.DOUBLE);
        toIcebergType(DecimalType.createDecimalType());
        toIcebergType(VarcharType.createUnboundedVarcharType());
        toIcebergType(CharType.createCharType(3));
        List<io.prestosql.spi.type.Type> types = typeManager.getTypes();
        try {
            toIcebergType(types.get(6));
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
        }
        try {
            toIcebergType(TIMESTAMP_TZ_MICROS);
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
        }
        toIcebergType(UUID);

        List<io.prestosql.spi.type.Type> types1 = types.subList(0, 5);
        List<RowType.Field> localFields = types1.stream()
                .map(type -> new RowType.Field(Optional.of("aa"), type))
                .collect(Collectors.toList());
        RowType from = RowType.from(localFields);
        toIcebergType(from);
    }

    @Test
    public void testToOrcType1()
    {
        Map<String, Type> map = new HashMap<>();
        map.put("a", Types.BooleanType.get());
        map.put("b", Types.IntegerType.get());
        map.put("d", Types.FloatType.get());
        map.put("e", Types.DoubleType.get());
        map.put("f", Types.DateType.get());
        map.put("g", Types.TimeType.get());
        map.put("h", Types.TimestampType.withZone());
        map.put("i", Types.StringType.get());
        map.put("l", Types.DecimalType.of(10, 4));
        map.put("m", Types.UUIDType.get());
        map.put("n", Types.StructType.of(optional(61, "nn", Types.StringType.get())));
        ArrayType arrayType = new ArrayType(VARCHAR);
        Types.ListType listType = Types.ListType.ofOptional(62, toIcebergType(arrayType));
        map.put("o", Types.ListType.ofOptional(63, listType));
        map.put("q", Types.MapType.ofOptional(64, 65, toIcebergType(IntegerType.INTEGER), toIcebergType(IntegerType.INTEGER)));
        Schema schema = IcebergTestUtil.createSchema2(map);
        TypeConverter.toOrcType(schema);
    }
}
