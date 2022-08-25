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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Assertions.assertContains;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestHiveTypeTranslator
{
    private final TypeTranslator typeTranslator;

    private final Map<String, HiveType> typeTranslationMap;

    public TestHiveTypeTranslator()
    {
        this(new HiveTypeTranslator(), ImmutableMap.of());
    }

    protected TestHiveTypeTranslator(TypeTranslator typeTranslator, Map<String, HiveType> overwriteTranslation)
    {
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");

        ImmutableMap<String, HiveType> hiveTypeTranslationMap = ImmutableMap.<String, HiveType>builder()
                .put("bigint", HiveType.HIVE_LONG)
                .put("integer", HiveType.HIVE_INT)
                .put("smallint", HiveType.HIVE_SHORT)
                .put("tinyint", HiveType.HIVE_BYTE)
                .put("double", HiveType.HIVE_DOUBLE)
                .put("varchar(3)", HiveType.valueOf("varchar(3)"))
                .put("varchar", HiveType.HIVE_STRING)
                .put("date", HiveType.HIVE_DATE)
                .put("timestamp", HiveType.HIVE_TIMESTAMP)
                .put("decimal(5,3)", HiveType.valueOf("decimal(5,3)"))
                .put("varbinary", HiveType.HIVE_BINARY)
                .put("array(timestamp)", HiveType.valueOf("array<timestamp>"))
                .put("map(boolean,varbinary)", HiveType.valueOf("map<boolean,binary>"))
                .put("row(col0 integer,col1 varbinary)", HiveType.valueOf("struct<col0:int,col1:binary>"))
                .build();

        typeTranslationMap = new HashMap<>();
        typeTranslationMap.putAll(hiveTypeTranslationMap);
        typeTranslationMap.putAll(overwriteTranslation);
    }

    @Test
    public void testTypeTranslator()
    {
        for (Map.Entry<String, HiveType> entry : typeTranslationMap.entrySet()) {
            assertTypeTranslation(entry.getKey(), entry.getValue());
        }

        assertInvalidTypeTranslation("row(integer,varbinary)", NOT_SUPPORTED.toErrorCode(), "Anonymous row type is not supported in Hive. Please give each field a name: row(integer,varbinary)");
    }

    @Test
    public void testToTypeInfo()
    {
        HiveTypeTranslator.toTypeInfo(BooleanType.BOOLEAN);
        HiveTypeTranslator.toTypeInfo(BigintType.BIGINT);
        HiveTypeTranslator.toTypeInfo(IntegerType.INTEGER);
        HiveTypeTranslator.toTypeInfo(SmallintType.SMALLINT);
        HiveTypeTranslator.toTypeInfo(TinyintType.TINYINT);
        HiveTypeTranslator.toTypeInfo(RealType.REAL);
        HiveTypeTranslator.toTypeInfo(DoubleType.DOUBLE);
        HiveTypeTranslator.toTypeInfo(VarcharType.createUnboundedVarcharType());
        HiveTypeTranslator.toTypeInfo(CharType.createCharType(2));
        HiveTypeTranslator.toTypeInfo(VarbinaryType.VARBINARY);
        HiveTypeTranslator.toTypeInfo(DateType.DATE);
        HiveTypeTranslator.toTypeInfo(TimestampType.TIMESTAMP);
        HiveTypeTranslator.toTypeInfo(DecimalType.createDecimalType());
    }

    private void assertTypeTranslation(String typeName, HiveType hiveType)
    {
        Type type = HiveTestUtils.TYPE_MANAGER.getType(parseTypeSignature(typeName));
        assertEquals(HiveType.toHiveType(typeTranslator, type), hiveType);
    }

    private void assertInvalidTypeTranslation(String typeName, ErrorCode errorCode, String message)
    {
        Type type = HiveTestUtils.TYPE_MANAGER.getType(parseTypeSignature(typeName));
        try {
            HiveType.toHiveType(typeTranslator, type);
            fail("expected exception");
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), errorCode);
                assertContains(e.getMessage(), message);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }
}
