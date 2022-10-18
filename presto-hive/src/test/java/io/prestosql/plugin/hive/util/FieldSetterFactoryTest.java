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
package io.prestosql.plugin.hive.util;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
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
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.joda.time.DateTimeZone;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

public class FieldSetterFactoryTest
{
    @Mock
    private DateTimeZone mockTimeZone;

    private FieldSetterFactory fieldSetterFactoryUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        fieldSetterFactoryUnderTest = new FieldSetterFactory(mockTimeZone);
    }

    @Test
    public void testCreate() throws Exception
    {
        // Setup
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        ArrayList<String> strings = new ArrayList<>();
        strings.add("name");
        structTypeInfo.setAllStructFieldNames(strings);
        ArrayList<TypeInfo> typeInfos = new ArrayList<>();
        typeInfos.add(new CharTypeInfo());
        structTypeInfo.setAllStructFieldTypeInfos(typeInfos);
        final SettableStructObjectInspector rowInspector = new ArrayWritableObjectInspector(structTypeInfo);
        StructField fieldnam = rowInspector.getStructFieldRef("name");

        // Run the test
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, BooleanType.BOOLEAN);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, BigintType.BIGINT);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, IntegerType.INTEGER);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, SmallintType.SMALLINT);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, TinyintType.TINYINT);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, RealType.REAL);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, DoubleType.DOUBLE);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, VarcharType.VARCHAR);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, CharType.createCharType(10));
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, VarbinaryType.VARBINARY);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, DateType.DATE);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, TimestampType.TIMESTAMP);
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, DecimalType.createDecimalType(1, 1));

        // Verify the results
    }

    @Test
    public void test()
    {
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        ArrayList<String> strings = new ArrayList<>();
        strings.add("name");
        structTypeInfo.setAllStructFieldNames(strings);
        ArrayList<TypeInfo> typeInfos = new ArrayList<>();
        typeInfos.add(new CharTypeInfo());
        structTypeInfo.setAllStructFieldTypeInfos(typeInfos);
        final SettableStructObjectInspector rowInspector = new ArrayWritableObjectInspector(structTypeInfo);
        StructField fieldnam = rowInspector.getStructFieldRef("name");
        fieldSetterFactoryUnderTest.create(rowInspector, "row", fieldnam, new Type());
    }

    public static class Type
            implements io.prestosql.spi.type.Type
    {
        @Override
        public TypeSignature getTypeSignature()
        {
            return new TypeSignature("array", Arrays.asList(TypeSignatureParameter.of("array")));
        }

        @Override
        public String getDisplayName()
        {
            return null;
        }

        @Override
        public boolean isComparable()
        {
            return false;
        }

        @Override
        public boolean isOrderable()
        {
            return false;
        }

        @Override
        public Class<?> getJavaType()
        {
            return null;
        }

        @Override
        public List<io.prestosql.spi.type.Type> getTypeParameters()
        {
            return null;
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
        {
            return null;
        }

        @Override
        public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
        {
            return null;
        }

        @Override
        public <T> Object getObjectValue(ConnectorSession session, Block<T> block, int position)
        {
            return null;
        }

        @Override
        public boolean getBoolean(Block block, int position)
        {
            return false;
        }

        @Override
        public long getLong(Block block, int position)
        {
            return 0;
        }

        @Override
        public double getDouble(Block block, int position)
        {
            return 0;
        }

        @Override
        public Slice getSlice(Block block, int position)
        {
            return null;
        }

        @Override
        public <T> Object getObject(Block<T> block, int position)
        {
            return null;
        }

        @Override
        public void writeBoolean(BlockBuilder blockBuilder, boolean value)
        {
        }

        @Override
        public void writeLong(BlockBuilder blockBuilder, long value)
        {
        }

        @Override
        public void writeDouble(BlockBuilder blockBuilder, double value)
        {
        }

        @Override
        public void writeSlice(BlockBuilder blockBuilder, Slice value)
        {
        }

        @Override
        public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
        {
        }

        @Override
        public void writeObject(BlockBuilder blockBuilder, Object value)
        {
        }

        @Override
        public void appendTo(Block block, int position, BlockBuilder blockBuilder)
        {
        }

        @Override
        public <T> boolean equalTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition)
        {
            return false;
        }

        @Override
        public <T> long hash(Block<T> block, int position)
        {
            return 0;
        }

        @Override
        public <T> int compareTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition)
        {
            return 0;
        }
    }
}
