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
import org.apache.iceberg.types.Types;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ColumnIdentityTest
{
    private ColumnIdentity columnIdentityUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        columnIdentityUnderTest = new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.MAP,
                Arrays.asList(new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of())));
    }

    @Test
    public void testGetChildren()
    {
        // Setup
        final List<ColumnIdentity> expectedResult = Arrays.asList(
                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()));

        // Run the test
        final List<ColumnIdentity> result = columnIdentityUnderTest.getChildren();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetChildByFieldId()
    {
        // Setup
        final ColumnIdentity expectedResult = new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE,
                Arrays.asList());

        // Run the test
        final ColumnIdentity result = columnIdentityUnderTest.getChildByFieldId(0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetChildIndexByFieldId()
    {
        // Setup
        // Run the test
        final int result = columnIdentityUnderTest.getChildIndexByFieldId(0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testEquals()
    {
        // Setup
        // Run the test
        final boolean result = columnIdentityUnderTest.equals("o");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHashCode()
    {
        assertEquals(0, columnIdentityUnderTest.hashCode());
    }

    @Test
    public void testToString()
    {
        assertEquals("result", columnIdentityUnderTest.toString());
    }

    @Test
    public void testPrimitiveColumnIdentity()
    {
        // Run the test
        final ColumnIdentity result = ColumnIdentity.primitiveColumnIdentity(0, "name");
        result.getId();
        result.getName();
        result.getTypeCategory();
        result.getChildren();
        result.getChildByFieldId(0);
        result.getChildIndexByFieldId(0);
        result.equals("o");
        result.hashCode();
        result.toString();
    }

    @Test
    public void testCreateColumnIdentity()
    {
        // Setup
        Types.ListType listType = Types.ListType.ofOptional(1, new Types.StringType());
        final Types.NestedField column = Types.NestedField.optional(0, "name", new Types.StringType());
        final Types.NestedField column1 = Types.NestedField.optional(0, "name", listType);
        Types.NestedField.of(1, true, "name", new Types.StringType());
        Types.StructType structType = Types.StructType.of(Arrays.asList(Types.NestedField.of(1, true, "name", new Types.StringType())));
        final Types.NestedField column2 = Types.NestedField.optional(0, "name", structType);
        final Types.NestedField column3 = Types.NestedField.optional(0, "name", Types.MapType.ofOptional(1, 1, new Types.StringType(), new Types.StringType()));

        // Run the test
        ColumnIdentity.createColumnIdentity(column);
        ColumnIdentity.createColumnIdentity(column1);
        ColumnIdentity.createColumnIdentity(column2);
        ColumnIdentity.createColumnIdentity(column3);
    }
}
