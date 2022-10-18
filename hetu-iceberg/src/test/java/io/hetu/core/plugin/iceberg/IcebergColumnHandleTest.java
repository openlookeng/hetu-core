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

import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergColumnHandleTest
{
    @Mock
    private ColumnIdentity mockBaseColumnIdentity;
    @Mock
    private Type mockBaseType;
    @Mock
    private Type mockType;

    private IcebergColumnHandle icebergColumnHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        ColumnIdentity name = new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList());
        icebergColumnHandleUnderTest = new IcebergColumnHandle(new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.ARRAY, Arrays.asList(name)), mockBaseType,
                Arrays.asList(1), mockType, Optional.of("value"));
    }

    @Test
    public void testGetColumnIdentity()
    {
        // Run the test
        final ColumnIdentity result = icebergColumnHandleUnderTest.getColumnIdentity();
    }

    @Test
    public void testIsUpdateRowIdColumn()
    {
        icebergColumnHandleUnderTest.isUpdateRowIdColumn();
    }

    @Test
    public void testGetType()
    {
        icebergColumnHandleUnderTest.getType();
    }

    @Test
    public void testGetBaseColumnIdentity()
    {
        icebergColumnHandleUnderTest.getBaseColumnIdentity();
    }

    @Test
    public void testGetBaseType()
    {
        icebergColumnHandleUnderTest.getBaseType();
    }

    @Test
    public void testGetColumn()
    {
        icebergColumnHandleUnderTest.getBaseColumn();
    }

    @Test
    public void testComment()
    {
        icebergColumnHandleUnderTest.getComment();
    }

    @Test
    public void testGetName()
    {
        icebergColumnHandleUnderTest.getName();
    }

    @Test
    public void testGetColumnName()
    {
        icebergColumnHandleUnderTest.getColumnName();
    }

    @Test
    public void testGetQualifiedName()
    {
        icebergColumnHandleUnderTest.getQualifiedName();
    }

    @Test
    public void testPath()
    {
        icebergColumnHandleUnderTest.getPath();
    }

    @Test
    public void testIsBaseColumn()
    {
        // Setup
        // Run the test
        final boolean result = icebergColumnHandleUnderTest.isBaseColumn();
    }

    @Test
    public void testIsRowPositionColumn()
    {
        icebergColumnHandleUnderTest.isRowPositionColumn();
    }

    @Test
    public void testIsIsDeletedColumn()
    {
        icebergColumnHandleUnderTest.isIsDeletedColumn();
    }

    @Test
    public void testHashCode()
    {
        icebergColumnHandleUnderTest.hashCode();
    }

    @Test
    public void testEquals()
    {
        IcebergColumnHandle icebergColumnHandle = new IcebergColumnHandle(new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), mockBaseType,
                Arrays.asList(0), mockType, Optional.of("value"));

        icebergColumnHandleUnderTest.equals(icebergColumnHandle);
        icebergColumnHandleUnderTest.equals("obj");
    }

    @Test
    public void testToString()
    {
        final String result = icebergColumnHandleUnderTest.toString();
    }
}
