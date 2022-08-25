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
package io.prestosql.spi.session;

public class PropertyMetadataTest
{
//    @Mock
//    private Type mockSqlType;
//
//    public PropertyMetadata<Object> propertyMetadataUnderTest = new ThreadLocal<PropertyMetadata<T>>();
//
//    @BeforeMethod
//    public void setUp() throws Exception
//    {
//        initMocks(this);
//        propertyMetadataUnderTest = new PropertyMetadata<>("name", "description", mockSqlType, Object.class, null,
//                false, val -> {
//            return null;
//        }, val -> {
//            return null;
//        });
//    }
//
//    @Test
//    public void testDecode() throws Exception
//    {
//        // Setup
//        // Run the test
//        propertyMetadataUnderTest.decode("value");
//
//        // Verify the results
//    }
//
//    @Test
//    public void testEncode() throws Exception
//    {
//        // Setup
//        final Object value = null;
//
//        // Run the test
//        final Object result = propertyMetadataUnderTest.encode(value);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testBooleanProperty() throws Exception
//    {
//        // Run the test
//        final PropertyMetadata<Boolean> result = PropertyMetadata.booleanProperty("name", "description", false, false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testIntegerProperty() throws Exception
//    {
//        // Run the test
//        final PropertyMetadata<Integer> result = PropertyMetadata.integerProperty("name", "description", 0, false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testLongProperty()
//    {
//        // Run the test
//        final PropertyMetadata<Long> result = PropertyMetadata.longProperty("name", "description", 0L, false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testDoubleProperty1() throws Exception
//    {
//        // Run the test
//        final PropertyMetadata<Double> result = PropertyMetadata.doubleProperty("name", "description", 0.0, false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testDoubleProperty2() throws Exception
//    {
//        // Setup
//        final Consumer<Double> mockValidation = mock(Consumer.class);
//
//        // Run the test
//        final PropertyMetadata<Double> result = PropertyMetadata.doubleProperty("name", "description", 0.0,
//                mockValidation, false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testStringProperty1() throws Exception
//    {
//        // Run the test
//        final PropertyMetadata<String> result = PropertyMetadata.stringProperty("name", "description", "defaultValue",
//                false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testStringProperty2() throws Exception
//    {
//        // Setup
//        final Consumer<String> mockValidation = mock(Consumer.class);
//
//        // Run the test
//        final PropertyMetadata<String> result = PropertyMetadata.stringProperty("name", "description", "defaultValue",
//                mockValidation, false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testEnumProperty()
//    {
//        // Run the test
//        final PropertyMetadata<Object> result = PropertyMetadata.enumProperty("name", "descriptionPrefix", Object.class,
//                null, false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//        final Object value = null;
//        assertEquals("result", result.encode(value));
//    }
//
//    @Test
//    public void testDataSizeProperty() throws Exception
//    {
//        // Setup
//        final DataSize defaultValue = new DataSize(0.0, DataSize.Unit.BYTE);
//
//        // Run the test
//        final PropertyMetadata<DataSize> result = PropertyMetadata.dataSizeProperty("name", "description", defaultValue,
//                false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//    }
//
//    @Test
//    public void testDurationProperty() throws Exception
//    {
//        // Setup
//        final Duration defaultValue = new Duration(0.0, TimeUnit.MILLISECONDS);
//
//        // Run the test
//        final PropertyMetadata<Duration> result = PropertyMetadata.durationProperty("name", "description", defaultValue,
//                false);
//        assertEquals("name", result.getName());
//        assertEquals("description", result.getDescription());
//        assertEquals(null, result.getSqlType());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(null, result.getDefaultValue());
//        assertTrue(result.isHidden());
//        assertEquals(null, result.decode("value"));
//        final Object value = null;
//        assertEquals("result", result.encode(value));
//    }
}
