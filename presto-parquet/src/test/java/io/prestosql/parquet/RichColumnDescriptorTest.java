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
package io.prestosql.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;

import static org.mockito.MockitoAnnotations.initMocks;

public class RichColumnDescriptorTest
{
    @Mock
    private ColumnDescriptor mockDescriptor;

    private RichColumnDescriptor richColumnDescriptorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        richColumnDescriptorUnderTest = new RichColumnDescriptor(mockDescriptor, new PrimitiveType(
                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"));
    }
}
