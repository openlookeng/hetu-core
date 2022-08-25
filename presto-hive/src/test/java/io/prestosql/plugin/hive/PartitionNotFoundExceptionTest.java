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

import io.prestosql.spi.connector.SchemaTableName;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.MockitoAnnotations.initMocks;

public class PartitionNotFoundExceptionTest
{
    @Mock
    private SchemaTableName mockTableName;

    private PartitionNotFoundException partitionNotFoundExceptionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        partitionNotFoundExceptionUnderTest = new PartitionNotFoundException(SchemaTableName.schemaTableName("schemaname", "tablename"),
                Arrays.asList("value"), "message", new Exception("message"));
    }

    @Test
    public void test()
    {
        PartitionNotFoundException partitionNotFoundException = new PartitionNotFoundException(SchemaTableName.schemaTableName("schemaname", "tablename"), Arrays.asList("value"));
        PartitionNotFoundException partitionNotFoundException2 = new PartitionNotFoundException(SchemaTableName.schemaTableName("schemaname", "tablename"), Arrays.asList("value"), "msg");
        PartitionNotFoundException partitionNotFoundException3 = new PartitionNotFoundException(SchemaTableName.schemaTableName("schemaname", "tablename"), Arrays.asList("value"), new Exception("message"));
    }

    @Test
    public void testGetTableName()
    {
        partitionNotFoundExceptionUnderTest.getTableName();
        partitionNotFoundExceptionUnderTest.getPartitionValues();
    }
}
