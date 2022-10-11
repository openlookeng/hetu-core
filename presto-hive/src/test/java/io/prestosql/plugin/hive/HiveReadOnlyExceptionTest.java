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

import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveReadOnlyExceptionTest
{
    @Mock
    private SchemaTableName mockTableName;

    private HiveReadOnlyException hiveReadOnlyExceptionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveReadOnlyExceptionUnderTest = new HiveReadOnlyException(mockTableName, Optional.of("value"));
    }

    @Test
    public void test()
    {
        hiveReadOnlyExceptionUnderTest.getPartition();
        hiveReadOnlyExceptionUnderTest.getTableName();
    }
}
