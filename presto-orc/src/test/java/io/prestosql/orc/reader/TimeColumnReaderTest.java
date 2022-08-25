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
package io.prestosql.orc.reader;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.MockitoAnnotations.initMocks;

public class TimeColumnReaderTest
{
    @Mock
    private Type mockType;
    @Mock
    private LocalMemoryContext mockMemoryContext;

    private TimeColumnReader timeColumnReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        timeColumnReaderUnderTest = new TimeColumnReader(mockType,
                new OrcColumn("path", new OrcColumnId(0), "columnName", OrcType.OrcTypeKind.BOOLEAN,
                        new OrcDataSourceId("id"),
                        Arrays.asList(),
                        new HashMap<>()),
                mockMemoryContext);
    }

    @Test
    public void testMaybeTransformValues()
    {
        // Setup
        // Run the test
        timeColumnReaderUnderTest.maybeTransformValues(new long[]{0L}, 0);

        // Verify the results
    }
}
