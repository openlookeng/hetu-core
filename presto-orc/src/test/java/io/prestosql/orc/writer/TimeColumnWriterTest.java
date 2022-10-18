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
package io.prestosql.orc.writer;

import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class TimeColumnWriterTest
{
    @Mock
    private OrcColumnId mockColumnId;
    @Mock
    private Type mockType;

    private TimeColumnWriter timeColumnWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        timeColumnWriterUnderTest = new TimeColumnWriter(mockColumnId, mockType, CompressionKind.NONE, 1, () -> null);
    }

    @Test
    public void testTransformValue()
    {
        timeColumnWriterUnderTest.transformValue(0L);
    }
}
