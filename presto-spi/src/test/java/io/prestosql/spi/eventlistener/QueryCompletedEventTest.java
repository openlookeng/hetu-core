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
package io.prestosql.spi.eventlistener;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.WarningCode;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class QueryCompletedEventTest
{
    @Mock
    private QueryMetadata mockMetadata;
    @Mock
    private QueryStatistics mockStatistics;
    @Mock
    private QueryContext mockContext;
    @Mock
    private QueryIOMetadata mockIoMetadata;

    private QueryCompletedEvent queryCompletedEventUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        queryCompletedEventUnderTest = new QueryCompletedEvent(mockMetadata, mockStatistics, mockContext,
                mockIoMetadata,
                Optional.of(new QueryFailureInfo(new ErrorCode(0, "name", ErrorType.USER_ERROR), Optional.of("value"),
                        Optional.of("value"), Optional.of("value"), Optional.of("value"), "failuresJson")),
                Arrays.asList(new PrestoWarning(new WarningCode(0, "name"), "message")),
                LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC),
                LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC),
                LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC));
    }
}
