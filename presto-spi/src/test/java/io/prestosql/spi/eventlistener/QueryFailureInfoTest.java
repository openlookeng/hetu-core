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
import org.testng.annotations.BeforeMethod;

import java.util.Optional;

public class QueryFailureInfoTest
{
    private QueryFailureInfo queryFailureInfoUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        queryFailureInfoUnderTest = new QueryFailureInfo(new ErrorCode(0, "name", ErrorType.USER_ERROR),
                Optional.of("value"), Optional.of("value"), Optional.of("value"), Optional.of("value"), "failuresJson");
    }
}
