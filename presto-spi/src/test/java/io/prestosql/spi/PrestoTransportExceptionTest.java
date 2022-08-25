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
package io.prestosql.spi;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;

import static org.mockito.MockitoAnnotations.initMocks;

public class PrestoTransportExceptionTest
{
    @Mock
    private ErrorCodeSupplier mockErrorCodeSupplier;
    @Mock
    private HostAddress mockRemoteHost;

    private PrestoTransportException prestoTransportExceptionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        prestoTransportExceptionUnderTest = new PrestoTransportException(mockErrorCodeSupplier, mockRemoteHost,
                "message", new Exception("message"));
    }
}
