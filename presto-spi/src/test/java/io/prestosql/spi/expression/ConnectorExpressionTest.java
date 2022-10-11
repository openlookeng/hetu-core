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
package io.prestosql.spi.expression;

import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;

import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

public class ConnectorExpressionTest
{
    @Mock
    private Type mockType;

    private ConnectorExpression connectorExpressionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        connectorExpressionUnderTest = new ConnectorExpression(mockType) {
            @Override
            public String toString()
            {
                return null;
            }

            @Override
            public List<? extends ConnectorExpression> getChildren()
            {
                return null;
            }
        };
    }
}
