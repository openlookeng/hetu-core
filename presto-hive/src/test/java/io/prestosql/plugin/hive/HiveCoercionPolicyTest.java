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

import io.prestosql.spi.type.TypeManager;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveCoercionPolicyTest
{
    @Mock
    private TypeManager mockTypeManager;

    private HiveCoercionPolicy hiveCoercionPolicyUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveCoercionPolicyUnderTest = new HiveCoercionPolicy(mockTypeManager);
    }

    @Test
    public void testCanCoerce()
    {
        final boolean result = hiveCoercionPolicyUnderTest.canCoerce(HiveType.HIVE_BINARY, HiveType.HIVE_BOOLEAN);
    }
}
