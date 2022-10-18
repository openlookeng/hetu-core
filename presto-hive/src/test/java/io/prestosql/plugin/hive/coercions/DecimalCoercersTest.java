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
package io.prestosql.plugin.hive.coercions;

import io.prestosql.spi.type.DecimalType;
import org.testng.annotations.Test;

public class DecimalCoercersTest
{
    @Test
    public void testCreateDecimalToDecimalCoercer()
    {
        // Setup
        final DecimalType fromType = DecimalType.createDecimalType(1);
        final DecimalType toType = DecimalType.createDecimalType(1);

        // Run the test
        DecimalCoercers.createDecimalToDecimalCoercer(DecimalType.createDecimalType(1), DecimalType.createDecimalType(1));
        DecimalCoercers.createDecimalToDecimalCoercer(DecimalType.createDecimalType(1), DecimalType.createDecimalType(19));
        DecimalCoercers.createDecimalToDecimalCoercer(DecimalType.createDecimalType(19), DecimalType.createDecimalType(1));
        DecimalCoercers.createDecimalToDecimalCoercer(DecimalType.createDecimalType(19), DecimalType.createDecimalType(19));
    }

    @Test
    public void testCreateDecimalToDoubleCoercer()
    {
        DecimalCoercers.createDecimalToDoubleCoercer(DecimalType.createDecimalType(1));
        DecimalCoercers.createDecimalToDoubleCoercer(DecimalType.createDecimalType(19));

        // Verify the results
    }

    @Test
    public void testCreateDecimalToRealCoercer()
    {
        DecimalCoercers.createDecimalToRealCoercer(DecimalType.createDecimalType(1));
        DecimalCoercers.createDecimalToRealCoercer(DecimalType.createDecimalType(19));
    }

    @Test
    public void testCreateDoubleToDecimalCoercer()
    {
        DecimalCoercers.createDoubleToDecimalCoercer(DecimalType.createDecimalType(1));
        DecimalCoercers.createDoubleToDecimalCoercer(DecimalType.createDecimalType(19));
    }

    @Test
    public void testCreateRealToDecimalCoercer()
    {
        DecimalCoercers.createRealToDecimalCoercer(DecimalType.createDecimalType(1));
        DecimalCoercers.createRealToDecimalCoercer(DecimalType.createDecimalType(19));
    }
}
