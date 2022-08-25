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
package io.hetu.core.plugin.iceberg;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

public class IcebergTypesTest
{
    @Test
    public void testConvertIcebergValueToTrino()
    {
        // Setup
        final Type icebergType = null;

        // Run the test
        IcebergTypes.convertIcebergValueToTrino(icebergType, null);
        IcebergTypes.convertIcebergValueToTrino(new Types.BooleanType(), true);
        IcebergTypes.convertIcebergValueToTrino(new Types.IntegerType(), 1);
        IcebergTypes.convertIcebergValueToTrino(new Types.LongType(), new Long("10"));
        IcebergTypes.convertIcebergValueToTrino(new Types.FloatType(), 3.33F);
        IcebergTypes.convertIcebergValueToTrino(new Types.DoubleType(), 3.33);
        IcebergTypes.convertIcebergValueToTrino(new Types.StringType(), "value");
        IcebergTypes.convertIcebergValueToTrino(new Types.BinaryType(), ByteBuffer.allocate(10));
        IcebergTypes.convertIcebergValueToTrino(new Types.UUIDType(), UUID.randomUUID());
        IcebergTypes.convertIcebergValueToTrino(new Types.DateType(), 10);
        IcebergTypes.convertIcebergValueToTrino(Types.TimeType.get(), new Long("10"));
        IcebergTypes.convertIcebergValueToTrino(Types.TimestampType.withZone(), new Long("10"));
        IcebergTypes.convertIcebergValueToTrino(Types.DecimalType.of(1, 1), new BigDecimal(10));

        // Verify the results
    }
}
