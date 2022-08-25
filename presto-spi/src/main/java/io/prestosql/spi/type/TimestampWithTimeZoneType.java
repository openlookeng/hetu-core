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
package io.prestosql.spi.type;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;

import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;

public final class TimestampWithTimeZoneType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;
    public static final int MAX_SHORT_PRECISION = 3;
    private static final TimestampWithTimeZoneType[] TYPES = new TimestampWithTimeZoneType[MAX_PRECISION + 1];
    public static final TimestampWithTimeZoneType TIMESTAMP_WITH_TIME_ZONE = new TimestampWithTimeZoneType();
    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_MILLIS = new TimestampWithTimeZoneType();
    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_MICROS = new TimestampWithTimeZoneType();

    private final int precision;

    private TimestampWithTimeZoneType()
    {
        super(parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE));
        this.precision = 0; //xjp,此处随便写的值
    }

    public static TimestampWithTimeZoneType createTimestampWithTimeZoneType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIMESTAMP WITH TIME ZONE precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return new SqlTimestampWithTimeZone(block.getLong(position, 0));
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = unpackMillisUtc(leftBlock.getLong(leftPosition, 0));
        long rightValue = unpackMillisUtc(rightBlock.getLong(rightPosition, 0));
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return AbstractLongType.hash(unpackMillisUtc(block.getLong(position, 0)));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = unpackMillisUtc(leftBlock.getLong(leftPosition, 0));
        long rightValue = unpackMillisUtc(rightBlock.getLong(rightPosition, 0));
        return Long.compare(leftValue, rightValue);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIMESTAMP_WITH_TIME_ZONE;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    public final boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    public final int getPrecision()
    {
        return precision;
    }
}
