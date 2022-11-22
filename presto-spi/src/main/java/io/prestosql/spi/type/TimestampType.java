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
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;

/**
 * A timestamp is encoded as milliseconds from 1970-01-01T00:00:00 UTC and is to be interpreted as local date time without regards to any time zone.
 */
public final class TimestampType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;
    public static final int MAX_SHORT_PRECISION = 6;
    private static final TimestampType[] TYPES = new TimestampType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TYPES[precision] = (precision <= MAX_SHORT_PRECISION) ? new TimestampType(precision) : new TimestampType(precision);
        }
    }

    public static final TimestampType TIMESTAMP = new TimestampType();
    public static final TimestampType TIMESTAMP_MICROS = new TimestampType();

    private final int precision;
    public static final TimestampType TIMESTAMP_MILLIS = new TimestampType();
    public static final TimestampType TIMESTAMP_NANOS = new TimestampType();
    public static final TimestampType TIMESTAMP_TZ_MILLIS = new TimestampType();
    public static final TimestampType TIMESTAMP_TZ_MICROS = new TimestampType();
    public static final TimestampType TIMESTAMP_TZ_NANOS = new TimestampType();

    private TimestampType()
    {
        super(parseTypeSignature(StandardTypes.TIMESTAMP));
        this.precision = 0;
    }

    private TimestampType(int precision)
    {
        super(parseTypeSignature(StandardTypes.TIMESTAMP));
        this.precision = precision;
    }

    public static TimestampType createTimestampWithTimeZoneType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIMESTAMP WITH TIME ZONE precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    public static TimestampType createTimestampType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIMESTAMP precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlTimestamp(block.getLong(position, 0));
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIMESTAMP;
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

    public int getPrecision()
    {
        return precision;
    }
}
