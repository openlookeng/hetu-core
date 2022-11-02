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

//
// A time is stored as milliseconds from midnight on 1970-01-01T00:00:00 in the time zone of the session.
// When performing calculations on a time the client's time zone must be taken into account.
//
public final class TimeType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;
    public static final TimeType TIME = new TimeType();

    private static final TimeType[] TYPES = new TimeType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TYPES[precision] = new TimeType(precision);
        }
    }

    public static final TimeType TIME_MICROS = new TimeType();

    private final int precision;

    private TimeType()
    {
        super(parseTypeSignature(StandardTypes.TIME));
        this.precision = 0;
    }

    private TimeType(int precision)
    {
        super(new TypeSignature(StandardTypes.TIME));
        this.precision = precision;
    }

    public static TimeType createTimeType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIME precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlTime(block.getLong(position, 0));
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIME;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    public int getPrecision()
    {
        return precision;
    }
}
