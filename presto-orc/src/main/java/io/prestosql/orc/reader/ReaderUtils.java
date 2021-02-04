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
package io.prestosql.orc.reader;

import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.spi.type.Type;
import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;

import java.util.function.Predicate;

import static java.lang.Math.max;

final class ReaderUtils
{
    private ReaderUtils() {}

    public static void verifyStreamType(OrcColumn column, Type actual, Predicate<Type> validTypes)
            throws OrcCorruptionException
    {
        if (validTypes.test(actual)) {
            return;
        }

        throw new OrcCorruptionException(
                column.getOrcDataSourceId(),
                "Can not read SQL type %s from ORC stream %s of type %s",
                actual,
                column.getPath(),
                column.getColumnType());
    }

    public static int minNonNullValueSize(int nonNullCount)
    {
        return max(nonNullCount + 1, 1025);
    }

    public static byte[] unpackByteNulls(byte[] values, boolean[] isNull)
    {
        byte[] result = new byte[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[i] = values[position];
            if (!isNull[i]) {
                position++;
            }
        }
        return result;
    }

    public static short[] unpackShortNulls(short[] values, boolean[] isNull)
    {
        short[] result = new short[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[i] = values[position];
            if (!isNull[i]) {
                position++;
            }
        }
        return result;
    }

    public static IntVec unpackIntNulls(int[] values, boolean[] isNull)
    {
        IntVec result = new IntVec(isNull.length);

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result.set(i, values[position]);
            if (!isNull[i]) {
                position++;
            }
        }
        return result;
    }

    public static long[] unpackLongNulls(long[] values, boolean[] isNull)
    {
        long[] result = new long[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[i] = values[position];
            if (!isNull[i]) {
                position++;
            }
        }
        return result;
    }

    public static LongVec unpackLongNullsVec(long[] values, boolean[] isNull)
    {
        LongVec result = new LongVec(isNull.length);

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result.set(i, values[position]);
            if (!isNull[i]) {
                position++;
            }
        }
        return result;
    }

    public static DoubleVec unpackDoubleNulls(double[] values, boolean[] isNull)
    {
        DoubleVec result = new DoubleVec(isNull.length);

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result.set(i, values[position]);
            if (!isNull[i]) {
                position++;
            }
        }
        return result;
    }

    public static long[] unpackInt128Nulls(long[] values, boolean[] isNull)
    {
        long[] result = new long[isNull.length * 2];

        int position = 0;
        int outputPosition = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[outputPosition] = values[position];
            result[outputPosition + 1] = values[position + 1];
            if (!isNull[i]) {
                position += 2;
            }
            outputPosition += 2;
        }
        return result;
    }

    public static void unpackLengthNulls(int[] values, boolean[] isNull, int nonNullCount)
    {
        int nullSuppressedPosition = nonNullCount - 1;
        for (int outputPosition = isNull.length - 1; outputPosition >= 0; outputPosition--) {
            if (isNull[outputPosition]) {
                values[outputPosition] = 0;
            }
            else {
                values[outputPosition] = values[nullSuppressedPosition];
                nullSuppressedPosition--;
            }
        }
    }

    public static void convertLengthVectorToOffsetVector(int[] vector)
    {
        int currentLength = vector[0];
        vector[0] = 0;
        for (int i = 1; i < vector.length; i++) {
            int nextLength = vector[i];
            vector[i] = vector[i - 1] + currentLength;
            currentLength = nextLength;
        }
    }
}
