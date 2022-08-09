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
package io.hetu.core.plugin.exchange.filesystem.util;

import org.openjdk.jol.info.ClassLayout;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToLongFunction;

import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;

public class HetuSizeOf
{
    private static final int STRING_INSTANCE_SIZE = ClassLayout.parseClass(String.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();
    private static final int OPTIONAL_INSTANCE_SIZE = ClassLayout.parseClass(Optional.class).instanceSize();
    private static final int SIMPLE_ENTRY_INSTANCE_SIZE = ClassLayout.parseClass(AbstractMap.SimpleEntry.class).instanceSize();

    private HetuSizeOf()
    {
    }

    public static <T> long estimatedSizeOf(List<T> list, ToLongFunction<T> valueSize)
    {
        if (list == null) {
            return 0;
        }
        long result = sizeOfObjectArray(list.size());
        for (T value : list) {
            result += valueSize.applyAsLong(value);
        }
        return result;
    }

    public static <K, V> long estimatedSizeOf(Map<K, V> map, ToLongFunction<K> keySize, ToLongFunction<V> valueSize)
    {
        if (map == null) {
            return 0;
        }
        long result = sizeOfObjectArray(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            result += SIMPLE_ENTRY_INSTANCE_SIZE +
                    keySize.applyAsLong(entry.getKey()) +
                    valueSize.applyAsLong(entry.getValue());
        }
        return result;
    }

    public static long estimatedSizeOf(String string)
    {
        return (string == null) ? 0 : (STRING_INSTANCE_SIZE + (long) string.length() * Character.BYTES);
    }

    public static <T> long sizeOf(Optional<T> optional, ToLongFunction<T> valueSize)
    {
        return optional != null && optional.isPresent() ? OPTIONAL_INSTANCE_SIZE + valueSize.applyAsLong(optional.get()) : 0;
    }

    public static long sizeOf(byte[] array)
    {
        return (array == null) ? 0 : sizeOfByteArray(array.length);
    }

    public static long sizeOf(Integer value)
    {
        return value == null ? 0 : INTEGER_INSTANCE_SIZE;
    }
}
