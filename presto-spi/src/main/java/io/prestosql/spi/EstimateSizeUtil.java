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

import org.openjdk.jol.info.ClassLayout;
import sun.misc.Unsafe;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToLongFunction;

public class EstimateSizeUtil
{
    private static final int SIMPLE_ENTRY_INSTANCE_SIZE = ClassLayout.parseClass(AbstractMap.SimpleEntry.class).instanceSize();
    private static final int STRING_INSTANCE_SIZE = ClassLayout.parseClass(String.class).instanceSize();
    private static final int OPTIONAL_INSTANCE_SIZE = ClassLayout.parseClass(Optional.class).instanceSize();

    private EstimateSizeUtil()
    {
    }

    public static <K, V> long estimatedSizeOf(Map<K, V> map, ToLongFunction<K> keySize, ToLongFunction<V> valueSize)
    {
        if (map == null) {
            return 0L;
        }
        else {
            long result = sizeOfObjectArray(map.size());

            Map.Entry entry;
            for (Iterator var5 = map.entrySet().iterator(); var5.hasNext(); result += (long) SIMPLE_ENTRY_INSTANCE_SIZE + keySize.applyAsLong((K) entry.getKey()) + valueSize.applyAsLong((V) entry.getValue())) {
                entry = (Map.Entry) var5.next();
            }

            return result;
        }
    }

    public static long sizeOfObjectArray(int length)
    {
        return (long) Unsafe.ARRAY_OBJECT_BASE_OFFSET + (long) Unsafe.ARRAY_OBJECT_INDEX_SCALE * (long) length;
    }

    public static <T> long sizeOf(Optional<T> optional, ToLongFunction<T> valueSize)
    {
        return optional != null && optional.isPresent() ? (long) OPTIONAL_INSTANCE_SIZE + valueSize.applyAsLong(optional.get()) : 0L;
    }

    public static <T> long estimatedSizeOf(List<T> list, ToLongFunction<T> valueSize)
    {
        if (list == null) {
            return 0L;
        }
        else {
            long result = sizeOfObjectArray(list.size());

            Object value;
            for (Iterator var4 = list.iterator(); var4.hasNext(); result += valueSize.applyAsLong((T) value)) {
                value = var4.next();
            }

            return result;
        }
    }

    public static long estimatedSizeOf(String string)
    {
        return string == null ? 0L : (long) (STRING_INSTANCE_SIZE + string.length() * 2);
    }

    public static <T> long estimatedSizeOf(Set<T> set, ToLongFunction<T> valueSize)
    {
        if (set == null) {
            return 0L;
        }
        else {
            long result = sizeOfObjectArray(set.size());

            Object value;
            for (Iterator var4 = set.iterator(); var4.hasNext(); result += (long) SIMPLE_ENTRY_INSTANCE_SIZE + valueSize.applyAsLong((T) value)) {
                value = var4.next();
            }

            return result;
        }
    }
}
