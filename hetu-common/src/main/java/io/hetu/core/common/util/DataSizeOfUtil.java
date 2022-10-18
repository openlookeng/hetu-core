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
package io.hetu.core.common.util;

import io.airlift.units.DataSize;

import static java.util.Objects.requireNonNull;

public class DataSizeOfUtil
        extends DataSize
{
    public DataSizeOfUtil(double size, Unit unit)
    {
        super(size, unit);
    }

    public static DataSize of(long size, DataSize.Unit unit)
    {
        requireNonNull(unit, "unit is null");
        if (0L > size) {
            throw new IllegalArgumentException(String.format("size is negative: %s", new Object[]{size}));
        }
        return new DataSize(size, unit);
    }
}
