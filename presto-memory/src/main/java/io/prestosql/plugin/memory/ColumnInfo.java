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
package io.prestosql.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class ColumnInfo
{
    private final ColumnHandle handle;
    private final String name;
    private final Type type;

    @JsonCreator
    public ColumnInfo(
            @JsonProperty("handle") ColumnHandle handle,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public ColumnHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    public ColumnMetadata getMetadata()
    {
        return new ColumnMetadata(name, type);
    }

    public int getIndex()
    {
        return ((MemoryColumnHandle) handle).getColumnIndex();
    }

    @Override
    public String toString()
    {
        return name + "::" + type;
    }
}
