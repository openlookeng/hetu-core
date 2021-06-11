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
import io.airlift.json.JsonCodec;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class ColumnInfo
        implements Serializable
{
    private static final long serialVersionUID = 7527394454813793397L;
    private static final JsonCodec<MemoryColumnHandle> MEMORY_COLUMN_HANDLE_JSON_CODEC = JsonCodec.jsonCodec(MemoryColumnHandle.class);
    private MemoryColumnHandle handle;
    private String name;

    @JsonCreator
    public ColumnInfo(
            @JsonProperty("handle") MemoryColumnHandle handle,
            @JsonProperty("name") String name)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.name = requireNonNull(name, "name is null");
    }

    @JsonProperty
    public MemoryColumnHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public Type getType(TypeManager typeManager)
    {
        return handle.getType(typeManager);
    }

    public ColumnMetadata getMetadata(TypeManager typeManager)
    {
        return new ColumnMetadata(name, getType(typeManager));
    }

    public int getIndex()
    {
        return handle.getColumnIndex();
    }

    private void readObject(ObjectInputStream in)
            throws ClassNotFoundException, IOException
    {
        this.handle = MEMORY_COLUMN_HANDLE_JSON_CODEC.fromJson(in.readUTF());
        this.name = in.readUTF();
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException
    {
        out.writeUTF(MEMORY_COLUMN_HANDLE_JSON_CODEC.toJson(handle));
        out.writeUTF(name);
    }

    @Override
    public String toString()
    {
        return name + "::" + handle.getTypeSignature();
    }
}
