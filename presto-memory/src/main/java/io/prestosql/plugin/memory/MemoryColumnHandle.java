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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class MemoryColumnHandle
        implements ColumnHandle, Serializable
{
    private static final long serialVersionUID = 7527394454813793397L;
    private static final JsonCodec<TypeSignature> TYPE_SIGNATURE_JSON_CODEC = JsonCodec.jsonCodec(TypeSignature.class);
    private String columnName;
    private int columnIndex;
    private TypeSignature typeSignature;
    private transient Type typeCache;
    private boolean isPartitionKey;

    @JsonCreator
    public MemoryColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("typeSignature") TypeSignature typeSignature,
            @JsonProperty("isPartitionKey") boolean isPartitionKey)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnIndex = requireNonNull(columnIndex, "columnIndex is null");
        this.typeSignature = requireNonNull(typeSignature, "typeSignature is null");
        this.isPartitionKey = requireNonNull(isPartitionKey, "isPartitionKey is null");
    }

    @JsonProperty
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @JsonProperty
    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    @Override
    @JsonProperty("isPartitionKey")
    public boolean isPartitionKey()
    {
        return isPartitionKey;
    }

    public Type getType(TypeManager typeManager)
    {
        if (typeCache == null) {
            typeCache = typeManager.getType(getTypeSignature());
        }
        return typeCache;
    }

    public ColumnMetadata getMetadata(TypeManager typeManager)
    {
        return new ColumnMetadata(columnName, getType(typeManager));
    }

    private void readObject(ObjectInputStream in)
            throws ClassNotFoundException, IOException
    {
        this.columnName = in.readUTF();
        this.columnIndex = in.readInt();
        this.typeSignature = TYPE_SIGNATURE_JSON_CODEC.fromJson(in.readUTF());
        this.isPartitionKey = in.readBoolean();
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException
    {
        out.writeUTF(columnName);
        out.writeInt(columnIndex);
        out.writeUTF(TYPE_SIGNATURE_JSON_CODEC.toJson(typeSignature));
        out.writeBoolean(isPartitionKey);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnIndex, typeSignature, typeCache, isPartitionKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MemoryColumnHandle that = (MemoryColumnHandle) obj;
        return columnIndex == that.columnIndex && isPartitionKey == that.isPartitionKey && columnName.equals(that.columnName) && typeSignature.equals(that.typeSignature) && typeCache.equals(that.typeCache);
    }

    @Override
    public String toString()
    {
        return Integer.toString(columnIndex);
    }
}
