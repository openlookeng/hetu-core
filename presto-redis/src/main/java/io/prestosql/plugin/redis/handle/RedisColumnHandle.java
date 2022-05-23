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
package io.prestosql.plugin.redis.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RedisColumnHandle
        implements DecoderColumnHandle, Comparable<RedisColumnHandle>
{
    private final int ordinalPosition;
    private final String name;
    private final Type type;

    /**
     * Mapping hint for the decoder. Can be null.
     */
    private final String mapping;

    /**
     * Data format to use (selects the decoder). Can be null.
     */
    private final String dataFormat;

    /**
     * Additional format hint for the selected decoder. Selects a decoder subtype (e.g. which timestamp decoder).
     */
    private final String formatHint;

    /**
     * True if the key decoder should be used, false if the message decoder should be used.
     */
    private final boolean keyDecoder;

    private final String comment;

    private final boolean hidden;

    /**
     * True if the column is internal to the connector and not defined by a table definition.
     */
    private final boolean internal;

    @JsonCreator
    public RedisColumnHandle(
            @JsonProperty("ordinalPosition")final int ordinalPosition,
            @JsonProperty("name")final String name,
            @JsonProperty("type")final Type type,
            @JsonProperty("mapping")final String mapping,
            @JsonProperty("dataFormat")final String dataFormat,
            @JsonProperty("formatHint")final String formatHint,
            @JsonProperty("keyDecoder")final boolean keyDecoder,
            @JsonProperty("comment")final String comment,
            @JsonProperty("hidden")final boolean hidden,
            @JsonProperty("internal")final boolean internal)
    {
        this.ordinalPosition = ordinalPosition;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.mapping = mapping;
        this.dataFormat = dataFormat;
        this.formatHint = formatHint;
        this.keyDecoder = keyDecoder;
        this.comment = comment;
        this.hidden = hidden;
        this.internal = internal;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    @JsonProperty
    public String getMapping()
    {
        return mapping;
    }

    @Override
    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @Override
    @JsonProperty
    public String getFormatHint()
    {
        return formatHint;
    }

    @JsonProperty
    public boolean isKeyDecoder()
    {
        return keyDecoder;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(
                this.name,
                this.type,
                this.comment,
                this.hidden);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ordinalPosition, name, type, mapping, dataFormat, formatHint, keyDecoder, hidden, internal);
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RedisColumnHandle other = (RedisColumnHandle) obj;
        return Objects.equals(this.ordinalPosition, other.ordinalPosition)
                && Objects.equals(this.name, other.name)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.mapping, other.mapping)
                && Objects.equals(this.dataFormat, other.dataFormat)
                && Objects.equals(this.formatHint, other.formatHint)
                && Objects.equals(this.keyDecoder, other.keyDecoder)
                && Objects.equals(this.hidden, other.hidden)
                && Objects.equals(this.internal, other.internal);
    }

    @Override
    public int compareTo(final RedisColumnHandle otherHandle)
    {
        return Integer.compare(this.getOrdinalPosition(), otherHandle.getOrdinalPosition());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("ordinalPosition", ordinalPosition)
                .add("name", name)
                .add("type", type)
                .add("mapping", mapping)
                .add("dataFormat", dataFormat)
                .add("formatHint", formatHint)
                .add("keyDecoder", keyDecoder)
                .add("hidden", hidden)
                .add("internal", internal)
                .toString();
    }
}
