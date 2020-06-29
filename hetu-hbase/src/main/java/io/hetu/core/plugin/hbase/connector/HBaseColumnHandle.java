/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hbase.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * HBaseColumnHandle: HBase columns info
 *
 * @since 2020-03-30
 */
public class HBaseColumnHandle
        implements ColumnHandle, Comparable<HBaseColumnHandle>
{
    private final boolean indexed;
    private final Optional<String> family;
    private final Optional<String> qualifier;
    private final Type type;
    private final String comment;
    private final String name;
    private final int ordinal;

    /**
     * constructor
     *
     * @param name columnName
     * @param family columnFamily
     * @param qualifier columnQualifier
     * @param type columnType
     * @param ordinal columnId
     * @param comment columnComment
     * @param indexed is indexed
     */
    @JsonCreator
    public HBaseColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("family") Optional<String> family,
            @JsonProperty("qualifier") Optional<String> qualifier,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("comment") String comment,
            @JsonProperty("indexed") boolean indexed)
    {
        this.name = requireNonNull(name, "name is null");
        this.family = requireNonNull(family, "family is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.type = requireNonNull(type, "type is null");
        checkArgument(ordinal >= 0, "ordinal must be >= zero");
        this.ordinal = ordinal;
        this.comment = requireNonNull(comment, "comment is null");
        this.indexed = indexed;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getFamily()
    {
        return family;
    }

    @JsonProperty
    public Optional<String> getQualifier()
    {
        return qualifier;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public int getOrdinal()
    {
        return ordinal;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    /**
     * getColumnMetadata
     *
     * @return ColumnMetadata
     */
    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type, comment, false);
    }

    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexed, name, family, qualifier, type, ordinal, comment);
    }

    /**
     * get column name
     *
     * @return name
     */
    public String getColumnName()
    {
        return name;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (!(obj instanceof HBaseColumnHandle))) {
            return false;
        }

        HBaseColumnHandle other = (HBaseColumnHandle) obj;
        return Objects.equals(this.indexed, other.indexed)
                && Objects.equals(this.name, other.name)
                && Objects.equals(this.family, other.family)
                && Objects.equals(this.qualifier, other.qualifier)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.ordinal, other.ordinal)
                && Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("columnFamily", Optional.ofNullable(family))
                .add("columnQualifier", Optional.ofNullable(qualifier))
                .add("type", type)
                .add("ordinal", ordinal)
                .add("comment", comment)
                .add("indexed", indexed)
                .toString();
    }

    /**
     * parseToJson
     *
     * @return JSONObject
     * @throws JSONException JSONException
     */
    public JSONObject parseToJson()
            throws JSONException
    {
        JSONObject jo = new JSONObject();

        jo.put("name", name);
        jo.put("family", family.orElse(""));
        jo.put("qualifer", qualifier.orElse(""));
        jo.put("type", type.getClass().getName());
        jo.put("ordinal", ordinal);
        jo.put("comment", comment);
        jo.put("indexed", indexed);
        return jo;
    }

    @Override
    public int compareTo(HBaseColumnHandle obj)
    {
        return Integer.compare(this.getOrdinal(), obj.getOrdinal());
    }
}
