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

package io.hetu.core.materializedview.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.TypeSignature;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Mv columns info
 *
 * @since 2020-03-28
 */
public class MaterializedViewColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final long columnId;
    private final TypeSignature columnTypeSignature;

    /**
     * constructor of mv column handle
     *
     * @param columnName column name
     * @param columnId column id
     * @param columnTypeSignature column type signature
     */
    @JsonCreator
    public MaterializedViewColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnId") long columnId,
            @JsonProperty("columnType") TypeSignature columnTypeSignature)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnId = columnId;
        this.columnTypeSignature = columnTypeSignature;
    }

    @JsonProperty
    public String getColumnName()
    {
        return this.columnName;
    }

    @JsonProperty
    public long getColumnId()
    {
        return this.columnId;
    }

    @JsonProperty
    public TypeSignature getColumnTypeSignature()
    {
        return this.columnTypeSignature;
    }

    @Override
    public String toString()
    {
        return columnName + ":" + columnId + ":" + columnTypeSignature;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnId);
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
        MaterializedViewColumnHandle other = (MaterializedViewColumnHandle) obj;
        return Objects.equals(other.getColumnId(), this.columnId);
    }

    public JSONObject toJsonObject() throws JSONException
    {
        JSONObject jo = new JSONObject();
        jo.put("columnName", columnName);
        jo.put("columnId", columnId);
        jo.put("columnType", columnTypeSignature);
        return jo;
    }
}
