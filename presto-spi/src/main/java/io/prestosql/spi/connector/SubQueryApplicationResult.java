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
package io.prestosql.spi.connector;

import io.prestosql.spi.type.Type;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SubQueryApplicationResult<T>
{
    private final T handle;
    private final Map<String, ColumnHandle> assignments;
    private final Map<String, Type> types;

    public SubQueryApplicationResult(T handle, Map<String, ColumnHandle> assignments, Map<String, Type> types)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.assignments = assignments;
        this.types = types;
    }

    public T getHandle()
    {
        return handle;
    }

    public Map<String, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    public Map<String, Type> getTypes()
    {
        return types;
    }

    public Type getType(String name)
    {
        return this.types.get(name);
    }
}
