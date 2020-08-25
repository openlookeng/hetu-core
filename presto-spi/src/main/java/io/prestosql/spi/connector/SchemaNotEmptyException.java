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

import io.prestosql.spi.PrestoException;

import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static java.lang.String.format;

public class SchemaNotEmptyException
        extends PrestoException
{
    private final String schemaName;

    public SchemaNotEmptyException(String schemaName)
    {
        this(schemaName, format("Schema not empty: '%s'", schemaName));
    }

    public SchemaNotEmptyException(String schemaName, String message)
    {
        super(SCHEMA_NOT_EMPTY, message);
        this.schemaName = schemaName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }
}
