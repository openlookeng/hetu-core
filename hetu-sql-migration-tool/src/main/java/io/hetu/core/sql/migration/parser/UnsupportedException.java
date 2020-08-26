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
package io.hetu.core.sql.migration.parser;

import io.prestosql.sql.parser.ParsingException;

public class UnsupportedException
        extends ParsingException
{
    public UnsupportedException(String message, int line, int charPositionInLine)
    {
        super(message, null, line, charPositionInLine);
    }

    public UnsupportedException(String message)
    {
        this(message, 1, 0);
    }
}
