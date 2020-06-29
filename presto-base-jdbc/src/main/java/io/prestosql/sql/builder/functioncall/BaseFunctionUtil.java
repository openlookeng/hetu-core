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
package io.prestosql.sql.builder.functioncall;

import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;

import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

/**
 * Base Function Util
 *
 * @since 2019-12-17
 */
public class BaseFunctionUtil
{
    private BaseFunctionUtil()
    {
    }

    /**
     * formate identifier
     *
     * @param qualifiedNames qualified names
     * @param identifier     identifier
     * @return sql statement
     */
    public static String formatIdentifier(Optional<Map<String, Selection>> qualifiedNames, String identifier)
    {
        if (qualifiedNames.isPresent()) {
            return qualifiedNames.get().get(identifier).getExpression();
        }
        return identifier;
    }

    /**
     * formate qualified name
     *
     * @param name qualified name
     * @return sql statement
     */
    public static String formatQualifiedName(QualifiedName name)
    {
        return name.getParts()
                .stream()
                .map(identifier -> formatIdentifier(Optional.empty(), identifier))
                .collect(joining("."));
    }
}
