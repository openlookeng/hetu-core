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

package io.hetu.core.plugin.datacenter;

import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.sql.builder.BaseSqlQueryWriter;

import java.util.Map;
import java.util.Optional;

/**
 * Implementation of BaseSqlQueryWriter. It knows how to write
 * Hetu SQL for the logical plan.
 */
public class DataCenterSqlQueryWriter
        extends BaseSqlQueryWriter
{
    @Override
    public String formatIdentifier(Optional<Map<String, Selection>> qualifiedNames, String identifier)
    {
        if (qualifiedNames.isPresent()) {
            return qualifiedNames.get().get(identifier).getExpression();
        }
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }
}
