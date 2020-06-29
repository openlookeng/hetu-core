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
package io.prestosql.query;

import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.Query;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlQueryExecutionCacheKeyGenerator
{
    private SqlQueryExecutionCacheKeyGenerator()
    {
    }

    public static int buildKey(Query statement, List<String> tableNames, List<String> planOptimizers, Map<String, Type> columnTypes, TimeZoneKey timeZoneKey, Map<String, Object> systemSessionProperties)
    {
        return Objects.hash(statement, planOptimizers, tableNames, columnTypes, timeZoneKey.hashCode(), systemSessionProperties);
    }
}
