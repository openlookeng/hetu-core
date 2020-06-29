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

import io.prestosql.spi.security.Identity;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Statement;

import java.util.List;
import java.util.Map;

public class CachedSqlQueryExecutionPlan
{
    private final Plan plan;
    private final Statement statement;
    private final List<String> optimizers;
    private final List<String> tableNames; // fully qualified table names
    private final Map<String, TableStatistics> tableStatistics; // table name to table statistics mapping
    private final Map<String, Type> columnTypes;  // map of column names to types
    private final List<Expression> parameters;
    private final TimeZoneKey timeZoneKey;
    private final Identity identity;
    private final Map<String, Object> systemSessionProperties;

    CachedSqlQueryExecutionPlan(
            Statement statement,
            List<String> tableNames,
            Map<String, TableStatistics> tableStatistics,
            List<String> optimizers, Plan plan,
            List<Expression> parameters,
            Map<String, Type> columnTypes,
            TimeZoneKey timeZoneKey,
            Identity identity,
            Map<String, Object> systemSessionProperties)
    {
        this.statement = statement;
        this.tableNames = tableNames;
        this.tableStatistics = tableStatistics;
        this.optimizers = optimizers;
        this.plan = plan;
        this.parameters = parameters;
        this.columnTypes = columnTypes;
        this.timeZoneKey = timeZoneKey;
        this.identity = identity;
        this.systemSessionProperties = systemSessionProperties;
    }

    public Plan getPlan()
    {
        return plan;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public List<String> getOptimizers()
    {
        return optimizers;
    }

    public List<String> getTableNames()
    {
        return tableNames;
    }

    public Map<String, TableStatistics> getTableStatistics()
    {
        return tableStatistics;
    }

    public Map<String, Type> getColumnTypes()
    {
        return columnTypes;
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return this.timeZoneKey;
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public Map<String, Object> getSystemSessionProperties()
    {
        return systemSessionProperties;
    }
}
