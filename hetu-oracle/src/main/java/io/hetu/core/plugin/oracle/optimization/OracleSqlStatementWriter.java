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
package io.hetu.core.plugin.oracle.optimization;

import io.prestosql.plugin.jdbc.optimization.BaseJdbcSqlStatementWriter;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;

public class OracleSqlStatementWriter
        extends BaseJdbcSqlStatementWriter
{
    public OracleSqlStatementWriter(JdbcPushDownParameter pushDownParameter)
    {
        super(pushDownParameter);
    }

    /**
     * Oracle doesn't support limit, use [select * from table where rownum <= count],
     * this must add at last of sql expression
     *
     * @param table table
     * @param count limit count
     * @return limit statement
     */
    @Override
    public String limit(String table, long count)
    {
        return "SELECT * FROM (" + table + ") WHERE ROWNUM <= " + count;
    }
}
