/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.clickhouse.optimization;

import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcSqlStatementWriter;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.spi.sql.expression.Types;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * It is not clear what is the difference between aggregation and the previous version.
 */
public class ClickHouseSqlStatementWriter
        extends BaseJdbcSqlStatementWriter
{
    protected static final Logger log = Logger.get(ClickHouseSqlStatementWriter.class);

    public ClickHouseSqlStatementWriter(JdbcPushDownParameter pushDownParameter)
    {
        super(pushDownParameter);
    }

    @Override
    public String aggregation(String inputFunctionName, List<String> arguments, boolean isDistinct)
    {
        String functionName = inputFunctionName;
        if (functionName.toUpperCase(Locale.ENGLISH).equals("VARIANCE")) {
            functionName = "varPop";
        }
        return super.aggregation(functionName, arguments, isDistinct);
    }

    @Override
    public String windowFrame(Types.WindowFrameType type, String start, Optional<String> end)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support windows function");
    }

    @Override
    public String window(String functionName, List<String> functionArgs, List<String> partitionBy, Optional<String> orderBy, Optional<String> frame)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support windows function");
    }

    @Override
    public String from(String selections, String from)
    {
        String[] froms = from.split("\\.");
        if (froms.length == 2) {
            return selections + " FROM " + from;
        }
        else if (froms.length == 3) {
            StringBuilder sb = new StringBuilder(froms[1]);
            sb.append(".");
            sb.append(froms[2]);
            return selections + " FROM " + sb.toString();
        }
        else {
            log.error("params more than 3 " + from);
            return selections + " FROM " + from;
        }
    }
}
