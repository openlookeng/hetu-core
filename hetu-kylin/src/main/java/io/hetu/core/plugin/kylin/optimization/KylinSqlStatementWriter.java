/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.kylin.optimization;

import io.prestosql.plugin.jdbc.optimization.BaseJdbcSqlStatementWriter;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.spi.sql.RowExpressionConverter;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.StringJoiner;

public class KylinSqlStatementWriter
        extends BaseJdbcSqlStatementWriter
{
    private final JdbcPushDownParameter pushDownParameter;

    public KylinSqlStatementWriter(JdbcPushDownParameter pushDownParameter)
    {
        super(pushDownParameter);
        this.pushDownParameter = pushDownParameter;
    }

    @Override
    public String castAggregationType(String aggregationExpression, RowExpressionConverter converter, Type returnType)
    {
        return aggregationExpression;
    }

    @Override
    public String select(List<Selection> selections)
    {
        StringBuilder builder = new StringBuilder("SELECT ");
        if (selections == null || selections.size() == 0) {
            builder.append("null");
        }
        else {
            StringJoiner joiner = new StringJoiner(", ");
            for (Selection selection : selections) {
                if (selection.isAliased(!pushDownParameter.getCaseInsensitiveParameter())) {
                    joiner.add(selection.getExpression() + " AS " + KylinKeywords.getAlias(selection.getAlias()));
                }
                else {
                    joiner.add(KylinKeywords.getAlias(selection.getExpression()));
                }
            }
            builder.append(joiner);
        }
        return builder.toString();
    }
}
