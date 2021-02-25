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
package io.hetu.core.plugin.hana.optimization;

import com.google.common.base.Joiner;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcSqlStatementWriter;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

public class HanaSqlStatementWriter
        extends BaseJdbcSqlStatementWriter
{
    public HanaSqlStatementWriter(JdbcPushDownParameter pushDownParameter)
    {
        super(pushDownParameter);
    }

    @Override
    public String aggregation(String functionName, List<String> arguments, boolean isDistinct)
    {
        if (functionName.toUpperCase(Locale.ENGLISH).equals("VARIANCE")) {
            functionName = "VAR";
        }
        return super.aggregation(functionName, arguments, isDistinct);
    }

    @Override
    public String windowFrame(Types.WindowFrameType type, String start, Optional<String> end)
    {
        String frameString = super.windowFrame(type, start, end);
        if (type.name().toLowerCase(Locale.ENGLISH).equals("range")) {
            // should verify the hana default frame and the HeTu default range
            if (frameString.toLowerCase(Locale.ENGLISH).contains("range between unbounded preceding and current row")) {
                return "";
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, "Hana Connector does not support window frame: " + frameString);
            }
        }

        return frameString;
    }

    @Override
    public String window(String functionName, List<String> functionArgs, List<String> partitionBy, Optional<String> orderBy, Optional<String> frame)
    {
        // the window frame has limit to rows in the windowFrame method
        // in hana grammar, ROWS requires a ORDER BY clause to be specified.
        if (frame.isPresent() && frame.get().toLowerCase(Locale.ENGLISH).contains("rows") && !orderBy.isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, "Hana Connector does not support rows window frame without a " + "specified ORDER BY clause");
        }
        // the window frame has limit to rows in the windowFrame method
        if (functionArgs.size() == 0 && frame.isPresent() && frame.get().toLowerCase(Locale.ENGLISH).contains("rows")) {
            throw new PrestoException(NOT_SUPPORTED, "Hana Connector does not support function " + functionName + " with rows, only aggregation support this!");
        }

        List<String> parts = new ArrayList<>();
        if (!partitionBy.isEmpty()) {
            parts.add("PARTITION BY " + Joiner.on(", ").join(partitionBy));
        }
        orderBy.ifPresent(parts::add);
        frame.ifPresent(parts::add);
        String windows = '(' + Joiner.on(' ').join(parts) + ')';

        // Window aggregation does not support DISTINCT, the same as HeTu, do not need to verify here
        String signatureStr = HanaRowExpressionConverter.functionCall(new QualifiedName(Collections.singletonList(functionName)), false, functionArgs, Optional.empty(), Optional.empty(), Optional.empty());
        return " " + signatureStr + " OVER " + windows;
    }
}
