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
package io.hetu.core.plugin.clickhouse.rewrite.functioncall;

import io.hetu.core.plugin.clickhouse.rewrite.RewriteUtil;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

import java.util.List;
import java.util.Locale;

/**
 * Clickhouse date time type conversion functions.
 * The rewrite of this function limits date_parse.
 */
public class DateParseFunctionCallRewriter
        implements FunctionCallRewriter
{
    public static final String BUILD_IN_FUNC_DATE_PARSE = "date_parse";

    @Override
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        String functionName = RewriteUtil.formatQualifiedName(functionCallArgsPackage.getName());
        if (!functionName.toLowerCase(Locale.ENGLISH).equals("date_parse") || functionCallArgsPackage.getArgumentsList().size() != 2) {
            throw new UnsupportedOperationException("ClickHouse Connector does not support function call of " + RewriteUtil.formatQualifiedName(functionCallArgsPackage.getName()));
        }
        List<String> argsList = functionCallArgsPackage.getArgumentsList();
        String args0 = argsList.get(0);
        String args1 = argsList.get(1);
        if (args1.equals("'%Y-%m-%d %H:%i:%s'")) {
            return "toDateTime(" + args0 + ")";
        }
        else if (args1.equals("'%Y-%m-%d'")) {
            return "toDate(" + args0 + ")";
        }
        return String.format("parseDateTimeBestEffort(%s)", args0);
    }
}
