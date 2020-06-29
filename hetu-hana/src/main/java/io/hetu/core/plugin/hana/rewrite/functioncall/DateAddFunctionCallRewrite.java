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
package io.hetu.core.plugin.hana.rewrite.functioncall;

import io.hetu.core.plugin.hana.rewrite.RewriteUtil;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

import java.util.List;
import java.util.Locale;

import static java.lang.String.format;

/**
 * rewrite the HeTu date_add(xxx, value, date_column) to hana add_xxx(date_column, value)
 *
 * @since 2019-11-05
 */
public class DateAddFunctionCallRewrite
        implements FunctionCallRewriter
{
    /**
     * function call name of build-in 'DATE_ADD' function
     */
    public static final String BUILD_IN_FUNC_DATE_ADD = "date_add";

    private static final int ARGS_COUNT = 3;

    @Override
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        String functionName = RewriteUtil.formatQualifiedName(functionCallArgsPackage.getName());
        if (!functionName.toLowerCase(Locale.ENGLISH).equals("date_add") || functionCallArgsPackage.getArgumentsList().size() != ARGS_COUNT) {
            throw new UnsupportedOperationException("Hana Connector does not support function call of " + RewriteUtil.formatQualifiedName(functionCallArgsPackage.getName()));
        }
        List<String> argsList = functionCallArgsPackage.getArgumentsList();
        String args0 = argsList.get(0).toLowerCase(Locale.ENGLISH);
        String args1 = argsList.get(1).toLowerCase(Locale.ENGLISH);
        String args2 = argsList.get(argsList.size() - 1).toLowerCase(Locale.ENGLISH);
        if ("'second'".equals(args0)) {
            String result = format("ADD_SECONDS(%s, %s)", args2, args1);
            return result;
        }
        else if ("'minute'".equals(args0)) {
            String result = format("ADD_SECONDS(%s, %s * 60)", args2, args1);
            return result;
        }
        else if ("'hour'".equals(args0)) {
            String result = format("ADD_SECONDS(%s, %s * 3600)", args2, args1);
            return result;
        }
        else if ("'day'".equals(args0)) {
            String result = format("ADD_DAYS(%s, %s)", args2, args1);
            return result;
        }
        else if ("'week'".equals(args0)) {
            String result = format("ADD_DAYS(%s, %s * 7)", args2, args1);
            return result;
        }
        else if ("'month'".equals(args0)) {
            String result = format("ADD_MONTHS(%s, %s)", args2, args1);
            return result;
        }
        else if ("'quarter'".equals(args0)) {
            String result = format("ADD_MONTHS(%s, %s * 3)", args2, args1);
            return result;
        }
        else if ("'year'".equals(args0)) {
            String result = format("ADD_YEARS(%s, %s)", args2, args1);
            return result;
        }
        else {
            throw new UnsupportedOperationException("Hana Connector does not support function call of " + RewriteUtil.formatQualifiedName(functionCallArgsPackage.getName()) + " " + args0);
        }
    }
}
