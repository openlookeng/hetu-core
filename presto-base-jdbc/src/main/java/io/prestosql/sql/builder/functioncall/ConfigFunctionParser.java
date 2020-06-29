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

import java.util.Locale;

import static java.lang.String.format;

/**
 * Function Signature Parser to Rewrite the function args to function signature and function name
 *
 * @since 2019-12-18
 */
public class ConfigFunctionParser
{
    private ConfigFunctionParser()
    {
    }

    /**
     * the default Function Re-builder
     *
     * @param functionCallArgsPackage Function Call Args Package
     * @param functionPropertyValue Function Property Value
     * @return
     */
    public static String baseConfigPropertyValueToFunctionPushDownString(FunctionCallArgsPackage functionCallArgsPackage, String functionPropertyValue)
    {
        int numArgs = functionCallArgsPackage.getArgumentsList().size();
        String resultString = functionPropertyValue;
        for (int i = 0; i < numArgs; i++) {
            String arg1 = format("$%d", i + 1);
            resultString = resultString.replace(arg1, functionCallArgsPackage.getArgumentsList().get(i));
        }
        return resultString;
    }

    /**
     * the default function signature/propertyKey of a specific FunctionCallArgsPackage
     *
     * @param functionCallArgsPackage Function Call Args Package
     * @return function signature/propertyKey
     */
    public static String baseFunctionArgsToConfigPropertyName(FunctionCallArgsPackage functionCallArgsPackage)
    {
        String functionName = BaseFunctionUtil.formatQualifiedName(functionCallArgsPackage.getName()).toUpperCase(Locale.ENGLISH);
        StringBuilder funcSignatureSb = new StringBuilder(functionName);
        funcSignatureSb.append("(");
        for (int i = 0; i < functionCallArgsPackage.getArgumentsList().size(); i++) {
            funcSignatureSb.append(format("$%d", i + 1));
            if (i != functionCallArgsPackage.getArgumentsList().size() - 1) {
                funcSignatureSb.append(",");
            }
        }
        return funcSignatureSb.append(")").toString();
    }
}
