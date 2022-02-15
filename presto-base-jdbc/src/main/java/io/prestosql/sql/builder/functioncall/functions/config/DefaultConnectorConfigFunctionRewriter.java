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
package io.prestosql.sql.builder.functioncall.functions.config;

import io.prestosql.configmanager.ConfigSupplier;
import io.prestosql.sql.builder.functioncall.ConfigFunctionParser;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Connector Config Function Rewriter
 *
 * @since 2019-12-17
 */
public class DefaultConnectorConfigFunctionRewriter
        implements FunctionCallRewriter
{
    protected static volatile DefaultConnectorConfigFunctionRewriter instance;

    private BiFunction<FunctionCallArgsPackage, String, String> resultFunctionStringBuilder;

    private Function<FunctionCallArgsPackage, String> propertyNameBuilder;

    private String connectorName;

    private ConfigSupplier configSupplier;

    /**
     * the constructor, use an default defined function call args matcher
     *
     * @param connectorName connectorName
     * @param configSupplier default configSupplier
     */
    public DefaultConnectorConfigFunctionRewriter(String connectorName, ConfigSupplier configSupplier)
    {
        this(connectorName, configSupplier, ConfigFunctionParser::baseFunctionArgsToConfigPropertyName,
                ConfigFunctionParser::baseConfigPropertyValueToFunctionPushDownString);
    }

    /**
     * the constructor, use an user defined function call args matcher
     * we do not publish this constructor in this code version
     *
     * @param connectorName connectorName
     * @param propertyNameBuilder propertyNameBuilder
     * @param resultFunctionStringBuilder argsFunctionStringBuilder
     */
    private DefaultConnectorConfigFunctionRewriter(String connectorName, ConfigSupplier configSupplier, Function<FunctionCallArgsPackage, String> propertyNameBuilder, BiFunction<FunctionCallArgsPackage, String, String> resultFunctionStringBuilder)
    {
        this.connectorName = requireNonNull(connectorName, "versionName is null");
        this.propertyNameBuilder = requireNonNull(propertyNameBuilder, "signatureBuilder is null");
        this.resultFunctionStringBuilder = requireNonNull(resultFunctionStringBuilder, "argsFunction is null...");
        this.configSupplier = requireNonNull(configSupplier, "configSupplier is null");
    }

    /**
     * function Call rewrite method impl
     *
     * @param functionCallArgsPackage the package of SqlQueryWriter's function call methods args
     * @return result string
     */
    @Override
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        String functionPropertyName = propertyNameBuilder.apply(functionCallArgsPackage);
        Optional<String> propertyValue = this.configSupplier.getConfigValue(functionPropertyName);
        return propertyValue.map(s -> resultFunctionStringBuilder.apply(functionCallArgsPackage, s)).orElse(null);
    }

    public String getConnectorName()
    {
        return connectorName;
    }
}
