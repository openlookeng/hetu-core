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

import io.prestosql.configmanager.ConfigConstants;
import io.prestosql.configmanager.ConfigManager;
import io.prestosql.configmanager.ConfigUtil;
import io.prestosql.configmanager.ConfigVersionFileHandler;
import io.prestosql.sql.builder.functioncall.ConfigFunctionParser;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;

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
{
    protected static volatile DefaultConnectorConfigFunctionRewriter instance;

    private BiFunction<FunctionCallArgsPackage, String, String> resultFunctionStringBuilder;

    private Function<FunctionCallArgsPackage, String> propertyNameBuilder;

    private String connectorName;

    private String[] configModuleNames = {ConfigConstants.CONFIG_UDF_MODULE_NAME};

    private ConfigVersionFileHandler configVersionFileHandler;

    private ConfigManager configManager;

    /**
     * the constructor, use an default defined function call args matcher
     *
     * @param connectorName connectorName
     * @param defaultFilePath defaultFilePath
     */
    public DefaultConnectorConfigFunctionRewriter(String connectorName, String defaultFilePath)
    {
        this(connectorName, defaultFilePath, ConfigFunctionParser::baseFunctionArgsToConfigPropertyName, ConfigFunctionParser::baseConfigPropertyValueToFunctionPushDownString);
    }

    /**
     * the constructor, use an user defined function call args matcher
     * we do not publish this constructor in this code version
     *
     * @param connectorName connectorName
     * @param propertyNameBuilder propertyNameBuilder
     * @param resultFunctionStringBuilder argsFunctionStringBuilder
     */
    private DefaultConnectorConfigFunctionRewriter(String connectorName, String defaultFilePath, Function<FunctionCallArgsPackage, String> propertyNameBuilder, BiFunction<FunctionCallArgsPackage, String, String> resultFunctionStringBuilder)
    {
        this.connectorName = requireNonNull(connectorName, "versionName is null");
        this.propertyNameBuilder = requireNonNull(propertyNameBuilder, "signatureBuilder is null");
        this.resultFunctionStringBuilder = requireNonNull(resultFunctionStringBuilder, "argsFunction is null...");
        this.configVersionFileHandler = new ConfigVersionFileHandler();
        String defaultConfigFileName = ConfigUtil.buildFileNameFromCoNameAndVerName(connectorName, ConfigConstants.DEFAULT_VERSION_NAME);
        this.configVersionFileHandler.addVersionConfigFile(defaultConfigFileName, defaultFilePath);
        this.configManager = ConfigManager.newInstance(configVersionFileHandler, connectorName);
    }

    /**
     * function Call rewrite method impl
     *
     * @param functionCallArgsPackage the package of SqlQueryWriter's function call methods args
     * @return result string
     */
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        String functionPropertyName = propertyNameBuilder.apply(functionCallArgsPackage);
        String[] modules = new String[configModuleNames.length + 1];
        System.arraycopy(configModuleNames, 0, modules, 0, configModuleNames.length);
        modules[modules.length - 1] = functionPropertyName;
        Optional<String> propertyValue = this.configManager.getConfigPropertyValue(connectorName, ConfigConstants.DEFAULT_VERSION_NAME, modules);
        return propertyValue.map(s -> resultFunctionStringBuilder.apply(functionCallArgsPackage, s)).orElse(null);
    }
}
