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

import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;
import io.prestosql.sql.builder.functioncall.functions.config.DefaultConnectorConfigFunctionRewriter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Base Functions Writer Manager for Sub-Query Push Down Optimizer
 *
 * @since 2019-12-17
 */
public final class FunctionWriterManager
{
    private Map<String, FunctionCallRewriter> rewriteFunctionMap = new ConcurrentHashMap<>(Collections.emptyMap());

    private DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter;

    private Function<FunctionCallArgsPackage, String> argsFunction;

    private String connectorName;

    private String versionName;

    /**
     * Base Function Rewriter Manager constructor
     *
     * @param connectorName connectorName
     * @param versionName versionName
     * @param functionCallRewriterMap functionCallRewriterMap
     * @param connectorConfigFunctionRewriter Connector Config Function Rewriter
     * @param functionSignatureBuild function Signature Builder
     */
    private FunctionWriterManager(String connectorName, String versionName, Map<String, FunctionCallRewriter> functionCallRewriterMap, DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter, Function<FunctionCallArgsPackage, String> functionSignatureBuild)
    {
        this.connectorName = connectorName;
        this.versionName = versionName;
        this.rewriteFunctionMap.putAll(functionCallRewriterMap);
        addConfigFunctionRewriter(connectorConfigFunctionRewriter);
        this.argsFunction = functionSignatureBuild;
    }

    /**
     * Base Function Rewriter Manager constructor
     *
     * @param connectorName connectorName
     * @param versionName versionName
     * @param connectorConfigFunctionRewriter Connector Config Function Rewriter
     */
    protected FunctionWriterManager(String connectorName, String versionName, Map<String, FunctionCallRewriter> functionCallRewriterMap, DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter)
    {
        this(connectorName, versionName, functionCallRewriterMap, connectorConfigFunctionRewriter, FunctionCallArgsPackage::defaultBuildFunctionSignature);
    }

    /**
     * add function rewrite support by adding rewrite instance
     *
     * @param functionSignature functionSignature
     * @param functionCallRewriter functionCallRewriter
     * @return return true if a functionSignature does not exist, else return false
     */
    private final boolean addUserDefineFunctionSupport(String functionSignature, FunctionCallRewriter functionCallRewriter)
    {
        if (this.rewriteFunctionMap.containsKey(functionSignature)) {
            return false;
        }
        rewriteFunctionMap.put(functionSignature, functionCallRewriter);
        return true;
    }

    /**
     * add Config Function Re-writer, it will refresh the instance
     */
    private final void addConfigFunctionRewriter(DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter)
    {
        requireNonNull(connectorConfigFunctionRewriter);
        this.connectorConfigFunctionRewriter = connectorConfigFunctionRewriter;
    }

    /**
     * Get the function rewrite name if not support, throw UnsupportedOperationException
     *
     * @param name QualifiedName
     * @param isDistinct isDistinct
     * @param argumentsList argumentsList
     * @param orderBy order by
     * @param filter filter
     * @param window windows
     * @return String rewrite result, it can be empty in an optional Object for not support function
     */
    // CHECKSTYLE:OFF:ParameterNumber
    // inherit api(io.prestosql.spi.sql.SqlQueryWriter.functionCall), can't change it
    public final String getFunctionRewriteResult(QualifiedName name, boolean isDistinct, List<String> argumentsList, Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        // CHECKSTYLE:ON:ParameterNumber
        FunctionCallArgsPackage functionCallArgsPackage = new FunctionCallArgsPackage(name, isDistinct, argumentsList, orderBy, filter, window);
        FunctionCallRewriter functionCallRewriter;
        String functionSignature = this.argsFunction.apply(functionCallArgsPackage);
        functionCallRewriter = rewriteFunctionMap.getOrDefault(functionSignature, null);
        Optional<String> result = Optional.empty();
        if (functionCallRewriter != null) {
            result = Optional.ofNullable(functionCallRewriter.rewriteFunctionCall(functionCallArgsPackage));
        }
        if (!result.isPresent()) {
            // can not find function signature in the rewriteFunctionMap
            // find it in the config config udf
            // connector config
            if (connectorConfigFunctionRewriter != null) {
                result = Optional.ofNullable(connectorConfigFunctionRewriter.rewriteFunctionCall(functionCallArgsPackage));
            }
        }
        if (result.isPresent()) {
            return result.get();
        }
        else {
            String exceptionStr = connectorName + " Connector does not support function call of " + FunctionCallArgsPackage.defaultBuildFunctionSignature(functionCallArgsPackage);
            throw new UnsupportedOperationException(exceptionStr);
        }
    }

    public String getVersionName()
    {
        return versionName;
    }
}
