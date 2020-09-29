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

import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;
import io.prestosql.sql.builder.functioncall.functions.config.DefaultConnectorConfigFunctionRewriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Function Writer Manager Factory
 * use the version name of a specific connector is the specific identity of a FunctionWriterManager instance
 *
 * @since 2019-12-30
 */
public class FunctionWriterManagerGroup
{
    private static final Map<String, FunctionWriterManager> factoryInstances = new HashMap<>(Collections.emptyMap());

    private FunctionWriterManagerGroup()
    {
    }

    /**
     * get a Function Writer Manager Instance, every version of a connector can have only one FunctionWriterManager instance
     *
     * @param connectorName connector name
     * @param version the data source's sql version
     */
    public static Optional<FunctionWriterManager> getFunctionWriterManagerInstance(String connectorName, String version)
    {
        synchronized (FunctionWriterManagerGroup.class) {
            if (factoryInstances.containsKey(version)) {
                return Optional.of(factoryInstances.get(version));
            }
            else {
                return Optional.empty();
            }
        }
    }

    /**
     * New a Function Writer Manager Instance, every version of a connector can have only one FunctionWriterManager instance
     *
     * @param connectorName connector name
     * @param version the data source's sql version
     * @param functionCallRewriterMap functionCallRewriterMap
     * @param connectorConfigFunctionRewriter Connector Config Function Re-writer
     * @return FunctionWriterManager Instance
     */
    public static FunctionWriterManager newFunctionWriterManagerInstance(String connectorName, String version, Map<String, FunctionCallRewriter> functionCallRewriterMap, DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter)
    {
        requireNonNull(connectorName);
        requireNonNull(version);
        requireNonNull(connectorConfigFunctionRewriter);
        FunctionWriterManager instance = factoryInstances.get(version);
        if (instance == null) {
            synchronized (FunctionWriterManagerGroup.class) {
                if (!factoryInstances.containsKey(version)) {
                    instance = new FunctionWriterManager(connectorName, version, functionCallRewriterMap, connectorConfigFunctionRewriter);
                    factoryInstances.put(version, instance);
                }
                else {
                    instance = factoryInstances.get(version);
                }
            }
        }
        return instance;
    }
}
