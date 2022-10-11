/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SingleDataConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "singledata";

    private final Module module;
    private final ClassLoader classLoader;

    public SingleDataConnectorFactory(Module module, ClassLoader classLoader)
    {
        this.module = requireNonNull(module, "module is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new SingleDataHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                        binder.bind(DeterminismEvaluator.class).toInstance(context.getRowExpressionService().getDeterminismEvaluator());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(ClassLoader.class).toInstance(classLoader);
                    },
                    new MBeanServerModule(),
                    new MBeanModule(),
                    module);

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .quiet()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(SingleDataConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
