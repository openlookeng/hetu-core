/*
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
package io.prestosql.plugin.memory;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class MemoryConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "memory";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new MemoryHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        if (context.getHetuMetastore() == null) {
            throw new IllegalStateException("HetuMetastore must be configured to use the Memory Connector. Please refer to HetuMetastore docs for details.");
        }
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    binder -> {
                        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                        binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                        binder.bind(DeterminismEvaluator.class).toInstance(context.getRowExpressionService().getDeterminismEvaluator());
                        binder.bind(HetuMetastore.class).toInstance(context.getHetuMetastore());
                    },
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new JsonModule(),
                    new MemoryModule(context.getTypeManager(),
                            context.getNodeManager(),
                            new PagesSerdeFactory(context.getBlockEncodingSerde(), false).createPagesSerde()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            synchronized (MemoryThreadManager.class) {
                if (!MemoryThreadManager.isSharedThreadPoolInitilized()) {
                    MemoryThreadManager.initSharedThreadPool(injector.getInstance(MemoryConfig.class).getThreadPoolSize());
                }
            }

            MemoryConnector memoryConnector = injector.getInstance(MemoryConnector.class);
            memoryConnector.scheduleRefreshJob();

            return memoryConnector;
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
