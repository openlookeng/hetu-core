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
package io.hetu.core.plugin.functionnamespace.memory;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.spi.function.FunctionHandleResolver;
import io.prestosql.spi.function.FunctionNamespaceManager;
import io.prestosql.spi.function.FunctionNamespaceManagerContext;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;
import io.prestosql.spi.function.SqlFunctionHandle;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class InMemoryFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
    public static final String NAME = "memory";

    private static final SqlFunctionHandle.Resolver HANDLE_RESOLVER = new SqlFunctionHandle.Resolver();

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public FunctionHandleResolver getHandleResolver()
    {
        return HANDLE_RESOLVER;
    }

    @Override
    public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config, FunctionNamespaceManagerContext functionNamespaceManagerContext)
    {
        try {
            Bootstrap app = new Bootstrap(new InMemoryFunctionNamespaceManagerModule(catalogName));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            InMemoryFunctionNamespaceManager inMemoryFunctionNamespaceManager = injector.getInstance(InMemoryFunctionNamespaceManager.class);
            inMemoryFunctionNamespaceManager.setFunctionNamespaceManagerContext(functionNamespaceManagerContext);
            return inMemoryFunctionNamespaceManager;
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e.getMessage());
        }
    }
}
