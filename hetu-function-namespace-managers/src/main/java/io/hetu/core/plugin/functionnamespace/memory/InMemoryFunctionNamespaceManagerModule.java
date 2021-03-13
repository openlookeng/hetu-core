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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.hetu.core.plugin.functionnamespace.ServingCatalog;
import io.hetu.core.plugin.functionnamespace.SqlInvokedFunctionNamespaceManagerConfig;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class InMemoryFunctionNamespaceManagerModule
        implements Module
{
    private final String catalogName;

    public InMemoryFunctionNamespaceManagerModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(new TypeLiteral<String>() {}).annotatedWith(ServingCatalog.class).toInstance(catalogName);
        configBinder(binder).bindConfig(SqlInvokedFunctionNamespaceManagerConfig.class);
        binder.bind(InMemoryFunctionNamespaceManager.class).in(SINGLETON);
    }
}
