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
package io.hetu.core.plugin.functionnamespace;

import com.google.common.collect.ImmutableList;
import io.hetu.core.plugin.functionnamespace.memory.InMemoryFunctionNamespaceManagerFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;

public class FunctionNamespaceManagerPlugin
        implements Plugin
{
    @Override
    public Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
    {
        // add function namespace factory here
        return ImmutableList.of(new InMemoryFunctionNamespaceManagerFactory());
    }
}
