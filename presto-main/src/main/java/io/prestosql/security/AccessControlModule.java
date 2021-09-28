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
package io.prestosql.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.spi.security.GroupProvider;

import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AccessControlModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(AccessControlManager.class).in(Scopes.SINGLETON);
        binder.bind(GroupProviderManager.class).in(Scopes.SINGLETON);
        binder.bind(GroupProvider.class).to(GroupProviderManager.class).in(Scopes.SINGLETON);
        binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(AccessControlManager.class).withGeneratedName();
    }
}
