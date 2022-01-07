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
package io.prestosql.queryhistory;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.queryhistory.collectionsql.CollectionSqlResource;
import io.prestosql.queryhistory.collectionsql.CollectionSqlService;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class QueryHistoryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jaxrsBinder(binder).bind(CollectionSqlResource.class);
        binder.bind(CollectionSqlService.class).in(Scopes.SINGLETON);
        binder.bind(QueryHistoryService.class).in(Scopes.SINGLETON);
    }
}
