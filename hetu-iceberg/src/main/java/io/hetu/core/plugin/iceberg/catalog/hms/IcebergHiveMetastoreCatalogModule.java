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
package io.hetu.core.plugin.iceberg.catalog.hms;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.hetu.core.plugin.iceberg.catalog.IcebergCatalogModule.MetastoreValidator;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.prestosql.plugin.hive.metastore.DecoratedHiveMetastoreModule;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreModule;

public class IcebergHiveMetastoreCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new ThriftMetastoreModule());
        binder.bind(IcebergTableOperationsProvider.class).to(HiveMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoHiveCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(MetastoreValidator.class).asEagerSingleton();
        install(new DecoratedHiveMetastoreModule());
    }
}
