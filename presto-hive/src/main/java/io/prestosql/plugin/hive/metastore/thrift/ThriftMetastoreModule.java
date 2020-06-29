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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.ForCachingHiveMetastore;
import io.prestosql.plugin.hive.ForRecordingHiveMetastore;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.MetastoreClientFactory;
import io.prestosql.plugin.hive.metastore.MetastoreConfig;
import io.prestosql.plugin.hive.metastore.RecordingHiveMetastore;
import io.prestosql.plugin.hive.metastore.WriteHiveMetastoreRecordingProcedure;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ThriftMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(ThriftMetastoreModule.class);

    @Override
    protected void setup(Binder binder)
    {
        MetastoreConfig config = this.buildConfigObject(MetastoreConfig.class);
        try {
            // bind MetastoreClientFactory
            if (config.getMetastoreClientFactoryImp().isEmpty()) {
                log.info("Binding default implementation of MetastoreClientFactory.");
                binder.bind(ThriftMetastoreClientFactory.class).in(Scopes.SINGLETON);
                binder.bind(MetastoreClientFactory.class).to(ThriftMetastoreClientFactory.class).in(Scopes.SINGLETON);
            }
            else {
                log.info("Binding MetastoreClientFactory.class to %s", config.getMetastoreClientFactoryImp().trim());
                binder.bind(MetastoreClientFactory.class)
                        .to((Class<? extends MetastoreClientFactory>) Class.forName(config.getMetastoreClientFactoryImp().trim()))
                        .in(Scopes.SINGLETON);
            }

            // bind MetastoreLocator
            binder.bind(MetastoreLocator.class).to(StaticMetastoreLocator.class).in(Scopes.SINGLETON);

            // bind ThriftMetastore
            if (config.getThriftMetastoreImp().isEmpty()) {
                log.info("Binding default implementation of ThriftMetastore.");
                binder.bind(ThriftMetastore.class).to(ThriftHiveMetastore.class).in(Scopes.SINGLETON);
            }
            else {
                log.info("Binding ThriftMetastore.class to %s", config.getThriftMetastoreImp().trim());
                binder.bind(ThriftMetastore.class)
                        .to((Class<? extends ThriftMetastore>) Class.forName(config.getThriftMetastoreImp().trim()))
                        .in(Scopes.SINGLETON);
            }
        }
        catch (ClassNotFoundException e) {
            log.error("Failed to bind classes which specified in MetaStore configuration. error: %s", e.getLocalizedMessage());
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Failed to bind classes which specified in MetaStore configuration");
        }

        configBinder(binder).bindConfig(ThriftHiveMetastoreConfig.class);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);

        if (buildConfigObject(HiveConfig.class).getRecordingPath() != null) {
            binder.bind(HiveMetastore.class)
                    .annotatedWith(ForRecordingHiveMetastore.class)
                    .to(BridgingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
            binder.bind(HiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(RecordingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
            binder.bind(RecordingHiveMetastore.class).in(Scopes.SINGLETON);
            newExporter(binder).export(RecordingHiveMetastore.class).withGeneratedName();

            Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
            procedures.addBinding().toProvider(WriteHiveMetastoreRecordingProcedure.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(HiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(BridgingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
        }

        binder.bind(HiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ThriftMetastore.class)
                .as(generator -> generator.generatedNameOf(ThriftHiveMetastore.class));
        newExporter(binder).export(HiveMetastore.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));
    }
}
