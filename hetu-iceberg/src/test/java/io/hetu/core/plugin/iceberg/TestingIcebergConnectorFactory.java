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
package io.hetu.core.plugin.iceberg;

import com.google.inject.Module;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;
import java.util.Optional;

import static io.hetu.core.plugin.iceberg.InternalIcebergConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingIcebergConnectorFactory
        implements ConnectorFactory
{
    private final Optional<HiveMetastore> metastore;
    private final Optional<FileIoProvider> fileIoProvider;
    private final Module module;

    public TestingIcebergConnectorFactory(Optional<HiveMetastore> metastore, Optional<FileIoProvider> fileIoProvider, Module module)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public String getName()
    {
        return "iceberg";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return null;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, module, metastore, fileIoProvider);
    }
}
