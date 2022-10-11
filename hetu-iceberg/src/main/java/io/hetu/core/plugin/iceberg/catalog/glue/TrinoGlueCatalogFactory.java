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
package io.hetu.core.plugin.iceberg.catalog.glue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.glue.AWSGlueAsync;
import io.hetu.core.plugin.iceberg.IcebergConfig;
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.prestosql.spi.security.ConnectorIdentity;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.Optional;

import static io.prestosql.plugin.hive.metastore.glue.GlueHiveMetastore.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class TrinoGlueCatalogFactory
        implements TrinoCatalogFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final Optional<String> defaultSchemaLocation;
    private final AWSGlueAsync glueClient;
    private final boolean isUniqueTableLocation;
    private final GlueMetastoreStats stats;

    @Inject
    public TrinoGlueCatalogFactory(
            HdfsEnvironment hdfsEnvironment,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            GlueHiveMetastoreConfig glueConfig,
            AWSCredentialsProvider credentialsProvider,
            IcebergConfig icebergConfig,
            GlueMetastoreStats stats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(glueConfig, "glueConfig is null");
        this.defaultSchemaLocation = glueConfig.getDefaultWarehouseDir();
        requireNonNull(credentialsProvider, "credentialsProvider is null");
        this.glueClient = createAsyncGlueClient(glueConfig, credentialsProvider, Optional.empty(), stats.newRequestMetricsCollector());
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoGlueCatalog(hdfsEnvironment, tableOperationsProvider, trinoVersion, glueClient, stats, defaultSchemaLocation, isUniqueTableLocation);
    }
}
