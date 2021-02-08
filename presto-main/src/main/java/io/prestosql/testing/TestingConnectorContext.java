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
package io.prestosql.testing;

import io.prestosql.GroupByHashPageIndexerFactory;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.connector.ConnectorAwareNodeManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.PagesIndex;
import io.prestosql.server.ServerConfig;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.relational.ConnectorRowExpressionService;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import io.prestosql.type.InternalTypeManager;
import io.prestosql.version.EmbedVersion;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;

public class TestingConnectorContext
        implements ConnectorContext
{
    private final NodeManager nodeManager = new ConnectorAwareNodeManager(new InMemoryNodeManager(), "testenv", new CatalogName("test"));
    private final VersionEmbedder versionEmbedder = new EmbedVersion(new ServerConfig());
    private final TypeManager typeManager;
    private final PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
    private final PageIndexerFactory pageIndexerFactory;
    private final IndexClient indexClient = new NoOpIndexClient();
    private final RowExpressionService rowExpressionService;

    public TestingConnectorContext()
    {
        Metadata metadata = createTestMetadataManager();
        pageIndexerFactory = new GroupByHashPageIndexerFactory(new JoinCompiler(metadata));
        typeManager = new InternalTypeManager(metadata);
        rowExpressionService = new ConnectorRowExpressionService(new RowExpressionDomainTranslator(metadata), new RowExpressionDeterminismEvaluator(metadata));
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public VersionEmbedder getVersionEmbedder()
    {
        return versionEmbedder;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return pageIndexerFactory;
    }

    @Override
    public IndexClient getIndexClient()
    {
        return indexClient;
    }

    @Override
    public RowExpressionService getRowExpressionService()
    {
        return rowExpressionService;
    }
}
