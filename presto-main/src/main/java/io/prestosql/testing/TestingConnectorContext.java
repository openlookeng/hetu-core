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
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.PagesIndex;
import io.prestosql.server.ServerConfig;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.function.ExternalFunctionHub;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.relational.ConnectorRowExpressionService;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import io.prestosql.type.InternalTypeManager;
import io.prestosql.version.EmbedVersion;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

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
    private final FunctionMetadataManager functionMetadataManager;
    public final StandardFunctionResolution standardFunctionResolution;
    private final Optional<BiFunction<ExternalFunctionHub, CatalogSchemaName, Set<SqlInvokedFunction>>> externalFunctionParser;

    public TestingConnectorContext()
    {
        Metadata metadata = createTestMetadataManager();
        pageIndexerFactory = new GroupByHashPageIndexerFactory(new JoinCompiler(metadata));
        rowExpressionService = new ConnectorRowExpressionService(new RowExpressionDomainTranslator(metadata), new RowExpressionDeterminismEvaluator(metadata));
        typeManager = new InternalTypeManager(metadata.getFunctionAndTypeManager());
        FunctionAndTypeManager functionAndTypeManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        functionMetadataManager = functionAndTypeManager;
        this.standardFunctionResolution = new FunctionResolution(functionAndTypeManager);
        externalFunctionParser = Optional.empty();
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

    @Override
    public FunctionMetadataManager getFunctionMetadataManager()
    {
        return functionMetadataManager;
    }

    @Override
    public StandardFunctionResolution getStandardFunctionResolution()
    {
        return this.standardFunctionResolution;
    }

    @Override
    public Optional<BiFunction<ExternalFunctionHub, CatalogSchemaName, Set<SqlInvokedFunction>>> getExternalParserFunction()
    {
        return externalFunctionParser;
    }
}
