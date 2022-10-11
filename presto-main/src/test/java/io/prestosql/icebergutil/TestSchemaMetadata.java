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
package io.prestosql.icebergutil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.client.ClientCapabilities;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.testing.TestingConnectorContext;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.connector.CatalogName.createInformationSchemaCatalogName;
import static io.prestosql.spi.connector.CatalogName.createSystemTablesCatalogName;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingSession.TESTING_CATALOG;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Arrays.stream;

public class TestSchemaMetadata
{
    public TransactionManager transactionManager;
    public Metadata metadata;

    public TestSchemaMetadata()
    {
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        MockConnectorFactory mockConnectorFactory = builder.withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .withListTables((connectorSession, schemaNameOrNull) ->
                        ImmutableList.of(
                                new SchemaTableName("test_schema", "test_view"),
                                new SchemaTableName("test_schema", "another_table")))
                .withGetViews((connectorSession, prefix) -> {
                    ConnectorViewDefinition definition = new ConnectorViewDefinition(
                            "select 1",
                            Optional.of("test_catalog"),
                            Optional.of("test_schema"),
                            ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test", BIGINT.getTypeSignature())),
                            Optional.empty(),
                            true);
                    SchemaTableName viewName = new SchemaTableName("test_schema", "test_view");
                    return ImmutableMap.of(viewName, definition);
                }).build();
        Connector testConnector = mockConnectorFactory.create("test", ImmutableMap.of(), new TestingConnectorContext());
        CatalogManager catalogManager = new CatalogManager();
        String catalogName = "test_catalog";
        CatalogName catalog = new CatalogName("test_catalog");
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                catalog,
                testConnector,
                createInformationSchemaCatalogName(catalog),
                testConnector,
                createSystemTablesCatalogName(catalog),
                testConnector));
        transactionManager = createTestTransactionManager(catalogManager);
        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
    }

    public static ConnectorSession createNewSession(TransactionId transactionId)
    {
        return testSessionBuilder()
                .setCatalog("test_catalog")
                .setSchema("test_schema")
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .setTransactionId(transactionId)
                .build()
                .toConnectorSession();
    }

    public static ConnectorSession createNewSession2(TransactionId transactionId)
    {
        return testSessionBuilder()
                .setCatalog("test_catalog")
                .setSchema("test_schema")
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .setTransactionId(transactionId)
                .build()
                .toConnectorSession(new CatalogName("test_catalog"));
    }

    public static ConnectorSession createNewSession3(Session session, SessionPropertyManager sessionPropertyManager, Map<CatalogName, Map<String, String>> map, TransactionId transactionId)
    {
        Session session1 = new Session(
                session.getQueryId(),
                Optional.empty(),
                session.isClientTransactionSupport(),
                session.getIdentity(),
                session.getSource(),
                session.getCatalog(),
                session.getSchema(),
                session.getPath(),
                session.getTraceToken(),
                session.getTimeZoneKey(),
                session.getLocale(),
                session.getRemoteUserAddress(),
                session.getUserAgent(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getClientCapabilities(),
                session.getResourceEstimates(),
                session.getStartTime(),
                ImmutableMap.<String, String>builder()
                        .put("test_string", "foo string")
                        .put("test_long", "424242")
                        .build(),
                map,
                ImmutableMap.of(TESTING_CATALOG, ImmutableMap.<String, String>builder()
                        .put("connector_string", "bar string")
                        .put("connector_long", "11")
                        .build()),
                sessionPropertyManager,
                session.getPreparedStatements(),
                session.isPageMetadataEnabled());
        return session1.toConnectorSession(new CatalogName("test_catalog"));
    }
}
