/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.sql.rewrite;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.connector.informationschema.InformationSchemaConnector;
import io.prestosql.connector.system.SystemConnector;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlManager;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.sql.SqlPath;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.Statement;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.prestosql.spi.connector.CatalogName.createInformationSchemaCatalogName;
import static io.prestosql.spi.connector.CatalogName.createSystemTablesCatalogName;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.SemanticErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestRowFilterColumnMaskingRewrite
{
    private static final String TPCH_CATALOG = "tpch";
    private static final CatalogName TPCH_CATALOG_NAME = new CatalogName(TPCH_CATALOG);
    private static final String SECOND_CATALOG = "c2";
    private static final CatalogName SECOND_CATALOG_NAME = new CatalogName(SECOND_CATALOG);
    private static final String THIRD_CATALOG = "c3";
    private static final CatalogName THIRD_CATALOG_NAME = new CatalogName(THIRD_CATALOG);
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();
    private static final Session CLIENT_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();

    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;

    @Test
    public void testRowFilter()
    {
        // test table tpch.s1.t1 with a rowfilter: a > 1, without column masking
        analyze("SELECT * FROM tpch.s1.t1");
        // test table tpch.s1.t2 without a rowfilter, without column masking
        analyze("SELECT * FROM tpch.s1.t2");
    }

    @Test
    public void testColumnMasking()
    {
        // test table tpch.s1.t3 with column masking: column c :mask all , column c is of String type, without rowfilter
        analyze("SELECT * FROM tpch.s1.t3");

        // test table tpch.s1.t4 with column masking: column a :mask all , column a is NOT of String type, without rowfilter
        assertFails(FUNCTION_NOT_FOUND, "SELECT * FROM tpch.s1.t4");
    }

    @Test
    public void testRowFilterAndColumnMasking()
    {
        // test table tpch.s1.t5 with a rowfilter: a=1, column masking: column b :partial mask
        analyze("SELECT * FROM tpch.s1.t5");
    }

    @Test void testRowFilterAndColumnMaskingWithQuery()
    {
        Session.SessionBuilder sessionBuilder = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryIdGenerator().createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource("test")
                .setPath(new SqlPath(Optional.of("path")))
                .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent");
        analyze(sessionBuilder.build(), "WITH\n" +
                "  customer_total_return AS (\n" +
                "   SELECT \"a\" FROM tpch.s1.t1\n" +
                ")\n" +
                "SELECT \"a\"\n" +
                "FROM\n" +
                "  customer_total_return ctr1\n" +
                "WHERE (\"ctr1\".\"a\" > 1000)\n" +
                "LIMIT 100");
    }

    private void analyze(@Language("SQL") String query)
    {
        analyze(CLIENT_SESSION, query);
    }

    private void analyze(Session clientSession, @Language("SQL") String query)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(clientSession, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata);
                    Statement statement = SQL_PARSER.createStatement(query);
                    analyzer.analyze(statement);
                });
    }

    private static Analyzer createAnalyzer(Session session, Metadata metadata)
    {
        return new Analyzer(
                session,
                metadata,
                SQL_PARSER,
                new TestingRangerAccessControl(),
                Optional.empty(),
                emptyList(),
                WarningCollector.NOOP);
    }

    @BeforeClass
    public void setup()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager);

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        metadata.addFunctions(ImmutableList.of(APPLY_FUNCTION));

        Catalog tpchTestCatalog = createTestingCatalog(TPCH_CATALOG, TPCH_CATALOG_NAME);
        catalogManager.registerCatalog(tpchTestCatalog);
        metadata.getAnalyzePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getAnalyzeProperties());

        catalogManager.registerCatalog(createTestingCatalog(SECOND_CATALOG, SECOND_CATALOG_NAME));
        catalogManager.registerCatalog(createTestingCatalog(THIRD_CATALOG, THIRD_CATALOG_NAME));

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table1, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT))),
                false));

        SchemaTableName table2 = new SchemaTableName("s1", "t2");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table2, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT))),
                false));

        SchemaTableName table3 = new SchemaTableName("s1", "t3");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table3, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("c", VARCHAR),
                        new ColumnMetadata("x", BIGINT, null, true))),
                false));

        SchemaTableName table4 = new SchemaTableName("s1", "t4");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table4, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", VARCHAR))),
                false));

        SchemaTableName table5 = new SchemaTableName("s1", "t5");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table5, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", VARCHAR))),
                false));
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    private Catalog createTestingCatalog(String catalogName, CatalogName catalog)
    {
        CatalogName systemId = createSystemTablesCatalogName(catalog);
        Connector connector = createTestingConnector();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalog)));
    }

    private static Connector createTestingConnector()
    {
        return new Connector()
        {
            private final ConnectorMetadata metadata = new TestingMetadata();

            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return metadata;
            }

            @Override
            public List<PropertyMetadata<?>> getAnalyzeProperties()
            {
                return ImmutableList.of(
                        stringProperty("p1", "test string property", "", false),
                        integerProperty("p2", "test integer property", 0, false));
            }
        };
    }

    private void assertFails(SemanticErrorCode error, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, query);
    }

    private void assertFails(Session session, SemanticErrorCode error, @Language("SQL") String query)
    {
        assertFails(session, error, Optional.empty(), query);
    }

    private void assertFails(Session session, SemanticErrorCode error, Optional<NodeLocation> location, @Language("SQL") String query)
    {
        try {
            analyze(session, query);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }

            if (location.isPresent()) {
                NodeLocation expected = location.get();
                NodeLocation actual = e.getNode().getLocation().get();

                if (expected.getLineNumber() != actual.getLineNumber() || expected.getColumnNumber() != actual.getColumnNumber()) {
                    fail(format(
                            "Expected error '%s' to occur at line %s, offset %s, but was: line %s, offset %s",
                            e.getCode(),
                            expected.getLineNumber(),
                            expected.getColumnNumber(),
                            actual.getLineNumber(),
                            actual.getColumnNumber()));
                }
            }
        }
    }
}
