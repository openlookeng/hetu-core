/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.resourcegroups.NoOpResourceGroupManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropCache;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingSession.createBogusTestingCatalog;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestDropCacheTask
{
    private static final String CATALOG_NAME = "catalog";
    private static String schema = "test_schema";
    private static String table = "test_table";
    private static String table2 = "test_table2";
    private static String table3 = "test_table3";

    private final Metadata metadata = createTestMetadataManager();
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));
    private TransactionManager transactionManager;
    private QueryStateMachine stateMachine;

    private Session testSession;

    @BeforeMethod
    public void setUp()
    {
        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, true);
        SplitCacheMap splitCacheMap = createNew();
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        TablePropertyManager tablePropertyManager = new TablePropertyManager();
        ColumnPropertyManager columnPropertyManager = new ColumnPropertyManager();
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        tablePropertyManager.addProperties(
                testCatalog.getConnectorCatalogName(),
                ImmutableList.of(stringProperty("baz", "test property", null, false)));
        columnPropertyManager.addProperties(testCatalog.getConnectorCatalogName(), ImmutableList.of());
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();

        ColumnMetadata columnMetadataA = new ColumnMetadata("a", BIGINT);
        TupleDomain tupleDomainA = TupleDomain.withColumnDomains(
                ImmutableMap.of(columnMetadataA, Domain.singleValue(BIGINT, 23L)));

        ColumnMetadata columnMetadataB = new ColumnMetadata("b", BIGINT);
        TupleDomain tupleDomainB = TupleDomain.withColumnDomains(
                ImmutableMap.of(columnMetadataB, Domain.singleValue(BIGINT, 88L)));
        ColumnMetadata columnMetadataC = new ColumnMetadata("c", BIGINT);
        TupleDomain tupleDomainC = TupleDomain.withColumnDomains(
                ImmutableMap.of(columnMetadataC, Domain.singleValue(BIGINT, 66L)));

        // Adding entries into SplitCacheMap
        SplitCacheMap.getInstance().addCache(
                QualifiedName.of(CATALOG_NAME, schema, table),
                tupleDomainA,
                "a = 23");
        SplitCacheMap.getInstance().addCache(
                QualifiedName.of(CATALOG_NAME, schema, table2),
                tupleDomainB,
                "b = 88");
        SplitCacheMap.getInstance().addCache(
                QualifiedName.of(CATALOG_NAME, schema, table3),
                tupleDomainC,
                "b = 66");
        stateMachine = createQueryStateMachine("START TRANSACTION", testSession, transactionManager);
    }

    private SplitCacheMap createNew()
    {
        //way to hack around singleton object - Only intended for tests
        try {
            SplitCacheMap instance = SplitCacheMap.getInstance();
            Field field = instance.getClass().getDeclaredField("splitCacheMap");
            field.setAccessible(true);
            field.set(null, null);
            return instance;
        }
        catch (Exception e) {
            throw new IllegalStateException("Singleton creation failed!");
        }
    }

    @Test
    public void testGetName()
    {
        DropCacheTask task = new DropCacheTask();
        assertEquals("DROP CACHE", task.getName());
    }

    @Test
    public void testDropCacheNotExistsTrue()
    {
        QualifiedName tableName = QualifiedName.of(CATALOG_NAME, schema, table);
        DropCache statement = new DropCache(tableName, true);
        getFutureValue(new DropCacheTask().execute(statement, createTestTransactionManager(), metadata, new AllowAllAccessControl(), stateMachine, Collections.emptyList(), new HeuristicIndexerManager(new FileSystemClientManager(), new HetuMetaStoreManager())));
        assertFalse(SplitCacheMap.getInstance().cacheExists(tableName));

        QualifiedName table2Name = QualifiedName.of(CATALOG_NAME, schema, table2);
        DropCache statement2 = new DropCache(table2Name, true);
        assertTrue(SplitCacheMap.getInstance().cacheExists(table2Name));

        getFutureValue(new DropCacheTask().execute(statement2, createTestTransactionManager(), metadata, new AllowAllAccessControl(), stateMachine, Collections.emptyList(), new HeuristicIndexerManager(new FileSystemClientManager(), new HetuMetaStoreManager())));
        assertFalse(SplitCacheMap.getInstance().cacheExists(table2Name));
    }

    @Test
    public void testDropCacheNotExistsFalse()
    {
        DropCache statement = new DropCache(QualifiedName.of("test_nonexistent_table"), false);
        QueryStateMachine queryStateMachine = createQueryStateMachine("START TRANSACTION", testSession, transactionManager);

        try {
            getFutureValue(new DropCacheTask().execute(statement, createTestTransactionManager(), metadata, new AllowAllAccessControl(), queryStateMachine, Collections.emptyList(), new HeuristicIndexerManager(new FileSystemClientManager(), new HetuMetaStoreManager())));
            fail("expected exception");
        }
        catch (RuntimeException e) {
            // Expected
            assertTrue(e instanceof SemanticException);
            SemanticException semanticException = (SemanticException) e;
            assertEquals(semanticException.getCode(), SemanticErrorCode.MISSING_CACHE);
        }
    }

    @Test
    public void testDropWithNonQualifiedName()
    {
        QualifiedName tableName = QualifiedName.of(table3);
        QualifiedName fullName = QualifiedName.of(CATALOG_NAME, schema, table3);

        DropCache statement = new DropCache(tableName, true);

        Session session = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .setCatalog(CATALOG_NAME)
                .setSchema(schema)
                .build();
        QueryStateMachine queryStateMachine = createQueryStateMachine("START TRANSACTION", session, transactionManager);

        getFutureValue(new DropCacheTask().execute(statement, createTestTransactionManager(), metadata, new AllowAllAccessControl(), queryStateMachine, Collections.emptyList(), new HeuristicIndexerManager(new FileSystemClientManager(), new HetuMetaStoreManager())));
        assertFalse(SplitCacheMap.getInstance().cacheExists(fullName));
    }

    private QueryStateMachine createQueryStateMachine(String query, Session session, TransactionManager transactionManager)
    {
        return QueryStateMachine.begin(
                query,
                Optional.empty(),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                new NoOpResourceGroupManager(),
                true,
                transactionManager,
                new AccessControlManager(transactionManager),
                executor,
                metadata,
                WarningCollector.NOOP);
    }
}
