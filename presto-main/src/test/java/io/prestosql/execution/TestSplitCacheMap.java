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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.MockSplit;
import io.prestosql.client.NodeVersion;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.TestingBlockEncodingSerde;
import io.prestosql.spi.block.TestingBlockJsonSerde;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.type.TestingTypeDeserializer;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestSplitCacheMap
{
    private ObjectMapper mapper;

    private final TestingTypeManager typeManager = new TestingTypeManager();
    private final TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    private final InternalNode workerNode = new InternalNode("838986e0-7484-4bf6-87b9-7dd0af6bf901", URI.create("http://worker1:8080"), NodeVersion.UNKNOWN, false);
    private final InternalNode workerNode2 = new InternalNode("838986e0-7484-4bf6-87b9-7dd0af6bf902", URI.create("http://worker2:8080"), NodeVersion.UNKNOWN, false);

    private final ColumnMetadata columnMetadataA = new ColumnMetadata("a", BIGINT);
    private final TupleDomain<ColumnMetadata> tupleDomainA = TupleDomain.withColumnDomains(
            ImmutableMap.of(columnMetadataA, Domain.singleValue(BIGINT, 23L)));
    private final String tupleDomainAPredicateString = "a = 23";

    private final ColumnMetadata columnMetadataB = new ColumnMetadata("b", BIGINT);
    private final TupleDomain<ColumnMetadata> tupleDomainB = TupleDomain.withColumnDomains(
            ImmutableMap.of(columnMetadataB, Domain.singleValue(BIGINT, 88L)));
    private final String tupleDomainBPredicateString = "b = 88";

    private final CatalogName catalogName = new CatalogName("bogus_catalog");
    private final String table1Schema = "test_schema_1";
    private final String table1Name = "test_table_1";
    private final QualifiedName table1QN = QualifiedName.of(catalogName.getCatalogName(), table1Schema, table1Name);

    private final MockSplit table1ConnSplit1 = new MockSplit("hdfs://hacluster/user/hive/warehouse/test_schema_1.db/test_table_1/a=23/000000_0", 0, 10, System.currentTimeMillis());
    private final Split table1Split1 = new Split(catalogName, table1ConnSplit1, Lifespan.taskWide());
    private final SplitKey table1SplitKey1 = new SplitKey(table1Split1, catalogName.getCatalogName(), table1Schema, table1Name);

    private final MockSplit table1ConnSplit2 = new MockSplit("hdfs://hacluster/user/hive/warehouse/test_schema_1.db/test_table_1/b=88/000010_0", 0, 20, System.currentTimeMillis());
    private final Split table1Split2 = new Split(catalogName, table1ConnSplit2, Lifespan.taskWide());
    private final SplitKey table1SplitKey2 = new SplitKey(table1Split2, catalogName.getCatalogName(), table1Schema, table1Name);

    private final String table2Schema = "test_schema_2";
    private final String table2Name = "test_table_2";
    private final QualifiedName table2QN = QualifiedName.of(catalogName.getCatalogName(), table2Schema, table2Name);

    private final MockSplit table2ConnSplit1 = new MockSplit("hdfs://hacluster/user/hive/warehouse/test_schema_2.db/test_table_2/a=23/000000_0", 0, 30, System.currentTimeMillis());
    private final Split table2Split1 = new Split(catalogName, table2ConnSplit1, Lifespan.taskWide());
    private final SplitKey table2SplitKey1 = new SplitKey(table2Split1, catalogName.getCatalogName(), table2Schema, table2Name);

    @BeforeTest
    public void setUp()
            throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException
    {
        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, true);
        mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(ColumnHandle.class, new JsonDeserializer<ColumnHandle>()
                        {
                            @Override
                            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                                    throws IOException
                            {
                                return new ObjectMapperProvider().get().readValue(jsonParser, TestingColumnHandle.class);
                            }
                        })
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde))
                        .addKeyDeserializer(SplitKey.class, new SplitKey.KeyDeserializer()));
    }

    private SplitCacheMap createNew()
    {
        //way to hack around singleton object - Only intended for tests
        try {
            return SplitCacheMapSingletonFactory.createInstance();
        }
        catch (Exception e) {
            throw new IllegalStateException("Singleton creation failed!");
        }
    }

    @Test
    public void testCacheExists()
    {
        SplitCacheMap splitCacheMap = createNew();
        assertFalse(splitCacheMap.cacheExists(table1QN));
        assertTrue(splitCacheMap.showCache().isEmpty());

        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        assertTrue(splitCacheMap.cacheExists(table1QN));
        assertEquals(splitCacheMap.showCache().size(), 1);
    }

    @Test
    public void testAddCache()
    {
        SplitCacheMap splitCacheMap = createNew();
        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        assertTrue(splitCacheMap.cacheExists(table1QN));

        assertFalse(splitCacheMap.getCachedNodeId(table1SplitKey1).isPresent());
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 1);
        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCache(table1QN, tupleDomainB, tupleDomainBPredicateString);
        assertFalse(splitCacheMap.getCachedNodeId(table1SplitKey1).isPresent());
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 2);
    }

    @Test
    public void testAddCacheForNullPredicate()
    {
        ColumnMetadata columnMetadataC = new ColumnMetadata("c", BIGINT);
        TupleDomain<ColumnMetadata> tupleDomainC = TupleDomain.withColumnDomains(
                ImmutableMap.of(columnMetadataC, Domain.onlyNull(BIGINT)));
        String tupleDomainCPredicateString = "c IS NULL";

        SplitCacheMap splitCacheMap = createNew();
        splitCacheMap.addCache(table1QN, tupleDomainC, tupleDomainCPredicateString);
        assertTrue(splitCacheMap.cacheExists(table1QN));

        assertFalse(splitCacheMap.getCachedNodeId(table1SplitKey1).isPresent());
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 1);
        splitCacheMap.addCache(table1QN, tupleDomainC, tupleDomainCPredicateString);
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 1);
    }

    @Test
    public void testAddCacheForNotNullPredicate()
    {
        ColumnMetadata columnMetadataC = new ColumnMetadata("c", BIGINT);
        TupleDomain<ColumnMetadata> tupleDomainC = TupleDomain.withColumnDomains(
                ImmutableMap.of(columnMetadataC, Domain.notNull(BIGINT)));
        String tupleDomainCPredicateString = "c IS NOT NULL";

        SplitCacheMap splitCacheMap = createNew();
        splitCacheMap.addCache(table1QN, tupleDomainC, tupleDomainCPredicateString);
        assertTrue(splitCacheMap.cacheExists(table1QN));

        assertFalse(splitCacheMap.getCachedNodeId(table1SplitKey1).isPresent());
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 1);
        splitCacheMap.addCache(table1QN, tupleDomainC, tupleDomainCPredicateString);
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 1);
    }

    @Test
    public void testShowCache()
    {
        SplitCacheMap splitCacheMap = createNew();
        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCachedNode(table1SplitKey1, workerNode.getNodeIdentifier());

        assertTrue(splitCacheMap.showCache("a.b.c").isEmpty());

        Map<String, TableCacheInfo> showCacheResult = splitCacheMap.showCache(table1QN.toString());
        assertEquals(showCacheResult.size(), 1);
        assertTrue(showCacheResult.containsKey(table1QN.toString()));
        assertTrue(showCacheResult.get(table1QN.toString()).showNodes().contains(workerNode.getNodeIdentifier()));

        splitCacheMap.addCache(table2QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCachedNode(table2SplitKey1, workerNode2.getNodeIdentifier());
        showCacheResult = splitCacheMap.showCache();
        assertEquals(showCacheResult.size(), 2);
        assertTrue(showCacheResult.containsKey(table2QN.toString()));
        assertTrue(showCacheResult.get(table2QN.toString()).showNodes().contains(workerNode2.getNodeIdentifier()));
    }

    @Test
    public void testDropCache()
    {
        SplitCacheMap splitCacheMap = createNew();
        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCache(table1QN, tupleDomainB, tupleDomainBPredicateString);
        splitCacheMap.addCache(table2QN, tupleDomainA, tupleDomainAPredicateString);
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 2);
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table2QN).size(), 1);

        splitCacheMap.dropCache(table1QN, Optional.empty());
        assertFalse(splitCacheMap.cacheExists(table1QN));
        assertTrue(splitCacheMap.cacheExists(table2QN));
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table2QN).size(), 1);

        splitCacheMap.dropCache();
        assertFalse(splitCacheMap.cacheExists(table2QN));
    }

    @Test
    public void testDropCacheWithWhere()
    {
        SplitCacheMap splitCacheMap = createNew();
        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCache(table1QN, tupleDomainB, tupleDomainBPredicateString);

        splitCacheMap.dropCache(table1QN, Optional.of(tupleDomainAPredicateString));
        assertEquals(splitCacheMap.getCachePredicateTupleDomains(table1QN).size(), 1);

        splitCacheMap.dropCache(table1QN, Optional.of(tupleDomainBPredicateString));
        assertFalse(splitCacheMap.cacheExists(table1QN));
    }

    @Test
    public void testAddCachedNode()
    {
        SplitCacheMap splitCacheMap = createNew();
        assertFalse(splitCacheMap.getCachedNodeId(table1SplitKey1).isPresent());

        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCachedNode(table1SplitKey1, workerNode.getNodeIdentifier());
        assertTrue(splitCacheMap.cacheExists(table1QN));
        assertEquals(splitCacheMap.getCachedNodeId(table1SplitKey1).get(), workerNode.getNodeIdentifier());
    }

    @Test
    public void testUpdateCachedNode()
    {
        SplitCacheMap splitCacheMap = createNew();
        splitCacheMap.addCachedNode(table1SplitKey1, workerNode.getNodeIdentifier());
        assertFalse(splitCacheMap.cacheExists(table1QN));

        splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCachedNode(table1SplitKey1, workerNode.getNodeIdentifier());
        assertTrue(splitCacheMap.cacheExists(table1QN));
        assertEquals(splitCacheMap.getCachedNodeId(table1SplitKey1).get(), workerNode.getNodeIdentifier());

        splitCacheMap.addCachedNode(table1SplitKey1, workerNode2.getNodeIdentifier());
        assertEquals(splitCacheMap.getCachedNodeId(table1SplitKey1).get(), workerNode2.getNodeIdentifier());
    }

    @Test
    public void testSerde()
            throws IOException
    {
        TableCacheInfo table1CacheInfo = new TableCacheInfo(table1QN.toString());
        table1CacheInfo.addCachedPredicate(new CachePredicate(tupleDomainA, tupleDomainAPredicateString));
        table1CacheInfo.addCachedPredicate(new CachePredicate(tupleDomainB, tupleDomainBPredicateString));
        table1CacheInfo.setCachedNode(table1SplitKey1, workerNode.getNodeIdentifier());
        table1CacheInfo.setCachedNode(table1SplitKey2, workerNode2.getNodeIdentifier());

        TableCacheInfo table2CacheInfo = new TableCacheInfo(table2QN.toString());
        table2CacheInfo.addCachedPredicate(new CachePredicate(tupleDomainA, tupleDomainAPredicateString));
        table2CacheInfo.setCachedNode(table2SplitKey1, workerNode.getNodeIdentifier());

        Map<String, TableCacheInfo> cacheInfoMap = new ConcurrentHashMap<>();
        cacheInfoMap.put(table1QN.toString(), table1CacheInfo);
        cacheInfoMap.put(table2QN.toString(), table2CacheInfo);

        String cacheInfoMapJson = mapper.writeValueAsString(cacheInfoMap);
        assertNotNull(cacheInfoMapJson);
        TypeReference<HashMap<String, TableCacheInfo>> typeRef
                = new TypeReference<HashMap<String, TableCacheInfo>>() {};
        Map<String, TableCacheInfo> deserialized = mapper.readerFor(typeRef).readValue(cacheInfoMapJson);
        assertEquals(deserialized, cacheInfoMap);
    }
}
