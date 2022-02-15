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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.MockSplit;
import io.prestosql.block.BlockJsonSerde;
import io.prestosql.client.NodeVersion;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.TestingBlockEncodingSerde;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.statestore.Member;
import io.prestosql.spi.statestore.listener.EntryEvent;
import io.prestosql.spi.statestore.listener.EntryEventType;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.type.TypeDeserializer;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.net.URI;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSplitCacheChangesListener
{
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

    private final MockSplit table1ConnSplit1 = new MockSplit("hdfs://hacluster/user/hive/warehouse/test_schema_1.db/test_table_1/a=23/000000_0", 0, 10, 1589472398267L);
    private final Split table1Split1 = new Split(catalogName, table1ConnSplit1, Lifespan.taskWide());
    private final SplitKey table1SplitKey1 = new SplitKey(table1Split1, catalogName.getCatalogName(), table1Schema, table1Name);

    private final MockSplit table1ConnSplit2 = new MockSplit("hdfs://hacluster/user/hive/warehouse/test_schema_1.db/test_table_1/b=88/000010_0", 0, 20, 1589472398269L);
    private final Split table1Split2 = new Split(catalogName, table1ConnSplit2, Lifespan.taskWide());
    private final SplitKey table1SplitKey2 = new SplitKey(table1Split2, catalogName.getCatalogName(), table1Schema, table1Name);

    private final String table2Schema = "test_schema_2";
    private final String table2Name = "test_table_2";
    private final QualifiedName table2QN = QualifiedName.of(catalogName.getCatalogName(), table2Schema, table2Name);

    private final MockSplit table2ConnSplit1 = new MockSplit("hdfs://hacluster/user/hive/warehouse/test_schema_2.db/test_table_2/a=23/000000_0", 0, 30, 1589472398269L);
    private final Split table2Split1 = new Split(catalogName, table2ConnSplit1, Lifespan.taskWide());
    private final SplitKey table2SplitKey1 = new SplitKey(table2Split1, catalogName.getCatalogName(), table2Schema, table2Name);

    private ObjectMapper objectMapper;

    @BeforeSuite
    public void setup()
    {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, true);
        BlockEncodingSerde encodingSerde = metadata.getFunctionAndTypeManager().getBlockEncodingSerde();
        objectMapper = new ObjectMapperProvider().get().registerModule(new SimpleModule()
                .addDeserializer(Type.class, new TypeDeserializer(metadata))
                .addSerializer(Block.class, new BlockJsonSerde.Serializer(encodingSerde))
                .addDeserializer(Block.class, new BlockJsonSerde.Deserializer(encodingSerde))
                .addKeyDeserializer(SplitKey.class, new SplitKey.KeyDeserializer()));
    }

    @Test
    public void testListener()
    {
        SplitCacheMap splitCacheMap = SplitCacheMapSingletonFactory.createInstance(true);
        splitCacheMap.addCache(table2QN, tupleDomainA, tupleDomainAPredicateString);
        splitCacheMap.addCachedNode(table2SplitKey1, workerNode.getNodeIdentifier());
        assertTrue(splitCacheMap.cacheExists(table2QN));
        assertFalse(splitCacheMap.cacheExists(table1QN));

        SplitCacheStateStoreChangesListener listener = new SplitCacheStateStoreChangesListener(splitCacheMap, objectMapper);
        String table1CacheInfoJson = "{\"fqTableName\":\"bogus_catalog.test_schema_1.test_table_1\",\"splitWorkersMap\":{\"SplitKey{catalog='bogus_catalog', schema='test_schema_1', table='test_table_1', start=0, end=10, lastModifiedTime=1589472398267, qualifiedTableName=bogus_catalog.test_schema_1.test_table_1, path='hdfs://hacluster/user/hive/warehouse/test_schema_1.db/test_table_1/a=23/000000_0'}\":\"838986e0-7484-4bf6-87b9-7dd0af6bf901\",\"SplitKey{catalog='bogus_catalog', schema='test_schema_1', table='test_table_1', start=0, end=20, lastModifiedTime=1589472398269, qualifiedTableName=bogus_catalog.test_schema_1.test_table_1, path='hdfs://hacluster/user/hive/warehouse/test_schema_1.db/test_table_1/b=88/000010_0'}\":\"838986e0-7484-4bf6-87b9-7dd0af6bf902\"},\"predicates\":[{\"columnMetadataTupleDomain\":{\"columnDomains\":[{\"column\":{\"name\":\"a\",\"type\":\"bigint\",\"nullable\":true,\"hidden\":false,\"properties\":{}},\"domain\":{\"values\":{\"@type\":\"sortable\",\"type\":\"bigint\",\"ranges\":[{\"low\":{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAABcAAAAAAAAA\",\"bound\":\"EXACTLY\"},\"high\":{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAABcAAAAAAAAA\",\"bound\":\"EXACTLY\"}}]},\"nullAllowed\":false}}]},\"cachePredicateString\":\"a = 23\"},{\"columnMetadataTupleDomain\":{\"columnDomains\":[{\"column\":{\"name\":\"b\",\"type\":\"bigint\",\"nullable\":true,\"hidden\":false,\"properties\":{}},\"domain\":{\"values\":{\"@type\":\"sortable\",\"type\":\"bigint\",\"ranges\":[{\"low\":{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAAFgAAAAAAAAA\",\"bound\":\"EXACTLY\"},\"high\":{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAAFgAAAAAAAAA\",\"bound\":\"EXACTLY\"}}]},\"nullAllowed\":false}}]},\"cachePredicateString\":\"b = 88\"}],\"lastUpdated\":\"2020-05-14T11:06:38.968\"}";
        Member member = new Member("127.0.0.1", 5709);
        EntryEvent<String, String> addedEvent = new EntryEvent<>(member, EntryEventType.ADDED.getTypeId(), table1QN.toString(), table1CacheInfoJson);
        listener.entryAdded(addedEvent);
        assertTrue(splitCacheMap.cacheExists(table1QN));
        assertTrue(splitCacheMap.getCachedNodeId(table1SplitKey1).map(workerNode.getNodeIdentifier()::equals).orElse(false));
        assertTrue(splitCacheMap.getCachedNodeId(table1SplitKey2).map(workerNode2.getNodeIdentifier()::equals).orElse(false));

        assertTrue(splitCacheMap.getCachedNodeId(table2SplitKey1).map(workerNode.getNodeIdentifier()::equals).orElse(false));
        String table2UpdatedJson = "{\"fqTableName\":\"bogus_catalog.test_schema_2.test_table_2\",\"splitWorkersMap\":{\"SplitKey{catalog='bogus_catalog', schema='test_schema_2', table='test_table_2', start=0, end=30, lastModifiedTime=1589472398269, qualifiedTableName=bogus_catalog.test_schema_2.test_table_2, path='hdfs://hacluster/user/hive/warehouse/test_schema_2.db/test_table_2/a=23/000000_0'}\":\"838986e0-7484-4bf6-87b9-7dd0af6bf902\"},\"predicates\":[{\"columnMetadataTupleDomain\":{\"columnDomains\":[{\"column\":{\"name\":\"a\",\"type\":\"bigint\",\"nullable\":true,\"hidden\":false,\"properties\":{}},\"domain\":{\"values\":{\"@type\":\"sortable\",\"type\":\"bigint\",\"ranges\":[{\"low\":{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAABcAAAAAAAAA\",\"bound\":\"EXACTLY\"},\"high\":{\"type\":\"bigint\",\"valueBlock\":\"CgAAAExPTkdfQVJSQVkBAAAAABcAAAAAAAAA\",\"bound\":\"EXACTLY\"}}]},\"nullAllowed\":false}}]},\"cachePredicateString\":\"a = 23\"}],\"lastUpdated\":\"2020-05-14T11:06:38.968\"}";
        EntryEvent<String, String> updatedEvent = new EntryEvent<>(member, EntryEventType.UPDATED.getTypeId(), table2QN.toString(), table2UpdatedJson);
        listener.entryUpdated(updatedEvent);
        assertTrue(splitCacheMap.cacheExists(table2QN));
        assertTrue(splitCacheMap.getCachedNodeId(table2SplitKey1).map(workerNode2.getNodeIdentifier()::equals).orElse(false));

        EntryEvent<String, String> removedEvent = new EntryEvent<>(member, EntryEventType.REMOVED.getTypeId(), table1QN.toString(), table1CacheInfoJson, null);
        listener.entryRemoved(removedEvent);
        assertFalse(splitCacheMap.cacheExists(table1QN));
        assertTrue(splitCacheMap.cacheExists(table2QN));
    }
}
