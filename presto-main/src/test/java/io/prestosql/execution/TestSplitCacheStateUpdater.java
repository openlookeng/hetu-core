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
import io.airlift.units.Duration;
import io.prestosql.MockSplit;
import io.prestosql.block.BlockJsonSerde;
import io.prestosql.client.NodeVersion;
import io.prestosql.execution.SplitCacheStateInitializer.InitializationStatus;
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
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.listener.MapListener;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.type.TypeDeserializer;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSplitCacheStateUpdater
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
    public void testUpdateWhenStateStillInitializing()
            throws InterruptedException
    {
        StateStoreProvider provider = mock(StateStoreProvider.class);
        StateStore stateStore = mock(StateStore.class);
        when(stateStore.getName()).thenReturn("mock");
        when(provider.getStateStore()).thenReturn(stateStore);

        AtomicReference<InitializationStatus> status = new AtomicReference<>(InitializationStatus.INITIALIZING);
        SplitCacheMap splitCacheMap = createNew();
        SplitCacheStateUpdater updater = new SplitCacheStateUpdater(provider, splitCacheMap, new Duration(300, TimeUnit.MILLISECONDS), objectMapper, status);
        try {
            updater.start();
            Thread.sleep(1000);
        }
        finally {
            updater.stop();
        }
        verify(provider, atLeast(1)).getStateStore();
        verify(stateStore, never()).getStateCollection(StateStoreConstants.SPLIT_CACHE_METADATA_NAME);
    }

    @Test
    public void testStateUpdates()
            throws InterruptedException, IOException
    {
        StateStoreProvider provider = mock(StateStoreProvider.class);
        StateStore stateStore = mock(StateStore.class);
        when(stateStore.getName()).thenReturn("mock");

        Map<String, String> testStateMap = new HashMap<>();
        when(stateStore.getStateCollection(StateStoreConstants.SPLIT_CACHE_METADATA_NAME))
                .thenReturn(new MockStateMap<>(StateStoreConstants.SPLIT_CACHE_METADATA_NAME, testStateMap));

        //simulate behaviour wait until state store is ready
        when(provider.getStateStore()).thenReturn(null).thenReturn(stateStore);

        SplitCacheMap splitCacheMap = createNew();
        assertTrue(testStateMap.isEmpty());
        AtomicReference<InitializationStatus> status = new AtomicReference<>(InitializationStatus.COMPLETED);
        SplitCacheStateUpdater updater = new SplitCacheStateUpdater(provider, splitCacheMap, new Duration(500, TimeUnit.MILLISECONDS), objectMapper, status);
        try {
            updater.start();
            splitCacheMap.addCache(table1QN, tupleDomainA, tupleDomainAPredicateString);
            splitCacheMap.addCachedNode(table1SplitKey1, workerNode.getNodeIdentifier());
            Thread.sleep(2000);

            assertTrue(testStateMap.containsKey(table1QN.toString()));
            assertEquals(testStateMap.size(), 1);
            TableCacheInfo deserTable1Cache = objectMapper.readerFor(TableCacheInfo.class).readValue(testStateMap.get(table1QN.toString()));
            assertEquals(deserTable1Cache, splitCacheMap.tableCacheInfoMap().get(table1QN.toString()));

            splitCacheMap.addCache(table1QN, tupleDomainB, tupleDomainBPredicateString);
            splitCacheMap.addCachedNode(table1SplitKey2, workerNode2.getNodeIdentifier());
            splitCacheMap.addCache(table2QN, tupleDomainA, tupleDomainAPredicateString);
            splitCacheMap.addCachedNode(table2SplitKey1, workerNode2.getNodeIdentifier());

            Thread.sleep(2000);
            assertEquals(testStateMap.size(), 2);
            deserTable1Cache = objectMapper.readerFor(TableCacheInfo.class).readValue(testStateMap.get(table1QN.toString()));
            assertEquals(deserTable1Cache, splitCacheMap.tableCacheInfoMap().get(table1QN.toString()));
            TableCacheInfo deserTable2Cache = objectMapper.readerFor(TableCacheInfo.class).readValue(testStateMap.get(table2QN.toString()));
            assertEquals(deserTable2Cache, splitCacheMap.tableCacheInfoMap().get(table2QN.toString()));

            splitCacheMap.dropCache(table1QN, Optional.empty());
            assertFalse(splitCacheMap.cacheExists(table1QN));

            Thread.sleep(2000);

            assertEquals(testStateMap.size(), 1);
            assertTrue(testStateMap.containsKey(table2QN.toString()));
        }
        finally {
            updater.stop();
        }
    }

    static class MockStateMap<K, V>
            implements StateMap<K, V>
    {
        Map<K, V> map;
        String name;

        public MockStateMap(String name, Map<K, V> map)
        {
            this.name = name;
            this.map = map;
        }

        @Override
        public V get(K key)
        {
            return map.get(key);
        }

        @Override
        public Map<K, V> getAll(Set<K> keys)
        {
            return null;
        }

        @Override
        public Map<K, V> getAll()
        {
            return map;
        }

        @Override
        public V put(K key, V value)
        {
            return map.put(key, value);
        }

        @Override
        public V putIfAbsent(K key, V value)
        {
            return map.putIfAbsent(key, value);
        }

        @Override
        public void putAll(Map<K, V> map)
        {
            map.putAll(map);
        }

        @Override
        public V remove(K key)
        {
            return map.remove(key);
        }

        @Override
        public void removeAll(Set<K> keys)
        {
            keys.stream().forEach(k -> map.remove(k));
        }

        @Override
        public V replace(K key, V value)
        {
            return map.replace(key, value);
        }

        @Override
        public boolean containsKey(K key)
        {
            return map.containsKey(key);
        }

        @Override
        public Set<K> keySet()
        {
            return map.keySet();
        }

        @Override
        public void addEntryListener(MapListener listener) {}

        @Override
        public void removeEntryListener(MapListener listener) {}

        @Override
        public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction)
        {
            return null;
        }

        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public Type getType()
        {
            return Type.MAP;
        }

        @Override
        public void clear()
        {
            map.clear();
        }

        @Override
        public int size()
        {
            return map.size();
        }

        @Override
        public boolean isEmpty()
        {
            return map.isEmpty();
        }

        @Override
        public void destroy()
        {
            map.clear();
        }
    }
}
