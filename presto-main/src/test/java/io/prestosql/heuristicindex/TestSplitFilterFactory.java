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
package io.prestosql.heuristicindex;

import com.google.common.cache.CacheLoader;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.hetu.core.heuristicindex.IndexClient;
import io.hetu.core.heuristicindex.base.BloomIndex;
import io.hetu.core.heuristicindex.base.MinMaxIndex;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import io.hetu.core.spi.heuristicindex.SplitMetadata;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.Lifespan;
import io.prestosql.metadata.Split;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.utils.FilterModule;
import io.prestosql.utils.HetuConstant;
import io.prestosql.utils.MockSplit;
import io.prestosql.utils.Predicate;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestSplitFilterFactory
{
    private static final String TABLE = "test_table";
    private static final String COLUMN = "test_column";

    private SplitFilterFactory getFactory(Map<String, List<SplitIndexMetadata>> indices) throws IOException
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.FILTER_MAX_INDICES_IN_CACHE, 10L);
        // Since IndexFactory will try to initialize an indexClient in a static block, we'll need to provide it
        // with valid required properties
        PropertyService.setProperty(HetuConstant.FILTER_PLUGINS, "");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_TYPE, "local");

        // Then we replace the indexClient in IndexFactory with our MockIndexClient
        LocalIndexCacheLoader loader = new LocalIndexCacheLoader(new MockIndexClient(indices));
        LocalIndexCache cache = new LocalIndexCache(loader, false);
        Module testFilterModule = Modules.override(new FilterModule())
                .with(new AbstractModule()
                {
                    @Override
                    protected void configure()
                    {
                        bind(IndexManager.class);
                        bind(SplitFilterFactory.class);
                        bind(IndexCache.class).toInstance(cache);
                        bind(CacheLoader.class).toInstance(loader);
                    }
                });

        Injector injector = Guice.createInjector(testFilterModule);
        SplitFilterFactory splitFilterFactory = injector.getInstance(SplitFilterFactory.class);
        return splitFilterFactory;
    }

    /**
     * 1 hetu split with one corresponding bloom index split
     * <p>
     * predicate: test_column = "test_value"
     */
    @Test
    public void testEqualsOneValidIndex() throws IOException
    {
        String value = "test_value";

        List<Split> splits = new ArrayList<>();

        String splitUri = "/user/hive/warehouse/test_schema.db/test_table/testEqualsOneValidIndex";
        MockSplit split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));

        List<SplitIndexMetadata> splitIndices = new LinkedList<>();
        Index index = new BloomIndex();
        index.addValues(new String[]{value});
        SplitMetadata splitMetadata = new SplitMetadata(TABLE, COLUMN, null, splitUri, 0);
        splitIndices.add(new SplitIndexMetadata(index, splitMetadata, 100));

        checkFilteredResult(splitUri, splitIndices, value, splits, ComparisonExpression.Operator.EQUAL, 1);
    }

    /**
     * 1 hetu split with two corresponding minmax index splits
     * <p>
     * test multiple predicates
     */
    @Test
    public void testMinMaxIndex() throws IOException
    {
        List<Split> splits = new ArrayList<>();

        String splitUri = "/user/hive/warehouse/test_schema.db/test_table/testMinMaxIndex";
        MockSplit split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));

        List<SplitIndexMetadata> splitIndices = new LinkedList<>();
        Index index = new MinMaxIndex();
        index.addValues(new Integer[]{1, 2, 3});
        // has two valid indexes
        splitIndices.add(new SplitIndexMetadata(index, new SplitMetadata(TABLE, COLUMN, null, splitUri, 0), 100));
        splitIndices.add(new SplitIndexMetadata(index, new SplitMetadata(TABLE, COLUMN, null, splitUri, 5), 100));

        // < 10
        // 1 valid split
        checkFilteredResult(splitUri, splitIndices, 10, splits, ComparisonExpression.Operator.LESS_THAN, 1);

        // < 0
        // 0 valid splits
        checkFilteredResult(splitUri, splitIndices, 0, splits, ComparisonExpression.Operator.LESS_THAN, 0);

        // > 0
        // 1 valid split
        checkFilteredResult(splitUri, splitIndices, 0, splits, ComparisonExpression.Operator.GREATER_THAN, 1);

        // > 10
        // 0 valid splits
        checkFilteredResult(splitUri, splitIndices, 10, splits, ComparisonExpression.Operator.GREATER_THAN, 0);

        // <= 1
        // 1 valid split
        checkFilteredResult(splitUri, splitIndices, 1, splits, ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, 1);

        // <= 0
        // 0 valid splits
        checkFilteredResult(splitUri, splitIndices, 0, splits, ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, 0);

        // >= 1
        // 1 valid split
        checkFilteredResult(splitUri, splitIndices, 1, splits, ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, 1);

        // >= 10
        // 0 valid splits
        checkFilteredResult(splitUri, splitIndices, 10, splits, ComparisonExpression.Operator.GREATER_THAN, 0);

        // = 2
        // 1 valid splits
        checkFilteredResult(splitUri, splitIndices, 2, splits, ComparisonExpression.Operator.EQUAL, 1);

        // = 1
        // 1 valid splits
        checkFilteredResult(splitUri, splitIndices, 1, splits, ComparisonExpression.Operator.EQUAL, 1);

        // = 3
        // 1 valid splits
        checkFilteredResult(splitUri, splitIndices, 3, splits, ComparisonExpression.Operator.EQUAL, 1);

        // = 10
        // 0 valid splits
        checkFilteredResult(splitUri, splitIndices, 10, splits, ComparisonExpression.Operator.EQUAL, 0);

        // = -1
        // 0 valid splits
        checkFilteredResult(splitUri, splitIndices, -1, splits, ComparisonExpression.Operator.EQUAL, 0);
    }

    @Test
    public void testEqualsNoValidIndex() throws IOException
    {
        String value = "test_value";

        List<Split> splits = new ArrayList<>();

        String splitUri = "/user/hive/warehouse/test_schema.db/test_table/testEqualsNoValidIndex";
        MockSplit split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        Index index = new BloomIndex();
        index.addValues(new String[]{value});

        List<SplitIndexMetadata> splitIndices = new LinkedList<>();
        splitIndices.add(new SplitIndexMetadata(index, new SplitMetadata(TABLE, COLUMN, null, splitUri, 0), 100));

        checkFilteredResult(splitUri, splitIndices, "not_found", splits, ComparisonExpression.Operator.EQUAL, 0);
    }

    /**
     * This test is based on testEqualsNoValidIndex,
     * the first time the SplitFilter is created, the split should get filtered out
     * <p>
     * the second time the SplitFilter is created, the hetu split's lastModifiedTIme
     * has been increased, so the cached indexes should no longer be valid
     * and no filtering should be performed
     */
    @Test
    public void testUpdatedSplitLastModifiedTime() throws IOException
    {
        String value = "test_value";

        List<Split> splits = new ArrayList<>();

        String splitUri = "/user/hive/warehouse/test_schema.db/test_table/testLastModifiedTime";
        MockSplit split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        Index index = new BloomIndex();
        index.addValues(new String[]{value});

        List<SplitIndexMetadata> splitIndices = new LinkedList<>();
        splitIndices.add(new SplitIndexMetadata(index, new SplitMetadata(TABLE, COLUMN, null, splitUri, 0), 100));

        checkFilteredResult(splitUri, splitIndices, "not_found", splits, ComparisonExpression.Operator.EQUAL, 0);

        // update the split time, should no longer get filtered
        split = new MockSplit(splitUri, 0, 10, 150);
        splits = new ArrayList<>();
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        checkFilteredResult(splitUri, splitIndices, "not_found", splits, ComparisonExpression.Operator.EQUAL, 1);
    }

    /**
     * the index exists but is old
     */
    @Test
    public void testOldIndex() throws IOException
    {
        String value = "test_value";

        List<Split> splits = new ArrayList<>();

        String splitUri = "/user/hive/warehouse/test_schema.db/test_table/testOldIndex";
        MockSplit split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        Index index = new BloomIndex();
        index.addValues(new String[]{value});

        List<SplitIndexMetadata> splitIndices = new LinkedList<>();
        splitIndices.add(new SplitIndexMetadata(index, new SplitMetadata(TABLE, COLUMN, null, splitUri, 0), 50)); // invalid index

        checkFilteredResult(splitUri, splitIndices, "not_found", splits, ComparisonExpression.Operator.EQUAL, 1);
    }

    @Test
    public void testMultipleSplitIndices() throws IOException
    {
        String value = "test_value";

        List<Split> splits = new ArrayList<>();

        String splitUri = "/user/hive/warehouse/test_schema.db/test_table/testMultipleSplitIndices";
        MockSplit split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));

        // add indices in random order
        List<SplitIndexMetadata> splitIndices = new LinkedList<>();
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 1), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 3), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 2), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 5), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 4), 100));
        // Adding in the only index that contains the value
        Index index = new BloomIndex();
        index.addValues(new String[]{value});
        splitIndices.add(new SplitIndexMetadata(index, new SplitMetadata(TABLE, COLUMN, null, splitUri, 10), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 6), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 7), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 9), 100));
        splitIndices.add(new SplitIndexMetadata(new BloomIndex(), new SplitMetadata(TABLE, COLUMN, null, splitUri, 8), 100));

        checkFilteredResult(splitUri, splitIndices, value, splits, ComparisonExpression.Operator.EQUAL, 1);
    }

    @Test
    public void testMissingIndex() throws IOException
    {
        List<Split> splits = new ArrayList<>();

        String splitUri = "/user/hive/warehouse/test_schema.db/test_table/testMissingIndex";
        MockSplit split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));

        Map<String, List<SplitIndexMetadata>> indices = new HashMap<>();

        Predicate predicate = new Predicate();
        predicate.setTableName(TABLE);
        predicate.setColumnName(COLUMN);
        predicate.setValue("not_found");
        predicate.setOperator(ComparisonExpression.Operator.EQUAL);
        SplitFilter splitFilter = getFactory(indices).getFilter(predicate, splits);
        List<Split> filteredSplits = splitFilter.filter(splits, predicate.getValue());
        assertEquals(filteredSplits.size(), 1);
    }

    /**
     * test filtering through multiple index types
     * <p>
     * for example assume the value being searched is 10
     * and the minmax index has values min=10 max=20
     * and bloom index has values  [1,3,5,10,20]
     * notice that for the equal operator, we don't need to use the bloom index
     * since minmax will already tell us the value is found
     * <p>
     */
    @Test
    public void testMultipleIndexes() throws IOException
    {
        int value = 15;

        List<Split> splits = new ArrayList<>();

        String splitUri;
        MockSplit split;
        Object[] minmaxValues;
        Object[] bloomValues;

        // Note: splitUri needs to be different for each test cases because of the index cache
        splitUri = "/user/hive/warehouse/test_schema.db/test_table/testMultipleIndexes1";
        split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        minmaxValues = new Object[]{value, 20};
        bloomValues = new Object[]{value};
        testMultipleIndicesHelper(splitUri, minmaxValues, bloomValues, splits, value, 1);
        splits.clear();

        splitUri = "/user/hive/warehouse/test_schema.db/test_table/testMultipleIndexes2";
        split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        minmaxValues = new Object[]{value};
        bloomValues = new Object[]{value};
        testMultipleIndicesHelper(splitUri, minmaxValues, bloomValues, splits, value, 1);
        splits.clear();

        splitUri = "/user/hive/warehouse/test_schema.db/test_table/testMultipleIndexes3";
        split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        minmaxValues = new Object[]{value - 1, value + 1};
        bloomValues = new Object[]{};
        testMultipleIndicesHelper(splitUri, minmaxValues, bloomValues, splits, value, 0);
        splits.clear();

        splitUri = "/user/hive/warehouse/test_schema.db/test_table/testMultipleIndexes4";
        split = new MockSplit(splitUri, 0, 10, 100);
        splits.add(new Split(new CatalogName("test"), split, Lifespan.taskWide()));
        minmaxValues = new Object[]{value + 10};
        bloomValues = new Object[]{value + 20};
        testMultipleIndicesHelper(splitUri, minmaxValues, bloomValues, splits, value, 0);
        splits.clear();
    }

    private void testMultipleIndicesHelper(String splitUri, Object[] minmaxValues, Object[] bloomValues,
                                           List<Split> splits, Object predicateValue, int remainedSplitsCount) throws IOException
    {
        List<SplitIndexMetadata> splitIndices = new LinkedList<>();
        Index bloom = new BloomIndex(); // empty bloom
        if (bloomValues.length > 0) {
            bloom.addValues(bloomValues);
        }
        splitIndices.add(new SplitIndexMetadata(bloom, new SplitMetadata(TABLE, COLUMN, null, splitUri, 0), 100));

        Index minmax = new MinMaxIndex();
        if (minmaxValues.length > 0) {
            minmax.addValues(minmaxValues);
        }
        splitIndices.add(new SplitIndexMetadata(minmax, new SplitMetadata(TABLE, COLUMN, null, splitUri, 0), 100));

        checkFilteredResult(splitUri, splitIndices, predicateValue, splits, ComparisonExpression.Operator.EQUAL, remainedSplitsCount);
    }

    private synchronized void checkFilteredResult(String splitUri, List<SplitIndexMetadata> splitIndices, Object predicateValue, List<Split> splits, ComparisonExpression.Operator operator, int remainedSplitsCount) throws IOException
    {
        Map<String, List<SplitIndexMetadata>> indices = new HashMap<>();
        indices.put(Paths.get(TABLE, COLUMN, splitUri).toString(), splitIndices);
        Predicate predicate = new Predicate();
        predicate.setTableName(TABLE);
        predicate.setColumnName(COLUMN);
        predicate.setValue(predicateValue);
        predicate.setOperator(operator);
        // Careful!!
        // #getFactory(indices) will inject the dependencies, and #getFilter(predicate, splits) will trigger the class loading
        // they have to happen in the same thread, otherwise it will fail.
        // Thus, this method is made synchronized to make sure they run in the same thread.
        SplitFilter splitFilter = getFactory(indices).getFilter(predicate, splits);
        List<Split> filteredSplits = splitFilter.filter(splits, predicate.getValue());
        assertEquals(filteredSplits.size(), remainedSplitsCount);
    }

    static class MockIndexClient
            extends IndexClient
    {
        Map<String, List<SplitIndexMetadata>> indices;

        public MockIndexClient(Map<String, List<SplitIndexMetadata>> indices) throws IOException
        {
            super(Collections.emptySet(), null);
            this.indices = indices;
        }

        @Override
        public long getLastModified(String path) throws IOException
        {
            List<SplitIndexMetadata> list = indices.get(path);
            if (list == null || list.isEmpty()) {
                return 0;
            }
            else {
                return list.get(0).getLastUpdated();
            }
        }

        @Override
        public List<SplitIndexMetadata> readSplitIndex(String path, String... filterIndexTypes) throws IOException
        {
            return indices.get(path);
        }
    }
}
