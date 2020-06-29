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
package io.prestosql.utils;

import io.prestosql.connector.CatalogName;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.Split;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestSplitUtils
{
    /**
     * This test will not actually filter any splits since the indexes will not be found,
     * instead it's just testing the flow. The actual filtering is tested in other classes.
     */
    @Test
    public void testGetFilteredSplit()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.FILTER_MAX_INDICES_IN_CACHE, Long.valueOf(100));
        PropertyService.setProperty(HetuConstant.FILTER_PLUGINS, "");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_URI, "/tmp/hetu/indices");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_TYPE, "local");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_CONFIG_RESOURCES, "/tmp/core-site.xml,/tmp/hdfs-site.xml");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_AUTHENTICATION_TYPE, "KERBEROS");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_CONFIG_PATH, "/tmp/krb5.conf");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_KEYTAB_PATH, "/tmp/user.keytab");
        PropertyService.setProperty(HetuConstant.INDEXSTORE_HDFS_KRB5_PRINCIPAL, "user");

        ComparisonExpression expr = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new StringLiteral("test_value"));

        SqlStageExecution stage = TestUtil.getTestStage(expr);

        List<Split> mockSplits = new ArrayList<>();
        MockSplit mock = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_0", 0, 10, 0);
        MockSplit mock1 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_1", 0, 10, 0);
        MockSplit mock2 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000001_0", 0, 10, 0);
        MockSplit mock3 = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_4", 0, 10, 0);

        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock, Lifespan.taskWide()));
        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock1, Lifespan.taskWide()));
        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock2, Lifespan.taskWide()));
        mockSplits.add(new Split(new CatalogName("bogus_catalog"), mock3, Lifespan.taskWide()));

        SplitSource.SplitBatch nextSplits = new SplitSource.SplitBatch(mockSplits, true);
        List<Split> filteredSplits = SplitUtils.getFilteredSplit(PredicateExtractor.buildPredicates(stage), nextSplits);
        assertNotNull(filteredSplits);
        assertEquals(filteredSplits.size(), 4);
    }

    @Test
    public void testGetSplitKey()
    {
        List<Split> splits = new ArrayList<>();
        MockSplit mock = new MockSplit("hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_0", 0, 10, 0);
        splits.add(new Split(new CatalogName("test"), mock, Lifespan.taskWide()));

        String splitKey = SplitUtils.getSplitKey(splits.get(0));
        assertEquals(splitKey, "test:hdfs://hacluster/AppData/BIProd/DWD/EVT/bogus_table/000000_0");
    }
}
