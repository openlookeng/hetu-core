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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.hive.HiveConnector;
import io.prestosql.plugin.hive.HiveConnectorFactory;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.tpcds.TpcdsConnectorFactory;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.testing.LocalQueryRunner;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.prestosql.SystemSessionProperties.SORT_BASED_AGGREGATION_ENABLED;
import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

public class TestSortBasedAggregationPlan
        extends AbstractCostBasedPlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestSortBasedAggregationPlan()
    {
        super(() -> {
            String catalog = "hive";
            Session.SessionBuilder sessionBuilder = testSessionBuilder()
                    .setCatalog(catalog)
                    .setSchema("default")
                    .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                    .setSystemProperty(SORT_BASED_AGGREGATION_ENABLED, "true");

            LocalQueryRunner queryRunner = LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(sessionBuilder.build(), 8);

            // add hive
            File hiveDir = new File(createTempDirectory("TestSortBasedAggregation").toFile(), "hive_data");
            HiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);
            metastore.createDatabase(
                    new HiveIdentity(SESSION),
                    Database.builder()
                            .setDatabaseName("default")
                            .setOwnerName("public")
                            .setOwnerType(PrincipalType.ROLE)
                            .build());

            HiveConnectorFactory hiveConnectorFactory = new HiveConnectorFactory(
                    "hive",
                    HiveConnector.class.getClassLoader(),
                    Optional.of(metastore));

            Map<String, String> hiveCatalogConfig = ImmutableMap.<String, String>builder()
                    .put("hive.orc-predicate-pushdown-enabled", "false")
                    .build();

            queryRunner.createCatalog(catalog, hiveConnectorFactory, hiveCatalogConfig);
            queryRunner.createCatalog("tpcds", new TpcdsConnectorFactory(1), ImmutableMap.of());

            queryRunner.execute("create  table item with (format='orc') as select * from tpcds.tiny.item");
            queryRunner.execute("create  table store_returns with (transactional = false, format='orc', bucketed_by=array['sr_customer_sk'], " +
                    "bucket_count=1, sorted_by = ARRAY['sr_customer_sk','sr_item_sk']) as select * from tpcds.tiny.store_returns");

            queryRunner.execute("create  table store_sales_customer_item with (transactional = false, format='orc'," +
                    " bucketed_by=array['ss_customer_sk'], bucket_count = 1," +
                    "sorted_by = ARRAY['ss_customer_sk', 'ss_item_sk']) as select * from tpcds.tiny.store_sales");

            queryRunner.execute("create  table store_sales_item_customer with (transactional = false, format='orc'," +
                    " bucketed_by=array['ss_item_sk', 'ss_customer_sk'], bucket_count = 3," +
                    "sorted_by = ARRAY['ss_item_sk', 'ss_customer_sk']) as select * from tpcds.tiny.store_sales");

            queryRunner.execute("create  table store_sales_item_customer_solddate with (transactional = false, format='orc'," +
                    " bucketed_by=array['ss_item_sk'], bucket_count = 1," +
                    "sorted_by = ARRAY['ss_item_sk', 'ss_customer_sk', 'ss_sold_date_sk']) as select * from tpcds.tiny.store_sales");

            queryRunner.execute("create  table store_sales_item_customer_solddate_buckArr1_buckCount4 with (transactional = false, format='orc'," +
                    " bucketed_by=array['ss_item_sk'], bucket_count = 4," +
                    "sorted_by = ARRAY['ss_item_sk', 'ss_customer_sk', 'ss_sold_date_sk']) as select * from tpcds.tiny.store_sales");

            queryRunner.execute("create  table store_sales_item_customer_solddate_buckArr2_buckCount3 with (transactional = false, format='orc'," +
                    " bucketed_by=array['ss_item_sk' , 'ss_customer_sk'], bucket_count = 3," +
                    "sorted_by = ARRAY['ss_item_sk', 'ss_customer_sk', 'ss_sold_date_sk']) as select * from tpcds.tiny.store_sales");

            queryRunner.execute("create  table store_sales_item_customer_solddate_buckArr2_buckCount1_wrngOrder with (transactional = false, format='orc'," +
                    " bucketed_by=array['ss_customer_sk', 'ss_item_sk'], bucket_count = 1," +
                    "sorted_by = ARRAY['ss_item_sk', 'ss_customer_sk', 'ss_sold_date_sk']) as select * from tpcds.tiny.store_sales");

            queryRunner.execute("create  table SortWithColNameEndWithInt(data_100 , data_101, data_102 )  with (transactional = false, format='orc'," +
                    " bucketed_by=array['data_102', 'data_101'], bucket_count = 1," +
                    "sorted_by = ARRAY['data_102', 'data_101']) as select  ss_customer_sk, ss_sold_date_sk, ss_item_sk from tpcds.tiny.store_sales");

            queryRunner.execute("create table web_returns_partition_bucketCount1 with (" +
                    "bucketed_by=array['wr_return_quantity'], bucket_count = 1, " +
                    "partitioned_by = ARRAY['wr_net_loss'], " +
                    "sorted_by = ARRAY['wr_return_quantity','wr_returned_time_sk']) " +
                    "as select * from tpcds.tiny.web_returns limit 100");

            queryRunner.execute("create table web_returns_partition_bucketCount32 with (" +
                    "bucketed_by=array['wr_return_quantity'], bucket_count = 1, " +
                    "partitioned_by = ARRAY['wr_net_loss'], " +
                    "sorted_by = ARRAY['wr_return_quantity','wr_returned_time_sk']) " +
                    "as select * from tpcds.tiny.web_returns limit 100");

            return queryRunner;
        }, false, false, false, true);
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return Stream.of("q_InnerJoin", "q_leftJoin", "q_rightjoin", "q_rightjoin_wrong_order", "q_Inner_LeftJoin", "q_sort_groupby_notsameOrder",
                "q_sort_groupby_notsameOrder1", "q_groupByHavingMore", "q_InnerJoinWrongOrder", "q_InnerJoinLessCriterias",
                "q_InnerJoinGroupLessCriterias", "q_bucketAndSortDifferentOrder", "BucketAndGroupSameSortIsMore", "BucketGroupAreDifferent",
                "groupsAreMoreThanBucket", "ColNameEndWithInt", "q_PartitionBucketCount1", "q_PartitionBucketCount32")
                .flatMap(i -> {
                    String queryId = format("%s", i);
                    System.out.println("query ID: " + queryId);
                    return Stream.of(queryId);
                })
                .map(queryId -> format("/sql/presto/queries/%s.sql", queryId));
    }

    @SuppressWarnings("unused")
    public static final class UpdateTestFiles
    {
        // Intellij doesn't handle well situation when test class has main(), hence inner class.

        private UpdateTestFiles() {}

        public static void main(String[] args)
                throws Exception
        {
            new TestSortBasedAggregationPlan().generate();
        }
    }
}
