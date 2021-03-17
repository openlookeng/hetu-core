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
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.testing.LocalQueryRunner;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCDS queries.
 * This class is using TPCDS connector configured in way to mock Hive connector with unpartitioned TPCDS tables.
 */
public class TestTpcdsCostBasedPlanPushdown
        extends AbstractCostBasedPlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestTpcdsCostBasedPlanPushdown()
    {
        super(() -> {
            String catalog = "hive";
            Session.SessionBuilder sessionBuilder = testSessionBuilder()
                    .setCatalog(catalog)
                    .setSchema("default")
                    .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                    .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false");

            LocalQueryRunner queryRunner = LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(sessionBuilder.build(), 8);
            queryRunner.createCatalog("tpcds", new TpcdsConnectorFactory(1), ImmutableMap.of());

            // add hive
            File hiveDir = new File(createTempDirectory("TestTpcdsCostBasedPlanPushdown").toFile(), "hive_data");
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
                    .put("hive.orc-predicate-pushdown-enabled", "true")
                    .build();

            queryRunner.createCatalog(catalog, hiveConnectorFactory, hiveCatalogConfig);

            queryRunner.execute("create  table call_center with (format='orc') as select * from tpcds.tiny.call_center");
            queryRunner.execute("create  table catalog_page with (format='orc') as select * from tpcds.tiny.catalog_page");
            queryRunner.execute("create  table catalog_returns with (format='orc') as select * from tpcds.tiny.catalog_returns");
            queryRunner.execute("create  table catalog_sales with (format='orc') as select * from tpcds.tiny.catalog_sales");
            queryRunner.execute("create  table customer with (format='orc') as select * from tpcds.tiny.customer");
            queryRunner.execute("create  table customer_address with (format='orc') as select * from tpcds.tiny.customer_address");
            queryRunner.execute("create  table customer_demographics with (format='orc') as select * from tpcds.tiny.customer_demographics");
            queryRunner.execute("create  table date_dim with (format='orc') as select * from tpcds.tiny.date_dim");
            queryRunner.execute("create  table household_demographics with (format='orc') as select * from tpcds.tiny.household_demographics");
            queryRunner.execute("create  table income_band with (format='orc') as select * from tpcds.tiny.income_band");
            queryRunner.execute("create  table inventory with (format='orc') as select * from tpcds.tiny.inventory");
            queryRunner.execute("create  table item with (format='orc') as select * from tpcds.tiny.item");
            queryRunner.execute("create  table promotion with (format='orc') as select * from tpcds.tiny.promotion");
            queryRunner.execute("create  table reason with (format='orc') as select * from tpcds.tiny.reason");
            queryRunner.execute("create  table ship_mode with (format='orc') as select * from tpcds.tiny.ship_mode");
            queryRunner.execute("create  table store with (format='orc') as select * from tpcds.tiny.store");
            queryRunner.execute("create  table store_returns with (format='orc') as select * from tpcds.tiny.store_returns");
            queryRunner.execute("create  table store_sales with (format='orc') as select * from tpcds.tiny.store_sales");
            queryRunner.execute("create  table time_dim with (format='orc') as select * from tpcds.tiny.time_dim");
            queryRunner.execute("create  table warehouse with (format='orc') as select * from tpcds.tiny.warehouse");
            queryRunner.execute("create  table web_page with (format='orc') as select * from tpcds.tiny.web_page");
            queryRunner.execute("create  table web_returns with (format='orc') as select * from tpcds.tiny.web_returns");
            queryRunner.execute("create  table web_sales with (format='orc') as select * from tpcds.tiny.web_sales");
            queryRunner.execute("create  table web_site with (format='orc') as select * from tpcds.tiny.web_site");
            return queryRunner;
        }, true, false);
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return IntStream.range(1, 100)
                .boxed()
                .flatMap(i -> {
                    String queryId = format("q%02d", i);
                    if (i == 14 || i == 23 || i == 24 || i == 39) {
                        return Stream.of(queryId + "_1", queryId + "_2");
                    }
                    return Stream.of(queryId);
                })
                .map(queryId -> format("/sql/presto/tpcds/%s.sql", queryId));
    }

    @SuppressWarnings("unused")
    public static final class UpdateTestFiles
    {
        // Intellij doesn't handle well situation when test class has main(), hence inner class.

        private UpdateTestFiles() {}

        public static void main(String[] args)
                throws Exception
        {
            new TestTpcdsCostBasedPlanPushdown().generate();
        }
    }
}
