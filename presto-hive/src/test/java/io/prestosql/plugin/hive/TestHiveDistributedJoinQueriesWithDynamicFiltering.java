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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import io.prestosql.Session;
import io.prestosql.operator.OperatorStats;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.tests.ResultWithQueryId;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Supplier;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static io.airlift.tpch.TpchTable.getTables;
import static io.prestosql.SystemSessionProperties.DYNAMIC_FILTERING_WAIT_TIME;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveQueryRunner.createQueryRunnerWithStateStore;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveTestUtils.createTestHdfsEnvironment;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultHiveDataStreamFactories;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultHiveSelectiveFactories;
import static io.prestosql.plugin.hive.HiveTestUtils.getNoOpIndexCache;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveDistributedJoinQueriesWithDynamicFiltering
        extends AbstractTestQueryFramework
{
    public TestHiveDistributedJoinQueriesWithDynamicFiltering()
    {
        super(() -> createQueryRunnerWithStateStore(getTables()));
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                .setSystemProperty(DYNAMIC_FILTERING_WAIT_TIME, "2000ms")
                .build();
    }

    @Test
    public void testJoinWithEmptyBuildSide()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .build();
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                session,
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice = 123.4567");
        assertEquals(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:lineitem");
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertEquals(probeStats.getInputPositions(), 0L);
    }

    @Test
    public void testIsPartitionFiltered()
            throws IOException
    {
        Properties schema = new Properties();

        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("p1", "100"), new HivePartitionKey("p2", "101"), new HivePartitionKey("p3", "__HIVE_DEFAULT_PARTITION__"));

        HiveSplitWrapper split = HiveSplitWrapper.wrap(new HiveSplit("db", "table", "partitionId", "path", 0, 50, 50, 0, schema, partitionKeys, ImmutableList.of(), OptionalInt.empty(), false, ImmutableMap.of(), Optional.empty(), false, Optional.empty(), Optional.empty(), false));

        List<Long> filterValues = ImmutableList.of(1L, 50L, 100L);

        HiveColumnHandle testColumnHandle = new HiveColumnHandle("p1", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), 0, PARTITION_KEY, Optional.empty());
        Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilter = createDynamicFilterSupplier(filterValues, testColumnHandle, "filter1");
        Optional<DynamicFilterSupplier> dynamicFilterSupplier = Optional.of(new DynamicFilterSupplier(dynamicFilter, System.currentTimeMillis(), 10000));

        HiveColumnHandle testColumnHandle2 = new HiveColumnHandle("p2", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), 0, PARTITION_KEY, Optional.empty());
        Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilter2 = createDynamicFilterSupplier(filterValues, testColumnHandle2, "filter2");
        Optional<DynamicFilterSupplier> dynamicFilterSupplier2 = Optional.of(new DynamicFilterSupplier(dynamicFilter2, System.currentTimeMillis(), 10000));

        HiveColumnHandle testColumnHandle3 = new HiveColumnHandle("p3", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), 0, PARTITION_KEY, Optional.empty());
        Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilter3 = createDynamicFilterSupplier(filterValues, testColumnHandle3, "filter3");
        Optional<DynamicFilterSupplier> dynamicFilterSupplier3 = Optional.of(new DynamicFilterSupplier(dynamicFilter3, System.currentTimeMillis(), 10000));

        HiveColumnHandle testColumnHandle4 = new HiveColumnHandle("p4", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), 0, PARTITION_KEY, Optional.empty());
        Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilter4 = createDynamicFilterSupplier(filterValues, testColumnHandle4, "filter3");
        Optional<DynamicFilterSupplier> dynamicFilterSupplier4 = Optional.of(new DynamicFilterSupplier(dynamicFilter4, System.currentTimeMillis(), 0));

        HiveConfig config = new HiveConfig();
        HivePageSourceProvider provider = new HivePageSourceProvider(config, createTestHdfsEnvironment(config), getDefaultHiveRecordCursorProvider(config), getDefaultHiveDataStreamFactories(config), TYPE_MANAGER, getNoOpIndexCache(), getDefaultHiveSelectiveFactories(config));

        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(config, new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ConnectorTableHandle table = new HiveTableHandle("db", "table", ImmutableMap.of(), ImmutableList.of(), Optional.empty());
        HiveTransactionHandle transaction = new HiveTransactionHandle();

        try {
            ConnectorPageSource result = provider.createPageSource(transaction, session, split, table, ImmutableList.of(testColumnHandle), dynamicFilterSupplier);
            assertFalse(result instanceof FixedPageSource);
        }
        catch (Exception e) {
            assertTrue(e instanceof PrestoException);
        }

        try {
            ConnectorPageSource result = provider.createPageSource(transaction, session, split, table, ImmutableList.of(testColumnHandle2), dynamicFilterSupplier2);
            assertTrue(result instanceof FixedPageSource);
        }
        catch (Exception e) {
            fail("A FixedPageSource object should have been created");
        }

        try {
            ConnectorPageSource result = provider.createPageSource(transaction, session, split, table, ImmutableList.of(testColumnHandle3), dynamicFilterSupplier3);
            assertFalse(result instanceof FixedPageSource);
        }
        catch (Exception e) {
            assertTrue(e instanceof PrestoException);
        }

        try {
            ConnectorPageSource result = provider.createPageSource(transaction, session, split, table, ImmutableList.of(testColumnHandle3), dynamicFilterSupplier4);
            assertFalse(result instanceof FixedPageSource);
        }
        catch (Exception e) {
            assertTrue(e instanceof PrestoException);
        }
    }

    @Test
    public void testJoinWithSelectiveBuildSide()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .build();
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                session,
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:lineitem");
        // Probe side may be partially scanned, depending on the drivers' scheduling:
        assertLessThanOrEqual(probeStats.getInputPositions(), countRows("lineitem"));
    }

    private OperatorStats searchScanFilterAndProjectOperatorStats(QueryId queryId, String tableName)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        Plan plan = runner.getQueryPlan(queryId);
        PlanNodeId nodeId = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    if (!(node instanceof ProjectNode)) {
                        return false;
                    }
                    ProjectNode projectNode = (ProjectNode) node;
                    FilterNode filterNode = (FilterNode) projectNode.getSource();
                    TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
                    return tableName.equals(tableScanNode.getTable().getConnectorHandle().toString());
                })
                .findOnlyElement()
                .getId();
        return runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeId.equals(summary.getPlanNodeId()))
                .collect(MoreCollectors.onlyElement());
    }

    private Long countRows(String tableName)
    {
        MaterializedResult result = getQueryRunner().execute("SELECT COUNT() FROM " + tableName);
        return (Long) result.getOnlyValue();
    }

    private Supplier<Map<ColumnHandle, DynamicFilter>> createDynamicFilterSupplier(List<Long> values, ColumnHandle columnHandle, String filterId)
            throws IOException
    {
        BloomFilter filter = new BloomFilter(values.size(), 0.01);
        for (Long value : values) {
            filter.add(value);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        filter.writeTo(out);

        DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, columnHandle, out.toByteArray(), DynamicFilter.Type.GLOBAL);

        return () -> ImmutableMap.of(columnHandle, dynamicFilter);
    }
}
