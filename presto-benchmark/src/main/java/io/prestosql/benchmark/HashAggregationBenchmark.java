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
package io.prestosql.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PageSourceOperator;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.AggregationNode.Step;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.LocalQueryRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;

public class HashAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public static Page inputPage;
    public static Iterator<Page> inputPagesIterator;
    private final InternalAggregationFunction longSum;

    public HashAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_agg", 0, 1);

        longSum = localQueryRunner.getMetadata().getAggregateFunctionImplementation(
                new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    }

    public static void builderPage()
    {
        List<Type> dataTypes = new ArrayList<>();
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        PageBuilder pb = PageBuilder.withMaxPageSize(Integer.MAX_VALUE, dataTypes);
        BlockBuilder key = pb.getBlockBuilder(0);
        BlockBuilder value = pb.getBlockBuilder(1);
        for (int idx = 0; idx < 10_000_000; idx++) {
            key.writeLong(idx % 100_0000);
            value.writeLong(idx);
            pb.declarePosition();
        }
        inputPage = pb.build();

        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            inputPages.add(inputPage);
        }
        inputPagesIterator = inputPages.iterator();
    }

    public static void main(String[] args)
    {

        builderPage();
        LocalQueryRunner localQueryRunner = createLocalQueryRunner();

        new HashAggregationBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    OperatorFactory createOmniCacheTableScanOperator(int operatorId, PlanNodeId planNodeId, String tableName, String... columnNames)
    {
        checkArgument(session.getCatalog().isPresent(), "catalog not set");
        checkArgument(session.getSchema().isPresent(), "schema not set");

        long start = System.currentTimeMillis();

//        // look up the table
//        Metadata metadata = localQueryRunner.getMetadata();
//        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName);
//        TableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName).orElse(null);
//        checkArgument(tableHandle != null, "Table %s does not exist", qualifiedTableName);
//
//        // lookup the columns
//        Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(session, tableHandle);
//        ImmutableList.Builder<ColumnHandle> columnHandlesBuilder = ImmutableList.builder();
//        for (String columnName : columnNames) {
//            ColumnHandle columnHandle = allColumnHandles.get(columnName);
//            checkArgument(columnHandle != null, "Table %s does not have a column %s", tableName, columnName);
//            columnHandlesBuilder.add(columnHandle);
//        }
//        List<ColumnHandle> columnHandles = columnHandlesBuilder.build();
//
//        // get the split for this table
//        Split split = getLocalQuerySplit(session, tableHandle);
        System.out.println("create scan op fac in time: " + (System.currentTimeMillis() - start));

        return new OperatorFactory()
        {
            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                long start = System.currentTimeMillis();
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "BenchmarkSource");
                ConnectorPageSource pageSource = createOmniCachePageSource();//localQueryRunner.getPageSourceManager().createPageSource(session, split, tableHandle, columnHandles, Optional.empty());
                System.out.println("create scan op time: " + (System.currentTimeMillis() - start));

                return new PageSourceOperator(pageSource, operatorContext);
            }

            @Override
            public void noMoreOperators()
            {
            }

            @Override
            public OperatorFactory duplicate()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private ConnectorPageSource createOmniCachePageSource()
    {
        return new ConnectorPageSource()
        {
            boolean isFinished = false;

            @Override
            public long getCompletedBytes()
            {
                return 0;
            }

            @Override
            public long getReadTimeNanos()
            {
                return 0;
            }

            @Override
            public boolean isFinished()
            {
                return isFinished;
            }

            @Override
            public Page getNextPage()
            {
                if (inputPagesIterator.hasNext()) {
                    Page next = inputPagesIterator.next();
                    return next;
                }
                isFinished = true;
                return null;
            }

            @Override
            public long getSystemMemoryUsage()
            {
                return 0;
            }

            @Override
            public void close()
                    throws IOException
            {
            }
        };
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        long start0 = System.currentTimeMillis();

//        List<Type> tableTypes = getColumnTypes("orders", "orderstatus", "totalprice");
        System.out.println("getColumnTypes time: " + (System.currentTimeMillis() - start0));

        long start1 = System.currentTimeMillis();
        OperatorFactory tableScanOperator = createOmniCacheTableScanOperator(0, new PlanNodeId("test"), "orders", "orderstatus", "totalprice");
        System.out.println("create scan op fac execute time: " + (System.currentTimeMillis() - start1));

        long start = System.currentTimeMillis();
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(longSum.bind(ImmutableList.of(1), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                JOIN_COMPILER,
                false);
        System.out.println("create hash op fac execute time: " + (System.currentTimeMillis() - start));

        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }
}
