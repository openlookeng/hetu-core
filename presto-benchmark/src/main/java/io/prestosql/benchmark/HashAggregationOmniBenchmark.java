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
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.HashAggregationOmniOperator;
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
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.LocalQueryRunner;
import nova.hetu.omnicache.runtime.OmniRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;

public class HashAggregationOmniBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public static Page inputPage;
    public static Iterator<Page> inputPagesIterator;
    private final InternalAggregationFunction longSum;

    public HashAggregationOmniBenchmark(LocalQueryRunner localQueryRunner)
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
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        PageBuilder pb = PageBuilder.withMaxPageSize(Integer.MAX_VALUE, dataTypes);
        BlockBuilder group1 = pb.getBlockBuilder(0);
        BlockBuilder group2 = pb.getBlockBuilder(1);
        BlockBuilder sum1 = pb.getBlockBuilder(2);
        BlockBuilder sum2 = pb.getBlockBuilder(3);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                group1.writeLong(i);
                group2.writeLong(j);
                sum1.writeLong(i);
                sum2.writeLong(j);
                pb.declarePosition();
            }
        }
        inputPage = pb.build();

        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            inputPages.add(inputPage);
        }
        inputPagesIterator = inputPages.iterator();
    }

    public static void main(String[] args)
    {

        builderPage();
        LocalQueryRunner localQueryRunner = createLocalQueryRunner();

        new HashAggregationOmniBenchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    OperatorFactory createOmniCacheTableScanOperator(int operatorId, PlanNodeId planNodeId, String tableName, String... columnNames)
    {
        checkArgument(session.getCatalog().isPresent(), "catalog not set");
        checkArgument(session.getSchema().isPresent(), "schema not set");
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
        return new OperatorFactory()
        {
            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "BenchmarkSource");
                ConnectorPageSource pageSource = createOmniCachePageSource();//localQueryRunner.getPageSourceManager().createPageSource(session, split, tableHandle, columnHandles, Optional.empty());

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
        OperatorFactory tableScanOperator = createOmniCacheTableScanOperator(0, new PlanNodeId("test"), "orders", "orderstatus", "totalprice");

        String compileID;
        OmniRuntime omniRuntime;
        //omni
        long start = System.currentTimeMillis();
        omniRuntime = new OmniRuntime();
//        String code = "|k:vec[i64],v:vec[i64]|" +
//                "let rs = tovec(result(for(zip(k,v),dictmerger[i64,i64,+],|b,i,n| merge(b,{n.$0,n.$1}))));" +
//                "let k = result(for(rs,appender[i64],|b,i,n| merge(b,n.$0)));" +
//                "let v = result(for(rs,appender[i64],|b,i,n| merge(b,n.$1)));" +
////                "{k,v}";
//        String code = "|v0 :vec[vec[i64]], v1: vec[vec[i64]], v2: vec[vec[f64]], v3: vec[vec[f64]]|let sum_dict_ = for(zip(v0, v1, v2, v3), dictmerger[{i64,i64}, {f64, f64},+], |b,i,n|for(zip(n.0,n.1, n.2,n.3), b, |b_, i_, m|merge(b, {{m.0,m.1}, {m.2,m.3}})));let dict_0_1 = tovec(result(sum_dict_));let k0 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.0.0)));let k1 = result(for(dict_0_1, appender[i64], |b, i, n| merge(b, n.0.1)));let sum_1 = result(for(dict_0_1, appender[f64], |b, i, n| merge(b, n.1.0)));let sum_2 = result(for(dict_0_1, appender[f64], |b, i, n| merge(b, n.1.1)));{k0, k1, sum_1, sum_2}";

        String code = "|v0 :vec[vec[i64]], v1: vec[vec[i64]], v2: vec[vec[f64]], v3: vec[vec[f64]]|" +
                "let sum_dict_ = for(zip(v0, v1, v2, v3), dictmerger[{i64,i64}, {f64, f64},+], |b,i,n| " +
                "for(zip(n.$0, n.$1, n.$2, n.$3), b, |b_, i_, m|" +
                "merge(b, {{m.$0, m.$1}, {m.$2, m.$3}})));" +
                "let dict_0_1 = tovec(result(sum_dict_));" +
                "let k0 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$0.$0)));" +
                "let k1 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$0.$1)));" +
                "let sum_1 = result( for (dict_0_1, appender[f64], |b, i, n | merge(b, n.$1.$0)));" +
                "let sum_2 = result( for (dict_0_1, appender[f64], |b, i, n | merge(b, n.$1.$1)));" +
                "{k0, k1, sum_1, sum_2}";
        compileID = omniRuntime.compile(code);
        long end = System.currentTimeMillis();
        System.out.println("omni compile time: " + (end - start));

        HashAggregationOmniOperator.HashAggregationOmniOperatorFactory aggregationOperator = new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(1, new PlanNodeId("1"), omniRuntime, Collections.singletonList(compileID));
        System.out.println("create hash op fac execute time: " + (System.currentTimeMillis() - start));

        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }
}
