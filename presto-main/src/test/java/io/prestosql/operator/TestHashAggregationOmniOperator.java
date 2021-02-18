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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.builder.HashAggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.Spiller;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.omnicache.runtime.OmniRuntime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHashAggregationOmniOperator
{
    private static final Metadata metadata = createTestMetadataManager();

    private static final InternalAggregationFunction LONG_AVERAGE = metadata.getAggregateFunctionImplementation(
            new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction LONG_SUM = metadata.getAggregateFunctionImplementation(
            new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getAggregateFunctionImplementation(
            new Signature("count", AGGREGATE, BIGINT.getTypeSignature()));

    private static final int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private JoinCompiler joinCompiler = new JoinCompiler(createTestMetadataManager());
    private DummySpillerFactory spillerFactory;
    String weldIR = "|v0 :vec[vec[i64]], v1: vec[vec[i64]], v2: vec[vec[f64]], v3: vec[vec[f64]]|" +
            "let sum_dict_ = for(zip(v0, v1, v2, v3), dictmerger[{i64,i64}, {f64, f64},+], |b,i,n| " +
            "for(zip(n.$0, n.$1, n.$2, n.$3), b, |b_, i_, m|" +
            "merge(b, {{m.$0, m.$1}, {m.$2, m.$3}})));" +
            "let dict_0_1 = tovec(result(sum_dict_));" +
            "let k0 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$0.$0)));" +
            "let k1 = result( for (dict_0_1, appender[i64], |b, i, n | merge(b, n.$0.$1)));" +
            "let sum_1 = result( for (dict_0_1, appender[f64], |b, i, n | merge(b, n.$1.$0)));" +
            "let sum_2 = result( for (dict_0_1, appender[f64], |b, i, n | merge(b, n.$1.$1)));" +
            "{k0, k1, sum_1, sum_2}";

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider(name = "hashEnabledAndMemoryLimitForMergeValues")
    public static Object[][] hashEnabledAndMemoryLimitForMergeValuesProvider()
    {
        return new Object[][] {
                {true, true, true, 8, Integer.MAX_VALUE},
                {true, true, false, 8, Integer.MAX_VALUE},
                {false, false, false, 0, 0},
                {false, true, true, 0, 0},
                {false, true, false, 0, 0},
                {false, true, true, 8, 0},
                {false, true, false, 8, 0},
                {false, true, true, 8, Integer.MAX_VALUE},
                {false, true, false, 8, Integer.MAX_VALUE}};
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        spillerFactory = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private List<Page> builderPage()
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
            for (int j = 0; j < pageRows; j++) {
                group1.writeLong(i);
                group2.writeLong(i);
                sum1.writeLong(1);
                sum2.writeLong(1);
                pb.declarePosition();
            }
        }
        Page build = pb.build();

        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < totalPageCount; i++) {
            inputPages.add(build);
        }
        return inputPages;
    }

    int pageRows = 10;
    int totalPageCount = 1000;

    @Test(invocationCount = 20)
    public void testHashAggregation()
    {

        int threadNum = 100;
        List<Page> input = builderPage();
        OmniRuntime omniRuntime = new OmniRuntime();
        String compileID = omniRuntime.compile(weldIR);

        DriverContext driverContext = createDriverContext(Integer.MAX_VALUE);

        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT);

        long sum = totalPageCount * pageRows;
        expectedBuilder.row(0L, 0L, sum, sum);
        expectedBuilder.row(1L, 1L, sum, sum);
        MaterializedResult expected = expectedBuilder.build();

        ExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadNum));
        ArrayList<ListenableFuture<List<Page>>> futureArrayList = new ArrayList<>();
        List<List<Page>> pagesList = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            final int x = i;
            ListenableFuture<List<Page>> submit = (ListenableFuture<List<Page>>) service.submit(() -> toPages(new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(x, new PlanNodeId(String.valueOf(x)), omniRuntime, Collections.singletonList(compileID)), driverContext, input, false));
            submit.addListener(() -> {
                List<Page> pages = null;
                try {
                    pages = submit.get();
                }
                catch (InterruptedException|ExecutionException e) {
                    e.printStackTrace();
                }
                pagesList.add(pages);
            }, MoreExecutors.directExecutor());
            futureArrayList.add(submit);
        }

        while (!futureArrayList.isEmpty()) {
            Iterator<ListenableFuture<List<Page>>> iterator = futureArrayList.iterator();
            while (iterator.hasNext()) {
                ListenableFuture<List<Page>> next = iterator.next();
                if (next.isDone()) {
                    if (futureArrayList.size()%10==0) {
                        System.out.println("thread i finsished: " + futureArrayList.size());
                    }
                    iterator.remove();
                }
            }
        }
        assertNotEquals(0, pagesList.size());
        for (int i = 0; i < pagesList.size(); i++) {
            assertPagesEqualIgnoreOrder(driverContext, pagesList.get(i), expected, false, Optional.empty());
        }
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Integer.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private int getHashCapacity(Operator operator)
    {
        assertTrue(operator instanceof HashAggregationOperator);
        HashAggregationBuilder aggregationBuilder = ((HashAggregationOperator) operator).getAggregationBuilder();
        if (aggregationBuilder == null) {
            return 0;
        }
        assertTrue(aggregationBuilder instanceof InMemoryHashAggregationBuilder);
        return ((InMemoryHashAggregationBuilder) aggregationBuilder).getCapacity();
    }

    private static class FailingSpillerFactory
            implements SpillerFactory
    {
        @Override
        public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext)
        {
            return new Spiller()
            {
                @Override
                public ListenableFuture<?> spill(Iterator<Page> pageIterator)
                {
                    return immediateFailedFuture(new IOException("Failed to spill"));
                }

                @Override
                public List<Iterator<Page>> getSpills()
                {
                    return ImmutableList.of();
                }

                @Override
                public void close()
                {
                }
            };
        }
    }
}
