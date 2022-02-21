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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.Lifespan;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RowBlockBuilder;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.util.StructuralTestUtil.appendToBlockBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.fail;

public final class OperatorAssertion
{
    private static final Duration BLOCKED_DEFAULT_TIMEOUT = new Duration(10, MILLISECONDS);
    private static final Duration UNBLOCKED_DEFAULT_TIMEOUT = new Duration(1, SECONDS);

    private OperatorAssertion()
    {
    }

    public static List<Page> toPages(Operator operator, Iterator<Page> input)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartial(operator, input))
                .addAll(finishOperator(operator))
                .build();
    }

    public static List<Page> toPages(Operator operator, Iterator<Page> input, boolean revokeMemoryWhenAddingPages)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartial(operator, input, revokeMemoryWhenAddingPages))
                .addAll(finishOperator(operator))
                .build();
    }

    public static List<Page> toPagesWithRestoreToNewOperator(List<Operator> operators, List<Page> input, boolean revokeMemoryWhenAddingPages, OperatorFactory factory, Supplier<DriverContext> driverContextSupplier)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartialWithRestoreToNewOperator(operators, input, revokeMemoryWhenAddingPages, factory, driverContextSupplier))
                .addAll(finishOperatorWithRestoreToNewOperator(operators, factory, driverContextSupplier))
                .build();
    }

    public static List<Page> toPagesCompareState(Operator operator, List<Page> input, boolean revokeMemoryWhenAddingPages, Map<String, Object> expectedMapping)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartialCompareState(operator, input, revokeMemoryWhenAddingPages, expectedMapping))
                .addAll(finishOperator(operator))
                .build();
    }

    public static List<Page> toPagesCompareStateSimple(Operator operator, List<Page> input, boolean revokeMemoryWhenAddingPages, Map<String, Object> expectedMapping)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartialCompareStateSimple(operator, input, revokeMemoryWhenAddingPages, expectedMapping))
                .addAll(finishOperator(operator))
                .build();
    }

    public static List<Page> toPagesCompareSelfStateSimple(Operator operator, List<Page> input, boolean revokeMemoryWhenAddingPages, Map<String, Object> expectedMapping)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartialCompareSelfStateSimple(operator, input, revokeMemoryWhenAddingPages, expectedMapping))
                .addAll(finishOperator(operator))
                .build();
    }

    public static List<Page> toPagesPartial(Operator operator, Iterator<Page> input)
    {
        return toPagesPartial(operator, input, true);
    }

    public static List<Page> toPagesPartial(Operator operator, Iterator<Page> input, boolean revokeMemory)
    {
        // verify initial state
        assertEquals(operator.isFinished(), false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        for (int loopsSinceLastPage = 0; loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            if (revokeMemory) {
                handleMemoryRevoking(operator);
            }

            if (input.hasNext() && operator.needsInput()) {
                operator.addInput(input.next());
                loopsSinceLastPage = 0;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }
        }

        return outputPages.build();
    }

    public static List<Page> toPagesPartialWithRestoreToNewOperator(List<Operator> operators, List<Page> input, boolean revokeMemory, OperatorFactory factory, Supplier<DriverContext> driverContextSupplier)
    {
        // verify initial state
        Operator operator = operators.get(0);
        assertEquals(operator.isFinished(), false);
        int inputIndex = 0;
        boolean captured = false;
        boolean restored = false;
        Object snapshot = null;

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        for (int loopsSinceLastPage = 0; loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            if (revokeMemory) {
                handleMemoryRevoking(operator);
            }

            if (inputIndex < input.size() && operator.needsInput()) {
                if (captured && !restored) {
                    checkArgument(snapshot != null);
                    operator = factory.createOperator(driverContextSupplier.get());
                    operators.add(operator);
                    operators.remove(0);
                    operator.restore(snapshot, operator.getOperatorContext().getDriverContext().getSerde());
                    restored = true;
                    checkArgument(operator.needsInput());
                }
                operator.addInput(input.get(inputIndex));
                inputIndex++;
                loopsSinceLastPage = 0;
            }

            Page outputPage = operator.getOutput();
            //Capturing after getOutput because we want the page in page buffer to be consumed and turned into state of the Operator
            if (!captured && inputIndex == input.size() / 2) {
                //Operator must be in state to need input for snapshot to occur
                checkArgument(operator.needsInput());
                snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
                captured = true;
            }
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }
        }

        return outputPages.build();
    }

    public static List<Page> toPagesPartialCompareStateSimple(Operator operator, List<Page> input, boolean revokeMemory, Map<String, Object> expectedMapping)
    {
        // verify initial state
        assertEquals(operator.isFinished(), false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        int inputIdx = 0;
        Object snapshot;

        for (int loopsSinceLastPage = 0; loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            if (revokeMemory) {
                handleMemoryRevoking(operator);
            }

            if (inputIdx < input.size() && operator.needsInput()) {
                operator.addInput(input.get(inputIdx));
                if (inputIdx == input.size() / 2) {
                    snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
                    operator.restore(snapshot, operator.getOperatorContext().getDriverContext().getSerde());
                    Map<String, Object> snapshotMapping = (Map<String, Object>) SnapshotTestUtil.toFullSnapshotMapping(snapshot);
                    assertEquals(snapshotMapping, expectedMapping);
                }
                inputIdx++;
                loopsSinceLastPage = 0;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }
        }

        return outputPages.build();
    }

    /**
     * This method differs from one above due to change in testing strategy. The old strategy compares full
     * mapping of operator's snapshot (by calling obejctToMap(snapshot)) to an expected full mapping.The new
     * approach is to only test snapshot mapping of the operator's own state (mostly primitive state fields,
     * by mapping using SnapshotTestUtil.toSimpleSnapShotMapping) and leave the reference state field (ie.
     * groupByHash state) to be tested in their own tests (ie. TestGroupByHash). This is to reduce dependency
     * in testing and makes it easier to modify Restorable classes.
     **/
    public static List<Page> toPagesPartialCompareSelfStateSimple(Operator operator, List<Page> input, boolean revokeMemory, Map<String, Object> expectedMapping)
    {
        // verify initial state
        assertEquals(operator.isFinished(), false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        int inputIdx = 0;
        Object snapshot;
        Map<String, Object> snapshotMapping = null;

        for (int loopsSinceLastPage = 0; loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            if (revokeMemory) {
                handleMemoryRevoking(operator);
            }

            if (inputIdx < input.size() && operator.needsInput()) {
                operator.addInput(input.get(inputIdx));
                if (inputIdx == input.size() / 2) {
                    snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
                    operator.restore(snapshot, operator.getOperatorContext().getDriverContext().getSerde());
                    snapshotMapping = SnapshotTestUtil.toSimpleSnapshotMapping(snapshot);
                    assertEquals(snapshotMapping, expectedMapping);
                }
                inputIdx++;
                loopsSinceLastPage = 0;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }
        }

        return outputPages.build();
    }

    public static List<Page> toPagesPartialCompareState(Operator operator, List<Page> input, boolean revokeMemory, Map<String, Object> expectedMapping)
    {
        // verify initial state
        assertEquals(operator.isFinished(), false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        List<Page> preSnapshotPages = new ArrayList<>();
        List<Page> postSnapshotPages = new ArrayList<>();
        int inputIdx = 0;
        Object snapshot = null;
        boolean restored = false;

        for (int loopsSinceLastPage = 0; loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            if (revokeMemory) {
                handleMemoryRevoking(operator);
            }

            if (inputIdx < input.size() && operator.needsInput()) {
                if (inputIdx == input.size() / 2) {
                    snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
                    Map<String, Object> snapshotMapping = (Map<String, Object>) SnapshotTestUtil.toFullSnapshotMapping(snapshot);
                    assertEquals(snapshotMapping, expectedMapping);
                }
                else if (inputIdx == input.size() - 1 && snapshot != null && !restored) {
                    operator.restore(snapshot, operator.getOperatorContext().getDriverContext().getSerde());
                    inputIdx = input.size() / 2;
                    restored = true;
                }
                operator.addInput(input.get(inputIdx));
                inputIdx++;
                loopsSinceLastPage = 0;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                if (inputIdx - 1 < input.size() / 2) {
                    preSnapshotPages.add(outputPage);
                }
                if (restored) {
                    postSnapshotPages.add(outputPage);
                }
                loopsSinceLastPage = 0;
            }
        }

        return outputPages.addAll(preSnapshotPages).addAll(postSnapshotPages).build();
    }

    public static List<Page> finishOperator(Operator operator)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        for (int loopsSinceLastPage = 0; !operator.isFinished() && loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            operator.finish();
            Page outputPage = operator.getOutput();
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }

            // revoke memory when output pages have started being produced
            handleMemoryRevoking(operator);
        }

        assertEquals(operator.isFinished(), true, "Operator did not finish");
        assertEquals(operator.needsInput(), false, "Operator still wants input");
        assertEquals(operator.isBlocked().isDone(), true, "Operator is blocked");

        return outputPages.build();
    }

    public static List<Page> finishOperatorWithRestoreToNewOperator(List<Operator> operators, OperatorFactory factory, Supplier<DriverContext> driverContextSupplier)
    {
        Operator operator = operators.get(0);
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        Object snapshot = null;
        boolean captured = false;
        boolean restored = false;

        for (int loopsSinceLastPage = 0; !operator.isFinished() && loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            if (captured && !restored) {
                operator = factory.createOperator(driverContextSupplier.get());
                operator.restore(snapshot, operator.getOperatorContext().getDriverContext().getSerde());
                restored = true;
            }

            operator.finish();
            Page outputPage = operator.getOutput();
            if (!captured) {
                snapshot = operator.capture(operator.getOperatorContext().getDriverContext().getSerde());
                captured = true;
            }
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }

            // revoke memory when output pages have started being produced
            handleMemoryRevoking(operator);
        }

        assertEquals(operator.isFinished(), true, "Operator did not finish");
        assertEquals(operator.needsInput(), false, "Operator still wants input");
        assertEquals(operator.isBlocked().isDone(), true, "Operator is blocked");

        return outputPages.build();
    }

    private static boolean handledBlocked(Operator operator)
    {
        ListenableFuture<?> isBlocked = operator.isBlocked();
        if (!isBlocked.isDone()) {
            tryGetFutureValue(isBlocked, 1, TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private static void handleMemoryRevoking(Operator operator)
    {
        if (operator.getOperatorContext().getReservedRevocableBytes() > 0) {
            getFutureValue(operator.startMemoryRevoke());
            operator.finishMemoryRevoke();
        }
    }

    public static List<Page> toPages(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input)
    {
        return toPages(operatorFactory, driverContext, input, true);
    }

    public static List<Page> toPages(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, boolean revokeMemoryWhenAddingPages)
    {
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operatorFactory.noMoreOperators(Lifespan.taskWide());
            return toPages(operator, input.iterator(), revokeMemoryWhenAddingPages);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static List<Page> toPagesWithRestoreToNewOperator(OperatorFactory operatorFactory, DriverContext driverContext, Supplier<DriverContext> driverContextSupplier, List<Page> input, boolean revokeMemoryWhenAddingPages)
    {
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operatorFactory.noMoreOperators(Lifespan.taskWide());
            //Since I'm restoring state onto a new Operator, in finishOperatorWithRestoreToNewOperator, the operator will still be the operator created right here
            //while we actual want the new operator that has been restored. So I passed the operator in the List to achieve this.
            List<Operator> operators = new ArrayList<>();
            operators.add(operator);
            return toPagesWithRestoreToNewOperator(operators, input, revokeMemoryWhenAddingPages, operatorFactory, driverContextSupplier);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static List<Page> toPages(OperatorFactory operatorFactory, DriverContext driverContext)
    {
        return toPages(operatorFactory, driverContext, ImmutableList.of());
    }

    public static List<Page> toPagesCompareState(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, boolean revokeMemoryWhenAddingPages, Map<String, Object> expectedMapping)
    {
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operatorFactory.noMoreOperators();
            return toPagesCompareState(operator, input, revokeMemoryWhenAddingPages, expectedMapping);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static List<Page> toPagesCompareStateSimple(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, boolean revokeMemoryWhenAddingPages, Map<String, Object> expectedMapping)
    {
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operatorFactory.noMoreOperators(driverContext.getLifespan());
            operatorFactory.noMoreOperators();
            return toPagesCompareStateSimple(operator, input, revokeMemoryWhenAddingPages, expectedMapping);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static List<Page> toPagesCompareSelfStateSimple(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, boolean revokeMemoryWhenAddingPages, Map<String, Object> expectedMapping)
    {
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operatorFactory.noMoreOperators();
            return toPagesCompareSelfStateSimple(operator, input, revokeMemoryWhenAddingPages, expectedMapping);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static MaterializedResult toMaterializedResult(Session session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            if (!(outputPage instanceof MarkerPage)) {
                resultBuilder.page(outputPage);
            }
        }
        return resultBuilder.build();
    }

    public static Block toRow(List<Type> parameterTypes, Object... values)
    {
        checkArgument(parameterTypes.size() == values.length, "parameterTypes.size(" + parameterTypes.size() + ") does not equal to values.length(" + values.length + ")");

        RowType rowType = RowType.anonymous(parameterTypes);
        BlockBuilder blockBuilder = new RowBlockBuilder(parameterTypes, null, 1);
        BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
        for (int i = 0; i < values.length; i++) {
            appendToBlockBuilder(parameterTypes.get(i), values[i], singleRowBlockWriter);
        }
        blockBuilder.closeEntry();
        return rowType.getObject(blockBuilder, 0);
    }

    public static void assertOperatorEqualsWithRestoreToNewOperator(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            Supplier<DriverContext> driverContextSupplier,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages)
    {
        assertOperatorEqualsWithRestoreToNewOperator(operatorFactory, driverContext, driverContextSupplier, input, expected, false, ImmutableList.of(), revokeMemoryWhenAddingPages);
    }

    public static void assertOperatorEqualsWithRestoreToNewOperator(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            Supplier<DriverContext> driverContextSupplier,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            boolean revokeMemoryWhenAddingPages)
    {
        List<Page> pages = toPagesWithRestoreToNewOperator(operatorFactory, driverContext, driverContextSupplier, input, revokeMemoryWhenAddingPages);
        if (hashEnabled && !hashChannels.isEmpty()) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, hashChannels);
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEquals(OperatorFactory operatorFactory, List<Type> types, DriverContext driverContext, List<Page> input, List<Page> expected)
    {
        List<Page> actual = toPages(operatorFactory, driverContext, input);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(types, actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, MaterializedResult expected)
    {
        assertOperatorEquals(operatorFactory, driverContext, input, expected, true);
    }

    public static void assertOperatorEquals(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages)
    {
        assertOperatorEquals(operatorFactory, driverContext, input, expected, false, ImmutableList.of(), revokeMemoryWhenAddingPages);
    }

    public static void assertOperatorEquals(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, MaterializedResult expected, boolean hashEnabled, List<Integer> hashChannels)
    {
        assertOperatorEquals(operatorFactory, driverContext, input, expected, hashEnabled, hashChannels, true);
    }

    public static void assertOperatorEquals(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            boolean revokeMemoryWhenAddingPages)
    {
        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        if (hashEnabled && !hashChannels.isEmpty()) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, hashChannels);
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEqualsWithStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithStateComparison(operatorFactory, driverContext, input, expected, true, expectedMapping);
    }

    public static void assertOperatorEqualsWithStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithStateComparison(operatorFactory, driverContext, input, expected, false, ImmutableList.of(), revokeMemoryWhenAddingPages, expectedMapping);
    }

    public static void assertOperatorEqualsWithStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithStateComparison(operatorFactory, driverContext, input, expected, hashEnabled, hashChannels, true, expectedMapping);
    }

    public static void assertOperatorEqualsWithStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        List<Page> pages = toPagesCompareState(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages, expectedMapping);
        if (hashEnabled && !hashChannels.isEmpty()) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, hashChannels);
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEqualsWithSimpleStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithSimpleStateComparison(operatorFactory, driverContext, input, expected, true, expectedMapping);
    }

    public static void assertOperatorEqualsWithSimpleStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithSimpleStateComparison(operatorFactory, driverContext, input, expected, false, ImmutableList.of(), revokeMemoryWhenAddingPages, expectedMapping);
    }

    public static void assertOperatorEqualsWithSimpleStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithSimpleStateComparison(operatorFactory, driverContext, input, expected, hashEnabled, hashChannels, true, expectedMapping);
    }

    public static void assertOperatorEqualsWithSimpleStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        List<Page> pages = toPagesCompareStateSimple(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages, expectedMapping);
        if (hashEnabled && !hashChannels.isEmpty()) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, hashChannels);
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEqualsWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, true, expectedMapping);
    }

    public static void assertOperatorEqualsWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, hashEnabled, hashChannels, true, expectedMapping);
    }

    public static void assertOperatorEqualsWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, false, ImmutableList.of(), revokeMemoryWhenAddingPages, expectedMapping);
    }

    public static void assertOperatorEqualsWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        List<Page> pages = toPagesCompareSelfStateSimple(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages, expectedMapping);
        if (hashEnabled && !hashChannels.isEmpty()) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, hashChannels);
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEqualsIgnoreOrderWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsIgnoreOrderWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, false, expectedMapping);
    }

    public static void assertOperatorEqualsIgnoreOrderWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsIgnoreOrderWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, false, Optional.empty(), revokeMemoryWhenAddingPages, expectedMapping);
    }

    public static void assertOperatorEqualsIgnoreOrderWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel,
            Map<String, Object> expectedMapping)
    {
        assertOperatorEqualsIgnoreOrderWithSimpleSelfStateComparison(operatorFactory, driverContext, input, expected, hashEnabled, hashChannel, true, expectedMapping);
    }

    public static void assertOperatorEqualsIgnoreOrderWithSimpleSelfStateComparison(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel,
            boolean revokeMemoryWhenAddingPages,
            Map<String, Object> expectedMapping)
    {
        assertPagesEqualIgnoreOrder(
                driverContext,
                toPagesCompareSelfStateSimple(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages, expectedMapping),
                expected,
                hashEnabled,
                hashChannel);
    }

    public static void assertOperatorEqualsIgnoreOrderWithRestoreToNewOperator(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            Supplier<DriverContext> driverContextSupplier,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages)
    {
        assertOperatorEqualsIgnoreOrderWithRestoreToNewOperator(operatorFactory, driverContext, driverContextSupplier, input, expected, false, Optional.empty(), revokeMemoryWhenAddingPages);
    }

    public static void assertOperatorEqualsIgnoreOrderWithRestoreToNewOperator(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            Supplier<DriverContext> driverContextSupplier,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel,
            boolean revokeMemoryWhenAddingPages)
    {
        assertPagesEqualIgnoreOrder(
                driverContext,
                toPagesWithRestoreToNewOperator(operatorFactory, driverContext, driverContextSupplier, input, revokeMemoryWhenAddingPages),
                expected,
                hashEnabled,
                hashChannel);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected)
    {
        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, false);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages)
    {
        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, false, Optional.empty(), revokeMemoryWhenAddingPages);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel)
    {
        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, hashEnabled, hashChannel, true);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel,
            boolean revokeMemoryWhenAddingPages)
    {
        assertPagesEqualIgnoreOrder(
                driverContext,
                toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages),
                expected,
                hashEnabled,
                hashChannel);
    }

    public static void assertPagesEqualIgnoreOrder(
            DriverContext driverContext,
            List<Page> inputActualPages,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel)
    {
        List<Page> actualPages = inputActualPages;
        if (hashEnabled && hashChannel.isPresent()) {
            // Drop the hashChannel for all pages
            actualPages = dropChannel(actualPages, ImmutableList.of(hashChannel.get()));
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), actualPages);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    public static void assertOperatorIsBlocked(Operator operator)
    {
        assertOperatorIsBlocked(operator, BLOCKED_DEFAULT_TIMEOUT);
    }

    public static void assertOperatorIsBlocked(Operator operator, Duration timeout)
    {
        if (waitForOperatorToUnblock(operator, timeout)) {
            fail("Operator is expected to be blocked for at least " + timeout.toString());
        }
    }

    public static void assertOperatorIsUnblocked(Operator operator)
    {
        assertOperatorIsUnblocked(operator, UNBLOCKED_DEFAULT_TIMEOUT);
    }

    public static void assertOperatorIsUnblocked(Operator operator, Duration timeout)
    {
        if (!waitForOperatorToUnblock(operator, timeout)) {
            fail("Operator is expected to be unblocked within " + timeout.toString());
        }
    }

    private static boolean waitForOperatorToUnblock(Operator operator, Duration timeout)
    {
        try {
            operator.isBlocked().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted", e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (TimeoutException expected) {
            return false;
        }
    }

    static <T> List<T> without(List<T> list, Collection<Integer> indexes)
    {
        Set<Integer> indexesSet = ImmutableSet.copyOf(indexes);

        return IntStream.range(0, list.size())
                .filter(index -> !indexesSet.contains(index))
                .mapToObj(list::get)
                .collect(toImmutableList());
    }

    static List<Page> dropChannel(List<Page> pages, List<Integer> channels)
    {
        List<Page> actualPages = new ArrayList<>();
        for (Page page : pages) {
            if (page instanceof MarkerPage) {
                continue;
            }
            int channel = 0;
            Block[] blocks = new Block[page.getChannelCount() - channels.size()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                if (channels.contains(i)) {
                    continue;
                }
                blocks[channel++] = page.getBlock(i);
            }
            actualPages.add(new Page(blocks));
        }
        return actualPages;
    }
}
