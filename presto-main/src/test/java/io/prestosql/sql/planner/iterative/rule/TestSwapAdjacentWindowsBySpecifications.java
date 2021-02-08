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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.sql.planner.assertions.ExpectedValueProvider;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.Window;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.sql.expression.Types.FrameBoundType.CURRENT_ROW;
import static io.prestosql.spi.sql.expression.Types.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.specification;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.window;

public class TestSwapAdjacentWindowsBySpecifications
        extends BaseRuleTest
{
    private WindowNode.Frame frame;
    private Signature signature;

    public TestSwapAdjacentWindowsBySpecifications()
    {
        frame = new WindowNode.Frame(
                Types.WindowFrameType.RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        signature = new Signature(
                "avg",
                FunctionKind.WINDOW,
                ImmutableList.of(),
                ImmutableList.of(),
                DOUBLE.getTypeSignature(),
                ImmutableList.of(BIGINT.getTypeSignature()),
                false);
    }

    @Test
    public void doesNotFireOnPlanWithoutWindowFunctions()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnPlanWithSingleWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.window(new WindowNode.Specification(
                                ImmutableList.of(p.symbol("a")),
                                Optional.empty()),
                        ImmutableMap.of(p.symbol("avg_1"),
                                new WindowNode.Function(signature, ImmutableList.of(), frame)),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void subsetComesFirst()
    {
        String columnAAlias = "ALIAS_A";
        String columnBAlias = "ALIAS_B";

        ExpectedValueProvider<WindowNode.Specification> specificationA = specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());
        ExpectedValueProvider<WindowNode.Specification> specificationAB = specification(ImmutableList.of(columnAAlias, columnBAlias), ImmutableList.of(), ImmutableMap.of());

        Optional<Window> windowAB = Optional.of(new Window(ImmutableList.of(new SymbolReference("a"), new SymbolReference("b")), Optional.empty(), Optional.empty()));
        Optional<Window> windowA = Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new WindowNode.Specification(
                                        ImmutableList.of(p.symbol("a")),
                                        Optional.empty()),
                                ImmutableMap.of(p.symbol("avg_1", DOUBLE),
                                        new WindowNode.Function(signature, ImmutableList.of(p.variable("a")), frame)),
                                p.window(new WindowNode.Specification(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                Optional.empty()),
                                        ImmutableMap.of(p.symbol("avg_2", DOUBLE),
                                                new WindowNode.Function(signature, ImmutableList.of(p.variable("b")), frame)),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationAB)
                                        .addFunction(functionCall("avg", Optional.empty(), ImmutableList.of(columnBAlias))),
                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(specificationA)
                                                .addFunction(functionCall("avg", Optional.empty(), ImmutableList.of(columnAAlias))),
                                        values(ImmutableMap.of(columnAAlias, 0, columnBAlias, 1)))));
    }

    @Test
    public void dependentWindowsAreNotReordered()
    {
        Optional<Window> windowA = Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new WindowNode.Specification(
                                        ImmutableList.of(p.symbol("a")),
                                        Optional.empty()),
                                ImmutableMap.of(p.symbol("avg_1"),
                                        new WindowNode.Function(signature, ImmutableList.of(p.variable("avg_2")), frame)),
                                p.window(new WindowNode.Specification(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                Optional.empty()),
                                        ImmutableMap.of(p.symbol("avg_2"),
                                                new WindowNode.Function(signature, ImmutableList.of(p.variable("a")), frame)),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .doesNotFire();
    }
}
