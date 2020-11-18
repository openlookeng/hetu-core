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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.plan.WindowNode.Function;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public class WindowFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<FunctionCall> callMaker;
    private final Optional<FunctionHandle> functionHandle;
    private final Optional<ExpectedValueProvider<WindowNode.Frame>> frameMaker;

    /**
     * @param callMaker Always validates the function call
     * @param functionHandle Optionally validates the signature
     * @param frameMaker Optionally validates the frame
     */
    public WindowFunctionMatcher(
            ExpectedValueProvider<FunctionCall> callMaker,
            Optional<FunctionHandle> functionHandle,
            Optional<ExpectedValueProvider<WindowNode.Frame>> frameMaker)
    {
        this.callMaker = requireNonNull(callMaker, "functionCall is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.frameMaker = requireNonNull(frameMaker, "frameMaker is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        if (!(node instanceof WindowNode)) {
            return result;
        }

        WindowNode windowNode = (WindowNode) node;

        FunctionCall expectedCall = callMaker.getExpectedValue(symbolAliases);
        Optional<WindowNode.Frame> expectedFrame = frameMaker.map(maker -> maker.getExpectedValue(symbolAliases));

        for (Map.Entry<Symbol, Function> assignment : windowNode.getWindowFunctions().entrySet()) {
            Function function = assignment.getValue();
            boolean signatureMatches = functionHandle.map(assignment.getValue().getFunctionHandle()::equals).orElse(true);
            if (signatureMatches && windowFunctionMatches(function, expectedCall, expectedFrame, metadata)) {
                checkState(!result.isPresent(), "Ambiguous function calls in %s", windowNode);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private boolean windowFunctionMatches(Function windowFunction, FunctionCall expectedCall, Optional<WindowNode.Frame> expectedFrame, Metadata metadata)
    {
        if (expectedCall.getWindow().isPresent()) {
            return false;
        }
        if (!functionHandle.map(windowFunction.getFunctionHandle()::equals).orElse(true) ||
                !expectedFrame.map(windowFunction.getFrame()::equals).orElse(true) ||
                !Objects.equals(expectedCall.getName(), QualifiedName.of(metadata.getFunctionAndTypeManager().getFunctionMetadata(windowFunction.getFunctionHandle()).getName().getObjectName())) ||
                expectedCall.getArguments().size() != windowFunction.getArguments().size()) {
            return false;
        }
        for (int i = 0; i < windowFunction.getArguments().size(); i++) {
            if (isExpression(windowFunction.getArguments().get(i))) {
                if (!Objects.equals(expectedCall.getArguments().get(i), castToExpression(windowFunction.getArguments().get(i)))) {
                    return false;
                }
            }
            else {
                if (windowFunction.getArguments().get(i) instanceof VariableReferenceExpression && expectedCall.getArguments().get(i) instanceof SymbolReference) {
                    if (((SymbolReference) expectedCall.getArguments().get(i)).getName() != ((VariableReferenceExpression) windowFunction.getArguments().get(i)).getName()) {
                        return false;
                    }
                }
                else {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        // Only include fields in the description if they are actual constraints.
        return toStringHelper(this)
                .omitNullValues()
                .add("callMaker", callMaker)
                .add("functionHandle", functionHandle.orElse(null))
                .add("frameMaker", frameMaker.orElse(null))
                .toString();
    }
}
