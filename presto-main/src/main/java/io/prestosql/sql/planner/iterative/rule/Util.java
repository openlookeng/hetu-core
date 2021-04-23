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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.relational.OriginalExpressionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

class Util
{
    private Util()
    {
    }

    /**
     * Prune the set of available inputs to those required by the given expressions.
     * <p>
     * If all inputs are used, return Optional.empty() to indicate that no pruning is necessary.
     */
    public static Optional<Set<Symbol>> pruneInputs(Collection<Symbol> availableInputs, Collection<RowExpression> expressions)
    {
        Set<Symbol> availableInputsSet = ImmutableSet.copyOf(availableInputs);
        Set<Symbol> referencedInputs;
        if (expressions.stream().allMatch(OriginalExpressionUtils::isExpression)) {
            referencedInputs = SymbolsExtractor.extractUnique(
                    expressions.stream().map(OriginalExpressionUtils::castToExpression).collect(toImmutableList()));
        }
        else if (expressions.stream().noneMatch(OriginalExpressionUtils::isExpression)) {
            referencedInputs = SymbolsExtractor.extractUnique(expressions, null);
        }
        else {
            throw new IllegalStateException(format("Expression %s contains mixed Expression and RowExpression", expressions));
        }
        Set<Symbol> prunedInputs;
        prunedInputs = Sets.filter(availableInputsSet, referencedInputs::contains);

        if (prunedInputs.size() == availableInputsSet.size()) {
            return Optional.empty();
        }

        return Optional.of(prunedInputs);
    }

    /**
     * Transforms a plan like P->C->X to C->P->X
     */
    public static PlanNode transpose(PlanNode parent, PlanNode child)
    {
        return child.replaceChildren(ImmutableList.of(
                parent.replaceChildren(
                        child.getSources())));
    }

    /**
     * @return If the node has outputs not in permittedOutputs, returns an identity projection containing only those node outputs also in permittedOutputs.
     */
    public static Optional<PlanNode> restrictOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<Symbol> permittedOutputs, boolean useRowExpression, TypeProvider typeProvider)
    {
        List<Symbol> restrictedOutputs = node.getOutputSymbols().stream()
                .filter(permittedOutputs::contains)
                .collect(toImmutableList());

        if (restrictedOutputs.size() == node.getOutputSymbols().size()) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectNode(
                        idAllocator.getNextId(),
                        node,
                        useRowExpression ? AssignmentUtils.identityAssignments(typeProvider, restrictedOutputs) : AssignmentUtils.identityAsSymbolReferences(restrictedOutputs)));
    }

    /**
     * @return The original node, with identity projections possibly inserted between node and each child, limiting the columns to those permitted.
     * Returns a present Optional iff at least one child was rewritten.
     */
    @SafeVarargs
    public static Optional<PlanNode> restrictChildOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<Symbol>... permittedChildOutputsArgs)
    {
        List<Set<Symbol>> permittedChildOutputs = ImmutableList.copyOf(permittedChildOutputsArgs);

        checkArgument(
                (node.getSources().size() == permittedChildOutputs.size()),
                "Mismatched child (%d) and permitted outputs (%d) sizes",
                node.getSources().size(),
                permittedChildOutputs.size());

        ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
        boolean rewroteChildren = false;

        for (int i = 0; i < node.getSources().size(); ++i) {
            PlanNode oldChild = node.getSources().get(i);
            Optional<PlanNode> newChild = restrictOutputs(idAllocator, oldChild, permittedChildOutputs.get(i), false, null);
            rewroteChildren |= newChild.isPresent();
            newChildrenBuilder.add(newChild.orElse(oldChild));
        }

        if (!rewroteChildren) {
            return Optional.empty();
        }
        return Optional.of(node.replaceChildren(newChildrenBuilder.build()));
    }
}
