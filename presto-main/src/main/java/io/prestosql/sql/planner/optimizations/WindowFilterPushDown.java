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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.window.RankingFunction;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.isOptimizeTopNRankingNumber;
import static io.prestosql.spi.function.FunctionKind.WINDOW;
import static io.prestosql.spi.predicate.Marker.Bound.BELOW;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.planner.ExpressionDomainTranslator.ExtractionResult;
import static io.prestosql.sql.planner.ExpressionDomainTranslator.fromPredicate;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class WindowFilterPushDown
        implements PlanOptimizer
{
    private static final Signature ROW_NUMBER_SIGNATURE = new Signature("row_number", WINDOW, parseTypeSignature(StandardTypes.BIGINT), ImmutableList.of());

    private final Metadata metadata;
    private final ExpressionDomainTranslator domainTranslator;

    public WindowFilterPushDown(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.domainTranslator = new ExpressionDomainTranslator(new LiteralEncoder(metadata));
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(planSymbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, domainTranslator, session, types), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final ExpressionDomainTranslator domainTranslator;
        private final Session session;
        private final TypeProvider types;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, ExpressionDomainTranslator domainTranslator, Session session, TypeProvider types)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            checkState(node.getWindowFunctions().size() == 1, "WindowFilterPushdown requires that WindowNodes contain exactly one window function");
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            if (canReplaceWithRowNumber(node)) {
                return new RowNumberNode(idAllocator.getNextId(),
                        rewrittenSource,
                        node.getPartitionBy(),
                        getOnlyElement(node.getWindowFunctions().keySet()),
                        Optional.empty(),
                        Optional.empty());
            }
            return replaceChildren(node, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            if (node.isWithTies()) {
                return context.defaultRewrite(node);
            }

            // Operators can handle MAX_VALUE rows per page, so do not optimize if count is greater than this value
            if (node.getCount() > Integer.MAX_VALUE) {
                return context.defaultRewrite(node);
            }

            PlanNode source = context.rewrite(node.getSource());
            int limit = toIntExact(node.getCount());
            if (source instanceof RowNumberNode) {
                RowNumberNode rowNumberNode = mergeLimit(((RowNumberNode) source), limit);
                if (rowNumberNode.getPartitionBy().isEmpty()) {
                    return rowNumberNode;
                }
                source = rowNumberNode;
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source) && isOptimizeTopNRankingNumber(session)) {
                WindowNode windowNode = (WindowNode) source;
                //TODO: need optimizer this case `select ranking() over() from (select * from t1) limit 10`
                if (windowNode.getOrderingScheme().isPresent()) {
                    TopNRankingNumberNode topNRankingNumberNode = convertToTopNRankingFunction(windowNode, limit);
                    if (windowNode.getPartitionBy().isEmpty()) {
                        return topNRankingNumberNode;
                    }
                    source = topNRankingNumberNode;
                }
                else {
                    return context.defaultRewrite(node);
                }
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        @Override
        public PlanNode visitFilter(FilterNode filterNode, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(filterNode.getSource());
            TupleDomain<Symbol> tupleDomain = fromPredicate(metadata, session, castToExpression(filterNode.getPredicate()), types).getTupleDomain();

            if (source instanceof RowNumberNode) {
                Symbol rowNumberSymbol = ((RowNumberNode) source).getRowNumberSymbol();
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);

                if (upperBound.isPresent()) {
                    source = mergeLimit(((RowNumberNode) source), upperBound.getAsInt());
                    return rewriteFilterSource(filterNode, source, rowNumberSymbol, upperBound.getAsInt());
                }
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source) && isOptimizeTopNRankingNumber(session)) {
                WindowNode windowNode = (WindowNode) source;
                Symbol symbol = getOnlyElement(windowNode.getWindowFunctions().entrySet()).getKey();
                OptionalInt upperBound = extractUpperBound(tupleDomain, symbol);

                if (upperBound.isPresent()) {
                    source = convertToTopNRankingFunction(windowNode, upperBound.getAsInt());
                    return rewriteFilterSource(filterNode, source, symbol, upperBound.getAsInt());
                }
            }
            return replaceChildren(filterNode, ImmutableList.of(source));
        }

        private PlanNode rewriteFilterSource(FilterNode filterNode, PlanNode source, Symbol symbol, int upperBound)
        {
            ExtractionResult extractionResult = fromPredicate(metadata, session, castToExpression(filterNode.getPredicate()), types);
            TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();

            if (!isEqualRange(tupleDomain, symbol, upperBound)) {
                return new FilterNode(filterNode.getId(), source, filterNode.getPredicate());
            }

            // Remove the row number domain because it is absorbed into the node
            Map<Symbol, Domain> newDomains = tupleDomain.getDomains().get().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(symbol))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Construct a new predicate
            TupleDomain<Symbol> newTupleDomain = TupleDomain.withColumnDomains(newDomains);
            Expression newPredicate = ExpressionUtils.combineConjuncts(
                    extractionResult.getRemainingExpression(),
                    domainTranslator.toPredicate(newTupleDomain));

            if (newPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                return source;
            }
            return new FilterNode(filterNode.getId(), source, castToRowExpression(newPredicate));
        }

        private static boolean isEqualRange(TupleDomain<Symbol> tupleDomain, Symbol symbol, long upperBound)
        {
            if (tupleDomain.isNone()) {
                return false;
            }
            Domain domain = tupleDomain.getDomains().get().get(symbol);
            return domain.getValues().equals(ValueSet.ofRanges(Range.lessThanOrEqual(domain.getType(), upperBound)));
        }

        private static OptionalInt extractUpperBound(TupleDomain<Symbol> tupleDomain, Symbol symbol)
        {
            if (tupleDomain.isNone()) {
                return OptionalInt.empty();
            }

            Domain rowNumberDomain = tupleDomain.getDomains().get().get(symbol);
            if (rowNumberDomain == null) {
                return OptionalInt.empty();
            }
            ValueSet values = rowNumberDomain.getValues();
            if (values.isAll() || values.isNone() || values.getRanges().getRangeCount() <= 0) {
                return OptionalInt.empty();
            }

            Range span = values.getRanges().getSpan();

            if (span.getHigh().isUpperUnbounded()) {
                return OptionalInt.empty();
            }

            verify(rowNumberDomain.getType().equals(BIGINT));
            long upperBound = (Long) span.getHigh().getValue();
            if (span.getHigh().getBound() == BELOW) {
                upperBound--;
            }

            if (upperBound > 0 && upperBound <= Integer.MAX_VALUE) {
                return OptionalInt.of(toIntExact(upperBound));
            }
            return OptionalInt.empty();
        }

        private static RowNumberNode mergeLimit(RowNumberNode node, int newRowCountPerPartition)
        {
            if (node.getMaxRowCountPerPartition().isPresent()) {
                newRowCountPerPartition = Math.min(node.getMaxRowCountPerPartition().get(), newRowCountPerPartition);
            }
            return new RowNumberNode(node.getId(), node.getSource(), node.getPartitionBy(), node.getRowNumberSymbol(), Optional.of(newRowCountPerPartition), node.getHashSymbol());
        }

        private TopNRankingNumberNode convertToTopNRankingFunction(WindowNode windowNode, int limit)
        {
            return new TopNRankingNumberNode(idAllocator.getNextId(),
                    windowNode.getSource(),
                    windowNode.getSpecification(),
                    getOnlyElement(windowNode.getWindowFunctions().keySet()),
                    limit,
                    false,
                    Optional.empty(),
                    getRankingFunction(windowNode));
        }

        private static boolean canReplaceWithRowNumber(WindowNode node)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }

            Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            return node.getWindowFunctions().get(rowNumberSymbol).getSignature().equals(RankingFunction.ROW_NUMBER.getValue()) &&
                    !node.getOrderingScheme().isPresent();
        }

        private static boolean canOptimizeWindowFunction(WindowNode node)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            Symbol symbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            return canOptimizeRankingFunctionSignature(node.getWindowFunctions().get(symbol).getSignature());
        }

        private static boolean canOptimizeRankingFunctionSignature(Signature signature)
        {
            return signature.equals(RankingFunction.ROW_NUMBER.getValue()) ||
                    signature.equals(RankingFunction.RANK.getValue()) ||
                    signature.equals(RankingFunction.DENSE_RANK.getValue());
        }

        private static Optional<RankingFunction> getRankingFunction(WindowNode node)
        {
            Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            Signature signature = node.getWindowFunctions().get(rowNumberSymbol).getSignature();
            if (signature.equals(RankingFunction.ROW_NUMBER.getValue())) {
                return Optional.of(RankingFunction.ROW_NUMBER);
            }
            else if (signature.equals(RankingFunction.RANK.getValue())) {
                return Optional.of(RankingFunction.RANK);
            }
            else if (signature.equals(RankingFunction.DENSE_RANK.getValue())) {
                return Optional.of(RankingFunction.DENSE_RANK);
            }
            else {
                return Optional.empty();
            }
        }
    }
}
