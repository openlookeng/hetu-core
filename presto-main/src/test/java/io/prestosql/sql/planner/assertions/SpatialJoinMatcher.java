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
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode.Type;
import io.prestosql.sql.tree.Expression;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public class SpatialJoinMatcher
        implements Matcher
{
    private final Type type;
    private final Expression filter;
    private final Optional<String> kdbTree;

    public SpatialJoinMatcher(Type type, Expression filter, Optional<String> kdbTree)
    {
        this.type = type;
        this.filter = requireNonNull(filter, "filter can not be null");
        this.kdbTree = requireNonNull(kdbTree, "kdbTree can not be null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof SpatialJoinNode)) {
            return false;
        }

        SpatialJoinNode joinNode = (SpatialJoinNode) node;
        return joinNode.getType() == type;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        SpatialJoinNode joinNode = (SpatialJoinNode) node;
        if (isExpression(joinNode.getFilter())) {
            if (!new ExpressionVerifier(symbolAliases).process(castToExpression(joinNode.getFilter()), filter)) {
                return NO_MATCH;
            }
        }
        else {
            if (!new RowExpressionVerifier(symbolAliases, metadata, session, joinNode.getOutputSymbols()).process(filter, joinNode.getFilter())) {
                return NO_MATCH;
            }
        }
        if (!joinNode.getKdbTree().equals(kdbTree)) {
            return NO_MATCH;
        }
        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("filter", filter)
                .toString();
    }
}
