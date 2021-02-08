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
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.plan.RowNumberNode;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RowNumberSymbolMatcher
        implements RvalueMatcher
{
    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof RowNumberNode)) {
            return Optional.empty();
        }

        RowNumberNode rowNumberNode = (RowNumberNode) node;

        return Optional.of(rowNumberNode.getRowNumberSymbol());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .toString();
    }
}
