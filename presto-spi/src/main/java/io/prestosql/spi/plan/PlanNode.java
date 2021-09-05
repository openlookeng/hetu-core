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
package io.prestosql.spi.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_RULES;
import static java.util.Objects.requireNonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, property = "@type")
public abstract class PlanNode
{
    public enum SkipOptRuleLevel
    {
        APPLY_ALL_RULES,                // default behaviour
        APPLY_ALL_LEGACY_AND_ROWEXPR,    // applies all legacy rule and only RowExpressionRewriteRuleSet iterative rule
        APPLY_ALL_LEGACY_AND_ROWEXPR_PUSH_PREDICATE     // APPLY_ALL_LEGACY_AND_ROWEXPR + PushPredicateIntoTableScan
    }

    private final PlanNodeId id;
    private SkipOptRuleLevel skipOptRuleLevel;

    protected PlanNode(PlanNodeId id)
    {
        requireNonNull(id, "id is null");
        this.id = id;
        skipOptRuleLevel = APPLY_ALL_RULES;
    }

    @JsonProperty("id")
    public PlanNodeId getId()
    {
        return id;
    }

    public abstract List<PlanNode> getSources();

    public abstract List<Symbol> getOutputSymbols();

    public Collection<Symbol> getInputSymbols()
    {
        return ImmutableList.of();
    }

    public List<Symbol> getAllSymbols()
    {
        return Streams.concat(getInputSymbols().stream(), getOutputSymbols().stream())
                .distinct()
                .collect(Collectors.toList());
    }

    public abstract PlanNode replaceChildren(List<PlanNode> newChildren);

    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }

    public void setSkipOptRuleLevel(SkipOptRuleLevel skipOptRuleLevel)
    {
        this.skipOptRuleLevel = skipOptRuleLevel;
    }

    public SkipOptRuleLevel getSkipOptRuleLevel()
    {
        return this.skipOptRuleLevel;
    }
}
