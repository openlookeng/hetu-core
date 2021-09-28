/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.utils;

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.SimplePlanVisitor;
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.rule.HintedReorderJoins;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.ReorderJoins;
import io.prestosql.sql.planner.iterative.rule.RowExpressionRewriteRuleSet;
import io.prestosql.sql.planner.optimizations.ApplyConnectorOptimization;
import io.prestosql.sql.planner.optimizations.LimitPushDown;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.SystemSessionProperties.getJoinReorderingStrategy;
import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_LEGACY_AND_ROWEXPR;
import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_LEGACY_AND_ROWEXPR_PUSH_PREDICATE;
import static io.prestosql.spi.plan.PlanNode.SkipOptRuleLevel.APPLY_ALL_RULES;
import static java.util.Objects.requireNonNull;

public class OptimizerUtils
{
    private static final Map<String, Set<String>> planOptimizerBlacklist = new ConcurrentHashMap<>();
    private static final ThreadLocal<Map<String, Set<String>>> threadLocal = new ThreadLocal<>();
    private static final Logger log = Logger.get(OptimizerUtils.class);

    private OptimizerUtils()
    {
    }

    public static boolean isEnabledLegacy(PlanOptimizer optimizer, Session session)
    {
        if (optimizer instanceof ApplyConnectorOptimization) {
            return SystemSessionProperties.isQueryPushDown(session);
        }
        if (optimizer instanceof LimitPushDown) {
            return SystemSessionProperties.isLimitPushDown(session);
        }
        if (optimizer instanceof HintedReorderJoins) {
            // Use the community ReorderJoins
            if (getJoinReorderingStrategy(session) != FeaturesConfig.JoinReorderingStrategy.NONE) {
                return false;
            }
            String joinOrder = SystemSessionProperties.getJoinOrder(session);
            return joinOrder != null && !"".equals(joinOrder);
        }
        return true;
    }

    public static boolean isEnabledRule(Rule<?> rule, Session session)
    {
        if (rule instanceof PushLimitThroughUnion) {
            return SystemSessionProperties.isPushLimitThroughUnion(session);
        }
        if (rule instanceof PushLimitThroughSemiJoin) {
            return SystemSessionProperties.isPushLimitThroughSemiJoin(session);
        }
        if (rule instanceof PushLimitThroughOuterJoin) {
            return SystemSessionProperties.isPushLimitThroughOuterJoin(session);
        }
        if (rule instanceof ReorderJoins) {
            // Use Hetu HintedReorderJoins
            String joinOrder = SystemSessionProperties.getJoinOrder(session);
            return joinOrder == null || "".equals(joinOrder);
        }
        return true;
    }

    public static boolean isEnabledLegacy(PlanOptimizer optimizer, Session session, PlanNode node)
    {
        if (optimizer instanceof IterativeOptimizer) {
            for (Rule<?> rule : ((IterativeOptimizer) optimizer).getRules()) {
                if (isEnabledRule(rule, session, node)) {
                    return true;
                }
            }
            // None of the rules are enabled
            return false;
        }
        return isEnabledLegacy(optimizer, session);
    }

    public static boolean isEnabledRule(Rule<?> rule, Session session, PlanNode node)
    {
        if (rule instanceof ReorderJoins) {
            // Use Hetu HintedReorderJoins
            String joinOrder = SystemSessionProperties.getJoinOrder(session);
            int threshold = SystemSessionProperties.getSkipReorderingThreshold(session);
            return threshold > 0 && (joinOrder == null || "".equals(joinOrder)) && !containsJoinNodesMoreThan(node, threshold);
        }
        return true;
    }

    private static boolean containsJoinNodesMoreThan(PlanNode node, int maxLimit)
    {
        JoinNodeCounter counter = new JoinNodeCounter(maxLimit);
        node.accept(counter, null);
        return counter.isMaxCountReached();
    }

    public static boolean canApplyOptimizer(PlanOptimizer optimizer, PlanNode.SkipOptRuleLevel optimizationLevel)
    {
        if (optimizationLevel == APPLY_ALL_RULES) {
            return true;
        }

        // If it is IterativeOptimizer, then only rule as per level selected can be applied.
        return !(optimizer instanceof IterativeOptimizer)
                || (((optimizationLevel != APPLY_ALL_LEGACY_AND_ROWEXPR
                || ((IterativeOptimizer) optimizer).getRules().stream().findFirst().get() instanceof RowExpressionRewriteRuleSet.ValuesRowExpressionRewrite))
                && (optimizationLevel != APPLY_ALL_LEGACY_AND_ROWEXPR_PUSH_PREDICATE
                || ((IterativeOptimizer) optimizer).getRules().stream().findFirst().get() instanceof RowExpressionRewriteRuleSet.ValuesRowExpressionRewrite
                || ((IterativeOptimizer) optimizer).getRules().stream().findFirst().get() instanceof PushPredicateIntoTableScan));
    }

    //cutomer config connector optimizer
    private static Set<String> getPlanNodeCatalogs(PlanNode node)
    {
        Map<String, Set<String>> nodeCatalogMap = threadLocal.get();
        if (nodeCatalogMap == null || nodeCatalogMap.get(node.getId().toString()) == null) {
            if (nodeCatalogMap == null) {
                nodeCatalogMap = new HashMap<>();
            }
            Set<String> nodeCatalogs = new HashSet<>();
            findCatalogName(node, nodeCatalogs);
            nodeCatalogMap.putIfAbsent(node.getId().toString(), nodeCatalogs);
            threadLocal.set(nodeCatalogMap);
        }
        return nodeCatalogMap.get(node.getId().toString());
    }

    //cutomer config connector optimizer
    public static void addPlanOptimizerBlacklist(String catalogName, Map<String, String> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        String blackList = properties.get(HetuConstant.CONNECTOR_PLANOPTIMIZER_RULE_BLACKLIST);
        if (StringUtils.isNoneBlank(blackList)) {
            Set<String> tmpSet = new HashSet(Arrays.asList(blackList.split(",")));
            planOptimizerBlacklist.putIfAbsent(catalogName, tmpSet);
        }
    }

    //cutomer config connector optimizer
    public static void findCatalogName(PlanNode planNode, Set<String> catalogNames)
    {
        try {
            if (planNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) planNode;
                String catalogName = tableScanNode.getTable().getCatalogName().getCatalogName();
                catalogNames.add(catalogName);
            }
            List<PlanNode> sources = planNode.getSources();
            if (sources != null && sources.size() > 0) {
                for (PlanNode source : sources) {
                    findCatalogName(source, catalogNames);
                }
            }
        }
        catch (Exception e) {
            log.warn(e, "findCatalogName failed");
        }
    }

    private static class JoinNodeCounter
            extends SimplePlanVisitor<Void>
    {
        private final int maxLimit;
        private int count;

        JoinNodeCounter(int maxLimit)
        {
            this.maxLimit = maxLimit;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            count++;
            if (count >= maxLimit) {
                // Break once reached the maximum count
                return null;
            }
            return super.visitJoin(node, context);
        }

        public boolean isMaxCountReached()
        {
            return count >= maxLimit;
        }
    }
}
