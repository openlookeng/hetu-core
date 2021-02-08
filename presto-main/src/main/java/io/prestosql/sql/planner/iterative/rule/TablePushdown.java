/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.IndexSourceNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.shouldEnableTablePushdown;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.util.SpecialCommentFormatter.getUniqueColumnTableMap;
import static java.util.Objects.requireNonNull;

public class TablePushdown
        implements Rule<JoinNode>
{
    private static final Logger LOG = Logger.get(TablePushdown.class);
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> joinNode.getType() == JoinNode.Type.INNER);
    private static Map<String, String[]> uniqueColumnsPerTable;
    private static Stack<NodeWithTreeDirection> outerTablePathStack = new Stack<>();
    private static Stack<NodeWithTreeDirection> innerTablePathStack = new Stack<>();
    private static DIRECTION outerTableDirection;
    private static DIRECTION innerTableDirection;
    private final Metadata metadata;
    private Context ruleContext;
    private final String[] joinCriteriaStrings = new String[2];

    public TablePushdown(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldEnableTablePushdown(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        uniqueColumnsPerTable = getUniqueColumnTableMap();
        ruleContext = context;
        Lookup lookup = ruleContext.getLookup();
        if (verifyJoinConditions(node, lookup)) {
            LOG.info("Table Pushdown preconditions satisfied.");
            PlanNode rewrittenNode = planRewriter(node, lookup);
            clearAllDataStructures();
            return Result.ofPlanNode(rewrittenNode);
        }
        return Result.empty();
    }

    /**
     * @param node captured by the rule
     * @param lookup captured from the context
     * @return boolean signifying if the preconditions are satisfied
     * Conditions are evaluated in increasing order of complexity
     * As soon as any one fails, it will return false,
     * without having to check the next one.
     * We need to create two stacks with the nodes in the plan tree for all pre-condition checks.
     * The process of stack creation itself verifies if the join columns are unique for the outer table.
     */
    private boolean verifyJoinConditions(JoinNode node, Lookup lookup)
    {
        /* Extract the column names which are involved in the Join criteria
        * Column names may have a trailing _x (x is a number) which has to be removed.
        * NOTE: We assume only one join criteria is present
        * */
        List<JoinNode.EquiJoinClause> joinCriteria = node.getCriteria();

        if (joinCriteria.isEmpty()) {
            return false;
        }

        joinCriteriaStrings[0] = joinCriteria.get(0).getLeft().toString();
        joinCriteriaStrings[0] = joinCriteriaStrings[0].replaceAll("_\\p{Digit}+", "");
        joinCriteriaStrings[1] = joinCriteria.get(0).getRight().toString();
        joinCriteriaStrings[1] = joinCriteriaStrings[1].replaceAll("_\\p{Digit}+", "");

        PlanNode leftChildOfJoin = resolveNodeFromGroupReference(node, 0, lookup);
        PlanNode rightChildOfJoin = resolveNodeFromGroupReference(node, 1, lookup);

        Stack<NodeWithTreeDirection> leftDirStack = new Stack<>();
        Stack<NodeWithTreeDirection> rightDirStack = new Stack<>();

        /*
        * updateStack() method also checks if the table has unique columns or not.
        * */
        boolean leftIsOuter = updateStack(leftChildOfJoin, lookup, leftDirStack);
        boolean rightIsOuter = updateStack(rightChildOfJoin, lookup, rightDirStack);

        if (leftIsOuter) {
            // the outer table is present in the left subtree of the current join node.
            outerTablePathStack = leftDirStack;
            innerTablePathStack = rightDirStack;
            outerTableDirection = DIRECTION.LEFT;
            innerTableDirection = DIRECTION.RIGHT;
        }
        else if (rightIsOuter) {
            // the outer table is present in the right subtree of the current join node.
            outerTablePathStack = rightDirStack;
            innerTablePathStack = leftDirStack;
            outerTableDirection = DIRECTION.RIGHT;
            innerTableDirection = DIRECTION.LEFT;
        }
        else {
            /*
             * If neither of the sources of the current join node have the outer table in their subtrees,
             * immediately return false as the table which the user wants to join on, might not be present.
             * For example, the join column may not be unique in any table.
             * */
            return false;
        }

        /*
        * Given that the tables have been identified-
        * verify that only one subtree has a JoinNode and only the subquery's subtree has a GroupBy clause
        * */
        return verifyPresenceOfJoinsAndGroupBy();
    }

    /**
     * Recursive method to create a traversal path in the PlanTree from the input JoinNode
     * to the TableScanNode at the leaf.
     * Inner Table cannot have a JoinNode on the path anyway, so final returned result can also check which is inner
     * and which is outer.
     * @param node which is to be pushed into the stack and evaluated
     * @param lookup from the context, used to resolve GroupReference
     * @param stack to be updated with the path followed from Original JoinNode to TableScanNode
     * @return boolean informing if the outer table is found or not.
     */
    private boolean updateStack(PlanNode node, Lookup lookup, Stack<NodeWithTreeDirection> stack)
    {
        /*
         * Updates the stack with the path followed from i/p join node to leaf TableScanNode.
         * */
        stack.push(new NodeWithTreeDirection(node, DIRECTION.LEFT));
        if (!(node instanceof TableScanNode)) {
            if (node instanceof JoinNode) {
                if (updateStack(resolveNodeFromGroupReference(node, 0, lookup), lookup, stack)) {
                    return true;
                }
                else {
                    while (!(stack.peek().getNode() instanceof JoinNode)) {
                        stack.pop();
                    }
                    NodeWithTreeDirection tempNode = stack.pop();
                    stack.push(new NodeWithTreeDirection(tempNode.getNode(), DIRECTION.RIGHT));
                    return updateStack(resolveNodeFromGroupReference(node, 1, lookup), lookup, stack);
                }
            }
            else if (node instanceof ValuesNode || node instanceof IndexSourceNode) {
                /*
                 * These two type of nodes can be leaf nodes of the PlanTree but need not be evaluated
                 * Thus a false is returned to calling method
                 * */
                return false;
            }
            else {
                return updateStack(resolveNodeFromGroupReference(node, 0, lookup), lookup, stack);
            }
        }

        /*
         * This method checks if the outer table relevant to this Optimization rule is captured on the path or not
         * */
        return isTableWithUniqueColumns((TableScanNode) node);
    }

    /**
     * Verify if the Join is performed on a unique column
     * @param tableNode captured by the rule
     * @return boolean value denoting if condition is satisfied
     */
    private boolean isTableWithUniqueColumns(TableScanNode tableNode)
    {
        TableHandle tableHandle = tableNode.getTable();
        TableStatistics tableStatistics = metadata.getTableStatistics(ruleContext.getSession(), tableHandle, Constraint.alwaysTrue());

        /*
         * We check here if tablestats is null or not.
         * If not null then check if the stats match as per requirement.
         * */
        if (tableStatistics != null && isTableWithUniqueColumnTableStatistics(tableStatistics, tableHandle)) {
            return true;
        }
        else {
            /*
             * if table statistics are not available or due to approx stats doesn't match, check if user hint is available.
             * If not, return false as nothing can be tested at this point.
             * If user hint is available, check-
             * If table name of the current table doesn't match with user hint, then return false;
             * If it matches, then check if the user supplied column name is present in the join criteria. If it does, return true;
             * Else, return false.
             * */
            if (uniqueColumnsPerTable.isEmpty()) {
                return false;
            }
            else {
                return isTableWithUniqueColumnsUserHint(tableNode);
            }
        }
    }

    /**
     * @param tableStatistics for the current table being parsed in the plan tree.
     * @param tableHandle for the TableScanNode currently being evaluated in the plan tree.
     * @return if the table satisfies the unique column requirement
     */
    private boolean isTableWithUniqueColumnTableStatistics(TableStatistics tableStatistics, TableHandle tableHandle)
    {
        boolean joinColumnExists = false;
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(ruleContext.getSession(), tableHandle);
        ColumnStatistics columnStatistics = null;

        if (columnHandles.containsKey(joinCriteriaStrings[0])) {
            columnStatistics = tableStatistics.getColumnStatistics().get(columnHandles.get(joinCriteriaStrings[0]));
            joinColumnExists = true;
        }
        else if (columnHandles.containsKey(joinCriteriaStrings[1])) {
            columnStatistics = tableStatistics.getColumnStatistics().get(columnHandles.get(joinCriteriaStrings[1]));
            joinColumnExists = true;
        }

        if (!joinColumnExists) {
            return false;
        }
        else {
            requireNonNull(columnStatistics, "Column Statistics cannot be null if the column exists for the table");
            return tableStatistics.getRowCount().getValue() == columnStatistics.getDistinctValuesCount().getValue();
        }
    }

    /**
     * @param node the current TableScanNode being evaluated in the plan tree
     * @return if the user hint successfully identifies whether this node is the correct one.
     */
    private boolean isTableWithUniqueColumnsUserHint(TableScanNode node)
    {
        boolean joinColumnExists = false;
        if (tableNameInUniqueColumnMap(extractTableName(node))) {
            for (Map.Entry<String, String[]> entry : uniqueColumnsPerTable.entrySet()) {
                if (Arrays.stream(entry.getValue()).anyMatch(a -> a.equalsIgnoreCase(joinCriteriaStrings[0]) ||
                        a.equalsIgnoreCase(joinCriteriaStrings[1]))) {
                    joinColumnExists = true;
                }
            }
            return joinColumnExists;
        }
        else {
            return false;
        }
    }

    /**
     * @param node the TableScanNode from which table name is to be extracted
     * @return the name of the table after clearing all delimiters
     */
    private String extractTableName(TableScanNode node)
    {
        String tableName = node.getTable().getFullyQualifiedName();
        return tableName.substring(tableName.lastIndexOf(".") + 1);
    }

    /**
     * @param tableName the table that is to be checked for existence in the map
     * @return boolean signifying if the outer table in the map is being passed
     */
    private boolean tableNameInUniqueColumnMap(String tableName)
    {
        return uniqueColumnsPerTable.containsKey(tableName);
    }

    /**
     * Verify two things-
     * Both children's paths from the JoinNode cannot have a JoinNode
     * Only the path to the subquery table from the JoinNode has the GROUP BY clause
     * @return boolean value denoting if condition is satisfied
     */
    private boolean verifyPresenceOfJoinsAndGroupBy()
    {
        boolean outerHasJoin = verifyIfJoinNodeInPath(outerTablePathStack);
        boolean innerHasJoin = verifyIfJoinNodeInPath(innerTablePathStack);

        /*
         * If only outer table side contain Join, can proceed
         * if both have, or only inner has, then return false
         * */
        if (outerHasJoin && innerHasJoin) {
            return false;
        }
        else {
            /*
             * As only one or none of the paths have join Nodes, can move forward
             * Check if only one path contains GroupBy or OrderBy
             * */
            boolean outerHasGroupBy = verifyIfGroupByInPath(outerTablePathStack);
            /*
             * If the outer table has groupBy on it's path, return false.
             * */
            if (outerHasGroupBy) {
                return false;
            }
            else {
                /*
                 * If both sides have/don't have GroupBy/OrderBy return false
                 * Else return true.
                 * */
                return verifyIfGroupByInPath(innerTablePathStack);
            }
        }
    }

    /**
     * Verify that there is a JoinNode in the path from the original JoinNode to the TableScanNode
     * @param stack relevant to the side of the join node which is being checked
     * @return boolean denoting if the condition is satisfied
     */
    private boolean verifyIfJoinNodeInPath(Stack<NodeWithTreeDirection> stack)
    {
        boolean hasJoinInPath = false;

        for (NodeWithTreeDirection nodeInPath : stack) {
            PlanNode node = nodeInPath.getNode();
            if (node instanceof JoinNode) {
                hasJoinInPath = true;
                break;
            }
        }
        return hasJoinInPath;
    }

    /**
     * Check if the path to the TableScanNode has a GROUP BY clause
     * @param stack relevant to the side of the join node which is being checked
     * @return boolean denoting if the condition is satisfied
     */
    private boolean verifyIfGroupByInPath(Stack<NodeWithTreeDirection> stack)
    {
        boolean hasGroupByClause = false;
        PlanNode node;
        for (NodeWithTreeDirection childNode : stack) {
            node = childNode.getNode();
            if (node instanceof AggregationNode) {
                if (!((AggregationNode) node).getGroupingKeys().isEmpty()) {
                    hasGroupByClause = true;
                }
            }
        }
        return hasGroupByClause;
    }

    /**
     * Pass the GroupReference of a node and return the resolved form of it's source.
     * @param node whose source is to be resolved
     * @param index left or right child(for join node, can have two indices, otherwise 1)
     * @param lookup context parameter used to resolve GroupReference
     * @return PlanNode of source
     */
    private PlanNode resolveNodeFromGroupReference(PlanNode node, int index, Lookup lookup)
    {
        boolean precondition = (!(node instanceof JoinNode) && index == 0) ||
                ((node instanceof JoinNode) && (index == 0 || index == 1));

        checkState(precondition, "Attempt to access non-existing source of PlanNode");
        checkState(lookup.resolveGroup(node.getSources().get(index)).findFirst().isPresent(),
                "Attempt to resolve GroupReference when it doesn't exist");

        return lookup.resolveGroup(node.getSources().get(index)).findFirst().get();
    }

    /**
     * @param node the original JoinNode captured from the rule
     * @param lookup captured from the context
     * @return the final updated plan tree after all rearrangements
     */
    private PlanNode planRewriter(JoinNode node, Lookup lookup)
    {
        Stack<NodeWithTreeDirection> outerTablePathWithDir;
        outerTablePathWithDir = outerTablePathStack;
        return updateOuterTableAndInnerTablePath(node, outerTablePathWithDir, lookup);
    }

    /**
     * @param originalOuterJoinNode the original JoinNode captured by the rule
     * @param stack the stack which has all nodes from outer join to the TableScanNode of the table to be pushed down
     * @param lookup captured from the context
     * @return the final updated plan tree after all rearrangements
     */
    private PlanNode updateOuterTableAndInnerTablePath(JoinNode originalOuterJoinNode, Stack<NodeWithTreeDirection> stack, Lookup lookup)
    {
        if (verifyIfJoinNodeInPath(stack)) {
            Stack<NodeWithTreeDirection> intermediateOuterTableStack = new Stack<>();

            // First pop the stack till we reach a join node and push into another intermediateOuterTableStack.
            while (!(stack.peek().getNode() instanceof JoinNode)) {
                intermediateOuterTableStack.push(stack.pop());
            }

            JoinNode originalInnerJoinNode = (JoinNode) stack.peek().getNode();
            PlanNode childOfInnerJoin = intermediateOuterTableStack.peek().getNode();

            List<Symbol> outerJoinOutputSymbols = originalOuterJoinNode.getOutputSymbols();

            //update the outputSymbol for the new outer Join node to same as previous outer join
            List<Symbol> newOuterOutputSymbols = ImmutableList.<Symbol>builder()
                    .addAll(outerJoinOutputSymbols).build();
            List<Symbol> newInnerOutputSymbols;

            /*
             * Based on which direction the outer table is from the original Outer JoinNode(orig-OJ), do the following-
             * Get the output symbols from opposite direction source of orig-OJ
             * Get the output symbols from the child of the original Inner JoinNode(orig-IJ), which will be moved.
             * Add them to a List in appropriate order. This list is the output symbols for new-IJ (created from orig-OJ)
             * */
            if (outerTableDirection == DIRECTION.LEFT) {
                newInnerOutputSymbols = ImmutableList.<Symbol>builder()
                        .addAll(childOfInnerJoin.getOutputSymbols())
                        .addAll(originalOuterJoinNode.getRight().getOutputSymbols())
                        .build();
            }
            else {
                newInnerOutputSymbols = ImmutableList.<Symbol>builder()
                        .addAll(originalOuterJoinNode.getLeft().getOutputSymbols())
                        .addAll(childOfInnerJoin.getOutputSymbols())
                        .build();
            }

            /*
             * this sets new outer join's join criteria
             * As the earlier inner is new outer, it's join criteria must borrow from both the earlier join criterias
             * hence choose the unchanged ones, as these were the ones which were forwarded to earlier outerjoin
             * For the original join node's unchanged child-
             * this join criteria must be present here also as it was used earlier
             * */
            List<JoinNode.EquiJoinClause> newInnerJoinCriteria = getNewInnerJoinCriteria(originalInnerJoinNode, originalOuterJoinNode);
            List<JoinNode.EquiJoinClause> newOuterJoinCriteria = getNewOuterJoinCriteria(originalInnerJoinNode, originalOuterJoinNode);

            JoinNode newOuterJoinNode;
            JoinNode newInnerJoinNode;

            /*
            * Create the final new inner JoinNode based on the requirement of a JoinFilter or not
            * */
            if (needNewInnerJoinFilter(originalOuterJoinNode, childOfInnerJoin)) {
                newInnerJoinNode = new JoinNode(
                        originalOuterJoinNode.getId(),
                        originalOuterJoinNode.getType(),
                        childOfInnerJoin,
                        resolveNodeFromGroupReference(originalOuterJoinNode, 1, lookup),
                        newInnerJoinCriteria,
                        newInnerOutputSymbols,
                        originalOuterJoinNode.getFilter(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        originalOuterJoinNode.getDynamicFilters());
            }
            else {
                newInnerJoinNode = new JoinNode(
                        originalOuterJoinNode.getId(),
                        originalOuterJoinNode.getType(),
                        childOfInnerJoin,
                        resolveNodeFromGroupReference(originalOuterJoinNode, 1, lookup),
                        newInnerJoinCriteria,
                        newInnerOutputSymbols,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        originalOuterJoinNode.getDynamicFilters());
            }

            /*
            * Create the new outer JoinNode based on the direction of the subquery table
            * */
            if (innerTableDirection == DIRECTION.LEFT) {
                newOuterJoinNode = new JoinNode(
                        originalInnerJoinNode.getId(),
                        originalInnerJoinNode.getType(),
                        newInnerJoinNode,
                        originalInnerJoinNode.getRight(),
                        newOuterJoinCriteria,
                        newOuterOutputSymbols,
                        originalOuterJoinNode.getFilter(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        originalInnerJoinNode.getDynamicFilters());
            }
            else {
                newOuterJoinNode = new JoinNode(
                        originalInnerJoinNode.getId(),
                        originalInnerJoinNode.getType(),
                        originalInnerJoinNode.getLeft(),
                        newInnerJoinNode,
                        newOuterJoinCriteria,
                        newOuterOutputSymbols,
                        originalOuterJoinNode.getFilter(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        originalInnerJoinNode.getDynamicFilters());
            }

            return updateInnerTable(newOuterJoinNode, newInnerJoinNode, innerTablePathStack, true);
        }
        else {
            /*
             * The outer table has a direct path to the JoinNode under consideration
             * We don't change the tree at all, instead just returning the current node.
             * */
            return updateInnerTable(originalOuterJoinNode, originalOuterJoinNode, innerTablePathStack, false);
        }
    }

    /**
     * @param innerJoinNode the original InnerJoinNode which joined the two outer tables
     * @param originalOuterJoin the original JoinNode captured by the rule
     * @return the new join criteria for the newly created inner join nodes
     */
    private List<JoinNode.EquiJoinClause> getNewInnerJoinCriteria(JoinNode innerJoinNode, JoinNode originalOuterJoin)
    {
        Symbol joinSymbolOfInnerJoinTransferSource;
        Symbol joinSymbolOfOuterJoinFixedSource;
        JoinNode.EquiJoinClause newJoinClause;

        if (innerTableDirection == DIRECTION.LEFT) {
            joinSymbolOfInnerJoinTransferSource = innerJoinNode.getCriteria().get(0).getLeft();
        }
        else {
            joinSymbolOfInnerJoinTransferSource = innerJoinNode.getCriteria().get(0).getRight();
        }

        if (outerTableDirection == DIRECTION.LEFT) {
            joinSymbolOfOuterJoinFixedSource = originalOuterJoin.getCriteria().get(0).getRight();
            newJoinClause = new JoinNode.EquiJoinClause(joinSymbolOfInnerJoinTransferSource, joinSymbolOfOuterJoinFixedSource);
        }
        else {
            joinSymbolOfOuterJoinFixedSource = originalOuterJoin.getCriteria().get(0).getLeft();
            newJoinClause = new JoinNode.EquiJoinClause(joinSymbolOfOuterJoinFixedSource, joinSymbolOfOuterJoinFixedSource);
        }
        return ImmutableList.of(newJoinClause);
    }

    /**
     * @param innerJoinNode the original InnerJoinNode which joined the two outer tables
     * @param originalJoinNode the original JoinNode captured by the rule
     * @return the new join criteria for the newly created outer join nodes
     */
    private List<JoinNode.EquiJoinClause> getNewOuterJoinCriteria(JoinNode innerJoinNode, JoinNode originalJoinNode)
    {
        Symbol joinSymbolOfInnerJoinFixedSource;
        Symbol joinSymbolOfOuterJoinFixedSource;
        JoinNode.EquiJoinClause newJoinClause;

        if (outerTableDirection == DIRECTION.LEFT) {
            joinSymbolOfOuterJoinFixedSource = originalJoinNode.getCriteria().get(0).getRight();
        }
        else {
            joinSymbolOfOuterJoinFixedSource = originalJoinNode.getCriteria().get(0).getLeft();
        }

        if (innerTableDirection == DIRECTION.LEFT) {
            joinSymbolOfInnerJoinFixedSource = innerJoinNode.getCriteria().get(0).getRight();
            newJoinClause = new JoinNode.EquiJoinClause(joinSymbolOfOuterJoinFixedSource, joinSymbolOfInnerJoinFixedSource);
        }
        else {
            joinSymbolOfInnerJoinFixedSource = innerJoinNode.getCriteria().get(0).getLeft();
            newJoinClause = new JoinNode.EquiJoinClause(joinSymbolOfInnerJoinFixedSource, joinSymbolOfOuterJoinFixedSource);
        }

        return ImmutableList.of(newJoinClause);
    }

    /**
     * @param originalJoinNode the original JoinNode captured by the rule
     * @param childOfInnerJoin the child of the InnerJoinNode that has to be moved(the outer Table)
     * @return boolean signifying if the table being pushed down will need a join filter for the new join
     */
    private boolean needNewInnerJoinFilter(JoinNode originalJoinNode, PlanNode childOfInnerJoin)
    {
        if (originalJoinNode.getFilter().isPresent()) {
            ComparisonExpression originalJoinNodeFilter = (ComparisonExpression) castToExpression(originalJoinNode.getFilter().get());
            List<Symbol> innerJoinChildOPSymbols = childOfInnerJoin.getOutputSymbols();

            SymbolReference originalFilterSymbolRefLeft = (SymbolReference) originalJoinNodeFilter.getLeft();
            SymbolReference originalFilterSymbolRefRight = (SymbolReference) originalJoinNodeFilter.getRight();

            for (Symbol symbol : innerJoinChildOPSymbols) {
                if (symbol.toString().equalsIgnoreCase(originalFilterSymbolRefLeft.toString()) ||
                        symbol.toString().equalsIgnoreCase(originalFilterSymbolRefRight.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param newOuterJoinNode is the newly created OuterJoinNode after pushing the outer table into the subquery
     * @param newInnerJoinNode is the newly created InnerJoinNode after the outer table has been pushed into subquery
     * @param stack is the stack of nodes from the original captured JoinNode and subquery TableScanNode
     * @param outerTableNodeUpdated is the boolean value which checks if there was another join and the outer table has been moved
     * @return the final parent of the subtree after all rearrangement of the join nodes
     */
    private PlanNode updateInnerTable(JoinNode newOuterJoinNode, JoinNode newInnerJoinNode,
                                      Stack<NodeWithTreeDirection> stack, boolean outerTableNodeUpdated)
    {
        if (outerTableNodeUpdated) {
            PlanNode newProjectNode = getNewIntermediateTreeAfterInnerTableUpdate(newInnerJoinNode, stack);
            return new JoinNode(
                    newOuterJoinNode.getId(),
                    newOuterJoinNode.getType(),
                    newOuterJoinNode.getLeft(),
                    newProjectNode,
                    newOuterJoinNode.getCriteria(),
                    newOuterJoinNode.getOutputSymbols(),
                    newOuterJoinNode.getFilter(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    newOuterJoinNode.getDynamicFilters());
        }
        else {
            return getNewIntermediateTreeAfterInnerTableUpdate(newInnerJoinNode, stack);
        }
    }

    /**
     * @param newInnerJoinNode is the recently created join node after the outer table has been pushed into subquery
     * @param stack is the stack of nodes from the original captured JoinNode and subquery TableScanNode
     * @return the PlanNode after pulling the subquery table's Aggregation and Group By above the join
     */
    private PlanNode getNewIntermediateTreeAfterInnerTableUpdate(JoinNode newInnerJoinNode,
                                                                 Stack<NodeWithTreeDirection> stack)
    {
        TableScanNode subqueryTableNode = (TableScanNode) stack.peek().getNode();

        /*
         * create assignment builder for a new intermediate ProjectNode between the newInnerJoinNode and
         * TableScanNode for subquery table
         * */
        Assignments.Builder assignmentsBuilder = Assignments.builder();

        /*
         * All symbols from TableScanNode are directly copied over
         * */
        for (Map.Entry<Symbol, ColumnHandle> tableEntry : subqueryTableNode.getAssignments().entrySet()) {
            Symbol s = tableEntry.getKey();
            assignmentsBuilder.put(s, castToRowExpression(new SymbolReference(s.getName())));
        }

        ProjectNode parentOfSubqueryTableNode = new ProjectNode(
                ruleContext.getIdAllocator().getNextId(),
                subqueryTableNode,
                assignmentsBuilder.build());

        List<Symbol> parentOfSubqueryTableNodeOutputSymbols = parentOfSubqueryTableNode.getOutputSymbols();

        /*
         * Recreate the inner joinNode using the new ProjectNode as one of its sources.
         * */

        PlanNodeStatsEstimate leftSourceStats = ruleContext.getStatsProvider().getStats(newInnerJoinNode.getLeft());
        PlanNodeStatsEstimate rightSourceStats = ruleContext.getStatsProvider().getStats(parentOfSubqueryTableNode);

        JoinNode newJoinNode;

        if (leftSourceStats.isOutputRowCountUnknown()) {
            /*
            * CAUTION: the stats are not available, so source reordering is not allowed. Query may fail
            * */
            newJoinNode = new JoinNode(
                    newInnerJoinNode.getId(),
                    newInnerJoinNode.getType(),
                    newInnerJoinNode.getLeft(),
                    parentOfSubqueryTableNode,
                    newInnerJoinNode.getCriteria(),
                    ImmutableList.<Symbol>builder().addAll(parentOfSubqueryTableNodeOutputSymbols).build(),
                    newInnerJoinNode.getFilter(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    newInnerJoinNode.getDynamicFilters());
        }
        else {
            double leftSourceRowsCount = leftSourceStats.getOutputRowCount();

            double rightSourceRowsCount = rightSourceStats.getOutputRowCount();

            if (leftSourceRowsCount <= rightSourceRowsCount) {
                //We reorder the children of this new join node such that the table with more rows is on the left
                List<JoinNode.EquiJoinClause> newInnerJoinCriteria = newInnerJoinNode.getCriteria().stream()
                        .map(JoinNode.EquiJoinClause::flip)
                        .collect(toImmutableList());
                newJoinNode = new JoinNode(
                        newInnerJoinNode.getId(),
                        newInnerJoinNode.getType(),
                        parentOfSubqueryTableNode,
                        newInnerJoinNode.getLeft(),
                        newInnerJoinCriteria,
                        ImmutableList.<Symbol>builder().addAll(parentOfSubqueryTableNodeOutputSymbols).build(),
                        newInnerJoinNode.getFilter(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        newInnerJoinNode.getDynamicFilters());
            }
            else {
                newJoinNode = new JoinNode(
                        newInnerJoinNode.getId(),
                        newInnerJoinNode.getType(),
                        newInnerJoinNode.getLeft(),
                        parentOfSubqueryTableNode,
                        newInnerJoinNode.getCriteria(),
                        ImmutableList.<Symbol>builder().addAll(parentOfSubqueryTableNodeOutputSymbols).build(),
                        newInnerJoinNode.getFilter(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        newInnerJoinNode.getDynamicFilters());
            }
        }
        //Remove the TableScanNode from the stack
        stack.pop();

        AggregationNode newAggNode = (AggregationNode) stack.peek().getNode().replaceChildren(ImmutableList.of(newJoinNode));

        return stack.firstElement().getNode().replaceChildren(ImmutableList.of(newAggNode));
    }

    /**
     * Clear all the data structures explicitly once rule has completed its task
     */
    private void clearAllDataStructures()
    {
        outerTablePathStack.clear();
        innerTablePathStack.clear();
        uniqueColumnsPerTable.clear();
    }

    /**
     * The direction of nodes in the plan tree
     */
    private enum DIRECTION
    {
        LEFT, RIGHT
    }

    /**
     * Custom class created to store nodes with their direction in the plan tree
     * Objects of this class form elements of the stack
     */
    private static class NodeWithTreeDirection
    {
        private final DIRECTION direction;
        private final PlanNode node;

        public NodeWithTreeDirection(PlanNode node, DIRECTION dir)
        {
            this.node = node;
            this.direction = dir;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public DIRECTION getDirection()
        {
            return direction;
        }
    }
}
