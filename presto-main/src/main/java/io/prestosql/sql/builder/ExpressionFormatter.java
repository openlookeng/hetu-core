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
package io.prestosql.sql.builder;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.sql.SqlQueryWriter;
import io.prestosql.spi.sql.expression.Operators;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Time;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.AtTimeZone;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BindExpression;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CurrentPath;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.CurrentUser;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Extract;
import io.prestosql.sql.tree.FieldReference;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.tree.WhenClause;
import io.prestosql.sql.tree.Window;
import io.prestosql.sql.tree.WindowFrame;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class ExpressionFormatter
{
    private ExpressionFormatter() {}

    public static String formatExpression(SqlQueryWriter queryWriter, Node expression, Optional<List<Expression>> parameters)
    {
        return new Formatter(queryWriter, parameters, Optional.empty()).process(expression, null);
    }

    public static String formatExpression(SqlQueryWriter queryWriter, Node expression, Optional<List<Expression>> parameters, Map<String, Selection> qualifiedNames)
    {
        return new Formatter(queryWriter, parameters, Optional.of(qualifiedNames)).process(expression, null);
    }

    private static class Formatter
            extends AstVisitor<String, Void>
    {
        private final SqlQueryWriter queryWriter;
        private final Optional<List<Expression>> parameters;
        private final Optional<Map<String, Selection>> qualifiedNames;
        private Optional<List<String>> params = Optional.empty();

        private Formatter(SqlQueryWriter queryWriter, Optional<List<Expression>> parameters, Optional<Map<String, Selection>> qualifiedNames)
        {
            this.queryWriter = queryWriter;
            this.parameters = parameters;
            this.qualifiedNames = qualifiedNames;
        }

        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported to rewrite node: " + node.getClass().getName());
        }

        /////////////////////////////////////the following method is for sql expression/////////////////////////////////////
        @Override
        protected String visitRow(Row node, Void context)
        {
            return queryWriter.row(processAll(node.getItems(), context));
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitAtTimeZone(AtTimeZone node, Void context)
        {
            return queryWriter.atTimeZone(process(node.getValue(), context), process(node.getTimeZone(), context));
        }

        @Override
        protected String visitCurrentUser(CurrentUser node, Void context)
        {
            return queryWriter.currentUser();
        }

        @Override
        protected String visitCurrentPath(CurrentPath node, Void context)
        {
            return queryWriter.currentPath();
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context)
        {
            return queryWriter.currentTime(Time.Function.valueOf(node.getFunction().toString()), node.getPrecision());
        }

        @Override
        protected String visitExtract(Extract node, Void context)
        {
            return queryWriter.extract(process(node.getExpression(), context), Time.ExtractField.valueOf(node.getField().name()));
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return queryWriter.booleanLiteral(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return queryWriter.stringLiteral(node.getValue());
        }

        @Override
        protected String visitCharLiteral(CharLiteral node, Void context)
        {
            return queryWriter.charLiteral(node.getValue());
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return queryWriter.binaryLiteral(node.toHexString());
        }

        @Override
        protected String visitParameter(Parameter node, Void context)
        {
            if (parameters.isPresent() && !params.isPresent()) {
                params = parameters.map(list -> list.stream()
                        .map(exp -> process(exp, context))
                        .collect(toList()));
            }
            return queryWriter.parameter(params, node.getPosition());
        }

        @Override
        protected String visitArrayConstructor(ArrayConstructor node, Void context)
        {
            ImmutableList.Builder<String> valueStrings = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                // wait to verify formatSql's Difference visit rewrite features set
                valueStrings.add(SqlQueryFormatter.formatSqlQuery(queryWriter, value, parameters));
            }
            return queryWriter.arrayConstructor(valueStrings.build());
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            // wait to verify formatSql's Difference visit rewrite features set
            return queryWriter.subscriptExpression(SqlQueryFormatter.formatSqlQuery(queryWriter, node.getBase(), parameters),
                    SqlQueryFormatter.formatSqlQuery(queryWriter, node.getIndex(), parameters));
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return queryWriter.longLiteral(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return queryWriter.doubleLiteral(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return queryWriter.decimalLiteral(node.getValue());
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Void context)
        {
            return queryWriter.genericLiteral(node.getType(), node.getValue());
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Void context)
        {
            return queryWriter.timeLiteral(node.getValue());
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            return queryWriter.timestampLiteral(node.getValue());
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context)
        {
            return queryWriter.nullLiteral();
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            return queryWriter.intervalLiteral(Time.IntervalSign.valueOf(node.getSign().name()),
                    node.getValue(),
                    Time.IntervalField.valueOf(node.getStartField().name()),
                    node.getEndField().flatMap(field -> Optional.of(Time.IntervalField.valueOf(field.name()))));
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            // wait to verify formatSql's Difference visit rewrite features set
            return queryWriter.subqueryExpression(SqlQueryFormatter.formatSqlQuery(queryWriter, node.getQuery(), parameters));
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context)
        {
            // wait to verify formatSql's Difference visit rewrite features set
            return queryWriter.exists(SqlQueryFormatter.formatSqlQuery(queryWriter, node.getSubquery(), parameters));
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            return queryWriter.identifier(node.getValue(), node.isDelimited());
        }

        @Override
        protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context)
        {
            return queryWriter.lambdaArgumentDeclaration(process(node.getName(), context));
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Void context)
        {
            return queryWriter.formatIdentifier(qualifiedNames, node.getName());
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            return queryWriter.dereferenceExpression(process(node.getBase(), context), process(node.getField(), context));
        }

        @Override
        public String visitFieldReference(FieldReference node, Void context)
        {
            return queryWriter.fieldReference(node.getFieldIndex());
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            QualifiedName qualifiedName = new QualifiedName(node.getName().getParts());
            List<String> arguments = processAll(node.getArguments(), context);
            Optional<String> orderBy = node.getOrderBy().flatMap(order -> Optional.of(processOrderBy(order, context)));
            Optional<String> filter = node.getFilter().flatMap(exp -> Optional.of(visitFilter(exp, context)));
            Optional<String> window = node.getWindow().flatMap(win -> Optional.of(visitWindow(win, context)));
            return queryWriter.functionCall(qualifiedName, node.isDistinct(), arguments, orderBy, filter, window);
        }

        @Override
        protected String visitLambdaExpression(LambdaExpression node, Void context)
        {
            return queryWriter.lambdaExpression(processAll(node.getArguments(), context), process(node.getBody(), context));
        }

        @Override
        protected String visitBindExpression(BindExpression node, Void context)
        {
            return queryWriter.bindExpression(processAll(node.getValues(), context), process(node.getFunction(), context));
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return queryWriter.logicalBinaryExpression(Operators.LogicalOperator.valueOf(node.getOperator().toString()),
                    process(node.getLeft(), context),
                    process(node.getRight(), context));
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context)
        {
            return queryWriter.notExpression(process(node.getValue(), context));
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return queryWriter.comparisonExpression(Operators.ComparisonOperator.valueOf(node.getOperator().toString()),
                    process(node.getLeft(), context),
                    process(node.getRight(), context));
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return queryWriter.isNullPredicate(process(node.getValue(), context));
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return queryWriter.isNotNullPredicate(process(node.getValue(), context));
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Void context)
        {
            return queryWriter.nullIfExpression(process(node.getFirst(), context), process(node.getSecond(), context));
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context)
        {
            return queryWriter.ifExpression(process(node.getCondition(), context),
                    process(node.getTrueValue(), context),
                    node.getFalseValue().flatMap(value -> Optional.of(process(value, context))));
        }

        @Override
        protected String visitTryExpression(TryExpression node, Void context)
        {
            return queryWriter.tryExpression(process(node.getInnerExpression(), context));
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return queryWriter.coalesceExpression(processAll(node.getOperands(), context));
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            return queryWriter.arithmeticUnary(Operators.Sign.valueOf(node.getSign().toString()), process(node.getValue(), context));
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return queryWriter.arithmeticBinary(Operators.ArithmeticOperator.valueOf(node.getOperator().toString()),
                    process(node.getLeft(), context),
                    process(node.getRight(), context));
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context)
        {
            return queryWriter.likePredicate(process(node.getValue(), context),
                    process(node.getPattern(), context),
                    node.getEscape().flatMap(escape -> Optional.of(process(escape, context))));
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            return queryWriter.allColumns(node.getPrefix().flatMap(name -> Optional.of(new QualifiedName(name.getParts()))));
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return queryWriter.cast(process(node.getExpression(), context), node.getType(), node.isSafe(), node.isTypeOnly());
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            return queryWriter.searchedCaseExpression(processAll(node.getWhenClauses(), context), node.getDefaultValue().flatMap(value -> Optional.of(process(value, context))));
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            return queryWriter.simpleCaseExpression(process(node.getOperand(), context),
                    processAll(node.getWhenClauses(), context),
                    node.getDefaultValue()
                            .flatMap(value -> Optional.of(process(value, context))));
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context)
        {
            return queryWriter.whenClause(process(node.getOperand(), context), process(node.getResult(), context));
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return queryWriter.betweenPredicate(process(node.getValue(), context),
                    process(node.getMin(), context),
                    process(node.getMax(), context));
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context)
        {
            return queryWriter.inPredicate(process(node.getValue(), context), process(node.getValueList(), context));
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context)
        {
            return queryWriter.inListExpression(processAll(node.getValues(), context));
        }

        private String visitFilter(Expression node, Void context)
        {
            return queryWriter.filter(process(node, context));
        }

        @Override
        public String visitWindow(Window node, Void context)
        {
            Optional<String> orderBy = node.getOrderBy().flatMap(order -> Optional.of(processOrderBy(order, context)));
            Optional<String> frame = node.getFrame().flatMap(windowFrame -> Optional.of(process(windowFrame, context)));

            return queryWriter.window(processAll(node.getPartitionBy(), context), orderBy, frame);
        }

        @Override
        public String visitWindowFrame(WindowFrame node, Void context)
        {
            return queryWriter.windowFrame(Types.WindowFrameType.valueOf(node.getType().toString()),
                    process(node.getStart(), context),
                    node.getEnd().flatMap(end -> Optional.of(process(end, context))));
        }

        @Override
        public String visitFrameBound(FrameBound node, Void context)
        {
            return queryWriter.frameBound(Types.FrameBoundType.valueOf(node.getType().toString()),
                    node.getValue().flatMap(value -> Optional.of(process(value, context))));
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
        {
            return queryWriter.quantifiedComparisonExpression(Operators.ComparisonOperator.valueOf(node.getOperator().toString()),
                    Types.Quantifier.valueOf(node.getQuantifier().toString()),
                    process(node.getValue(), context),
                    process(node.getSubquery(), context));
        }

        @Override
        public String visitGroupingOperation(GroupingOperation node, Void context)
        {
            return queryWriter.groupingOperation(processAll(node.getGroupingColumns(), context));
        }

        private List<String> processAll(List<? extends Expression> expressions, Void context)
        {
            return expressions.stream()
                    .map(expression -> process(expression, context))
                    .collect(toList());
        }

        private String processOrderBy(OrderBy orderBy, Void context)
        {
            ImmutableList.Builder<io.prestosql.spi.sql.expression.OrderBy> sortItemsBuilder = new ImmutableList.Builder<>();
            for (SortItem sortItem : orderBy.getSortItems()) {
                // wait to verify formatSql's Difference visit rewrite features set
                sortItemsBuilder.add(new io.prestosql.spi.sql.expression.OrderBy(process(sortItem.getSortKey(), context),
                        toSortOrder(sortItem)));
            }
            return queryWriter.orderBy(sortItemsBuilder.build());
        }

        private static SortOrder toSortOrder(SortItem sortItem)
        {
            if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
                if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                    return SortOrder.ASC_NULLS_FIRST;
                }
                else {
                    return SortOrder.ASC_NULLS_LAST;
                }
            }
            else {
                if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                    return SortOrder.DESC_NULLS_FIRST;
                }
                else {
                    return SortOrder.DESC_NULLS_LAST;
                }
            }
        }
    }
}
