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
package io.prestosql.sql.builder;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.sql.SqlQueryWriter;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Cube;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GroupingElement;
import io.prestosql.sql.tree.GroupingSets;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.Rollup;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SimpleGroupBy;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.Values;
import io.prestosql.sql.tree.With;
import io.prestosql.sql.tree.WithQuery;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * the query formatter for sub query push down in base jdbc, refer to io.prestosql.sql.SqlFormatter
 *
 * @since 2020-02-27
 */
public class SqlQueryFormatter
{
    private static final String INDENT = " ";

    private SqlQueryFormatter() {}

    public static String formatSqlQuery(SqlQueryWriter queryWriter, Node root, Optional<List<Expression>> parameters)
    {
        Formatter.FormatterContext context = new Formatter.FormatterContext(0, queryWriter);
        Formatter formatter = new Formatter(parameters);
        return formatter.process(root, context);
    }

    private static class Formatter
            extends AstVisitor<String, Formatter.FormatterContext>
    {
        private final Optional<List<Expression>> parameters;

        public Formatter(Optional<List<Expression>> parameters)
        {
            this.parameters = parameters;
        }

        /////////////////////////////////////the following method is for sql statement/////////////////////////////////////
        @Override
        protected String visitNode(Node node, Formatter.FormatterContext context)
        {
            throw new UnsupportedOperationException("Subquery push down sql query formatter not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected String visitExpression(Expression node, Formatter.FormatterContext context)
        {
            checkArgument(context.indent == 0, "visitExpression should only be called at root");
            return ExpressionFormatter.formatExpression(context.queryWriter, node, parameters);
        }

        @Override
        protected String visitQuery(Query node, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                sb.append(Strings.repeat(INDENT, context.indent)).append("WITH");
                if (with.isRecursive()) {
                    sb.append(" RECURSIVE");
                }
                sb.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    sb.append(Strings.repeat(INDENT, context.indent))
                            .append(ExpressionFormatter.formatExpression(context.queryWriter, query.getName(), parameters));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(sb, columnNames, context));
                    sb.append(" AS ");
                    sb.append(process(new TableSubquery(query.getQuery()), context));
                    sb.append('\n');
                    if (queries.hasNext()) {
                        sb.append(", ");
                    }
                }
            }

            sb.append(processRelation(node.getQueryBody(), context));

            if (node.getOrderBy().isPresent()) {
                sb.append(process(node.getOrderBy().get(), context));
            }

            if (node.getOffset().isPresent()) {
                sb.append(process(node.getOffset().get(), context));
            }

            if (node.getLimit().isPresent()) {
                sb.append(process(node.getLimit().get(), context));
            }
            return sb.toString();
        }

        @Override
        protected String visitQuerySpecification(QuerySpecification node, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(process(node.getSelect(), context));

            if (node.getFrom().isPresent()) {
                sb.append(Strings.repeat(INDENT, context.indent)).append("FROM");
                sb.append('\n');
                sb.append(Strings.repeat(INDENT, context.indent)).append("  ");
                sb.append(process(node.getFrom().get(), context));
            }

            sb.append('\n');

            if (node.getWhere().isPresent()) {
                sb.append(Strings.repeat(INDENT, context.indent)).append("WHERE ")
                        .append(ExpressionFormatter.formatExpression(context.queryWriter, node.getWhere().get(), parameters)).append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                sb.append(Strings.repeat(INDENT, context.indent))
                        .append("GROUP BY ").append((node.getGroupBy().get().isDistinct() ? " DISTINCT " : ""))
                        .append(formatGroupBy(node.getGroupBy().get().getGroupingElements(), context)).append('\n');
            }

            if (node.getHaving().isPresent()) {
                sb.append(Strings.repeat(INDENT, context.indent)).append("HAVING ")
                        .append(ExpressionFormatter.formatExpression(context.queryWriter, node.getHaving().get(), parameters))
                        .append('\n');
            }

            if (node.getOrderBy().isPresent()) {
                sb.append(process(node.getOrderBy().get(), context));
            }

            if (node.getOffset().isPresent()) {
                sb.append(process(node.getOffset().get(), context));
            }

            if (node.getLimit().isPresent()) {
                sb.append(process(node.getLimit().get(), context));
            }
            return sb.toString();
        }

        @Override
        protected String visitOrderBy(OrderBy node, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(Strings.repeat(INDENT, context.indent)).append(formatOrderBy(node, parameters)).append('\n');
            return sb.toString();
        }

        private String formatOrderBy(OrderBy orderBy, Optional<List<Expression>> parameters)
        {
            return "ORDER BY " + Joiner.on(", ").join(orderBy.getSortItems().stream()
                    .map(io.prestosql.sql.ExpressionFormatter.sortItemFormatterFunction(parameters))
                    .iterator());
        }

        @Override
        protected String visitOffset(Offset node, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(Strings.repeat(INDENT, context.indent)).append("OFFSET ")
                    .append(node.getRowCount()).append(" ROWS").append('\n');
            return sb.toString();
        }

        @Override
        protected String visitSelect(Select node, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(Strings.repeat(INDENT, context.indent)).append("SELECT");

            if (node.isDistinct()) {
                sb.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    sb.append("\n")
                            .append(Strings.repeat(INDENT, context.indent))
                            .append(first ? "  " : ", ");

                    sb.append(process(item, context));
                    first = false;
                }
            }
            else {
                sb.append(' ');
                sb.append(process(getOnlyElement(node.getSelectItems()), context));
            }

            sb.append('\n');
            return sb.toString();
        }

        @Override
        protected String visitSingleColumn(SingleColumn node, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(ExpressionFormatter.formatExpression(context.queryWriter, node.getExpression(), parameters));
            if (node.getAlias().isPresent()) {
                sb.append(' ').append(ExpressionFormatter.formatExpression(context.queryWriter, node.getAlias().get(), parameters));
            }
            return sb.toString();
        }

        @Override
        protected String visitTable(Table node, Formatter.FormatterContext context)
        {
            return formatName(node.getName());
        }

        @Override
        protected String visitValues(Values node, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(" VALUES ");
            boolean first = true;
            for (Expression row : node.getRows()) {
                sb.append("\n")
                        .append(Strings.repeat(INDENT, context.indent))
                        .append(first ? "  " : ", ");

                sb.append(ExpressionFormatter.formatExpression(context.queryWriter, row, parameters));
                first = false;
            }
            sb.append('\n');
            return sb.toString();
        }

        private static String formatName(Identifier name)
        {
            String delimiter = name.isDelimited() ? "\"" : "";
            return delimiter + name.getValue().replace("\"", "\"\"") + delimiter;
        }

        private static String formatName(QualifiedName name)
        {
            return name.getOriginalParts().stream()
                    .map(Formatter::formatName)
                    .collect(joining("."));
        }

        public static class FormatterContext
        {
            private SqlQueryWriter queryWriter;

            private int indent;

            public FormatterContext(int indent, SqlQueryWriter queryWriter)
            {
                this.indent = indent;
                this.queryWriter = queryWriter;
            }
        }

        private String processRelation(Relation relation, Formatter.FormatterContext context)
        {
            StringBuilder sb = new StringBuilder();
            // TODO: handle this properly
            if (relation instanceof Table) {
                sb.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                sb.append(process(relation, context));
            }
            return sb.toString();
        }

        private void appendAliasColumns(StringBuilder builder, List<Identifier> columns, FormatterContext context)
        {
            if ((columns != null) && (!columns.isEmpty())) {
                String formattedColumns = columns.stream()
                        .map(name -> ExpressionFormatter.formatExpression(context.queryWriter, name, Optional.empty()))
                        .collect(Collectors.joining(", "));
                builder.append(" (")
                        .append(formattedColumns)
                        .append(')');
            }
        }

        private String formatGroupBy(List<GroupingElement> groupingElements, FormatterContext context)
        {
            return formatGroupBy(groupingElements, Optional.empty(), context);
        }

        private String formatGroupBy(List<GroupingElement> groupingElements, Optional<List<Expression>> parameters, FormatterContext context)
        {
            ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

            for (GroupingElement groupingElement : groupingElements) {
                String result = "";
                if (groupingElement instanceof SimpleGroupBy) {
                    List<Expression> columns = groupingElement.getExpressions();
                    if (columns.size() == 1) {
                        result = ExpressionFormatter.formatExpression(context.queryWriter, getOnlyElement(columns), parameters);
                    }
                    else {
                        result = formatGroupingSet(columns, parameters, context);
                    }
                }
                else if (groupingElement instanceof GroupingSets) {
                    result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                            ((GroupingSets) groupingElement).getSets().stream()
                                    .map(e -> formatGroupingSet(e, parameters, context))
                                    .iterator()));
                }
                else if (groupingElement instanceof Cube) {
                    result = format("CUBE %s", formatGroupingSet(groupingElement.getExpressions(), parameters, context));
                }
                else if (groupingElement instanceof Rollup) {
                    result = format("ROLLUP %s", formatGroupingSet(groupingElement.getExpressions(), parameters, context));
                }
                resultStrings.add(result);
            }
            return Joiner.on(", ").join(resultStrings.build());
        }

        private String formatGroupingSet(List<Expression> groupingSet, Optional<List<Expression>> parameters, FormatterContext context)
        {
            return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                    .map(e -> ExpressionFormatter.formatExpression(context.queryWriter, e, parameters))
                    .iterator()));
        }
    }
}
