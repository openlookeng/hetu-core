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
package io.hetu.core.sql.migration.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.hetu.core.migration.source.impala.ImpalaSqlBaseVisitor;
import io.hetu.core.migration.source.impala.ImpalaSqlLexer;
import io.hetu.core.migration.source.impala.ImpalaSqlParser;
import io.prestosql.spi.sql.expression.Types.FrameBoundType;
import io.prestosql.spi.sql.expression.Types.WindowFrameType;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AssignmentItem;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ColumnDefinition;
import io.prestosql.sql.tree.Comment;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CreateRole;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.Delete;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropRole;
import io.prestosql.sql.tree.DropSchema;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Except;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Extract;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Grant;
import io.prestosql.sql.tree.GroupBy;
import io.prestosql.sql.tree.GroupingElement;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.Intersect;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.JoinCriteria;
import io.prestosql.sql.tree.JoinOn;
import io.prestosql.sql.tree.JoinUsing;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.Lateral;
import io.prestosql.sql.tree.LikeClause;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.PathElement;
import io.prestosql.sql.tree.PathSpecification;
import io.prestosql.sql.tree.PrincipalSpecification;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QueryBody;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.Revoke;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SampledRelation;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.ShowColumns;
import io.prestosql.sql.tree.ShowCreate;
import io.prestosql.sql.tree.ShowFunctions;
import io.prestosql.sql.tree.ShowRoles;
import io.prestosql.sql.tree.ShowSchemas;
import io.prestosql.sql.tree.ShowSession;
import io.prestosql.sql.tree.ShowStats;
import io.prestosql.sql.tree.ShowTables;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.SimpleGroupBy;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableElement;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.sql.tree.Union;
import io.prestosql.sql.tree.Unnest;
import io.prestosql.sql.tree.Update;
import io.prestosql.sql.tree.Use;
import io.prestosql.sql.tree.Values;
import io.prestosql.sql.tree.WhenClause;
import io.prestosql.sql.tree.Window;
import io.prestosql.sql.tree.WindowFrame;
import io.prestosql.sql.tree.With;
import io.prestosql.sql.tree.WithQuery;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.hetu.core.sql.migration.Constants.FORMAT;
import static io.hetu.core.sql.migration.Constants.LOCATION;
import static io.hetu.core.sql.migration.Constants.PARTITIONED_BY;
import static io.hetu.core.sql.migration.Constants.SORTED_BY;
import static io.hetu.core.sql.migration.Constants.TRANSACTIONAL;
import static io.hetu.core.sql.util.AstBuilderUtils.check;
import static io.hetu.core.sql.util.AstBuilderUtils.getLocation;
import static io.hetu.core.sql.util.AstBuilderUtils.unsupportedError;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ImpalaAstBuilder
        extends ImpalaSqlBaseVisitor<Node>
{
    private int parameterPosition;
    private final ParsingOptions parsingOptions;

    private List<ParserDiffs> parserDiffsList = new ArrayList<>();

    public List<ParserDiffs> getParserDiffsList()
    {
        return parserDiffsList;
    }

    public void addDiff(DiffType type, String source, String target, String message)
    {
        parserDiffsList.add(new ParserDiffs(type,
                Optional.ofNullable(source),
                Optional.ofNullable(target),
                Optional.ofNullable(message)));
    }

    public void addDiff(DiffType type, String source, String message)
    {
        parserDiffsList.add(new ParserDiffs(type,
                Optional.ofNullable(source),
                Optional.empty(),
                Optional.ofNullable(message)));
    }

    private static final Map<String, String> IMPALA_TO_HETU_FILE_FORMAT = new HashMap<String, String>()
    {
        {
            put("PARQUET", "Parquet");
            put("TEXTFILE", "TextFile");
            put("AVRO", "Avro");
            put("SEQUENCEFILE", "SequenceFile");
            put("RCFILE", "RCText");
        }
    };

    public ImpalaAstBuilder(io.prestosql.sql.parser.ParsingOptions parsingOptions)
    {
        parserDiffsList.clear();
        this.parsingOptions = requireNonNull(parsingOptions, "Convertion Options is null");
    }

    @Override
    public Node visitSingleStatement(ImpalaSqlParser.SingleStatementContext context)
    {
        return visit(context.statement());
    }

    @Override
    public Node visitStandaloneExpression(ImpalaSqlParser.StandaloneExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Node visitStandalonePathSpecification(ImpalaSqlParser.StandalonePathSpecificationContext context)
    {
        return visit(context.pathSpecification());
    }

    @Override
    public Node visitStatementDefault(ImpalaSqlParser.StatementDefaultContext ctx)
    {
        return super.visitStatementDefault(ctx);
    }

    // ******************* statements **********************
    @Override
    public Node visitUse(ImpalaSqlParser.UseContext context)
    {
        return new Use(getLocation(context), Optional.empty(), (Identifier) visit(context.schema));
    }

    @Override
    public Node visitCreateSchema(ImpalaSqlParser.CreateSchemaContext context)
    {
        List<Property> properties = new ArrayList<>();
        if (context.COMMENT() != null) {
            // Comment is not supported yet, give an warning message.
            String comment = ((StringLiteral) visit(context.comment)).getValue();
            addDiff(DiffType.DELETED, context.COMMENT().getText(), null, null);
            addDiff(DiffType.DELETED, comment, null, format("[COMMENT] is omitted: %s", comment));
        }

        // handle location by properties
        if (context.LOCATION() != null) {
            Identifier identifier = new Identifier("location");
            StringLiteral location = (StringLiteral) visit(context.location);
            properties.add(new Property(getLocation(context), identifier, location));

            addDiff(DiffType.INSERTED, null, "WITH", "New [with] clause");
            addDiff(DiffType.MODIFIED, location.toString(), "location = " + location.toString(), "[location] is formatted");
        }

        // if database keyword to schema keyword
        if (context.DATABASE() != null && !context.DATABASE().getText().equalsIgnoreCase("schema")) {
            addDiff(DiffType.MODIFIED, context.DATABASE().getText(), "SCHEMA", "[DATABASE] is updated to [SCHEMA]");
        }

        return new CreateSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                properties);
    }

    @Override
    public Node visitAlterSchema(ImpalaSqlParser.AlterSchemaContext context)
    {
        if (context.ALTER() != null) {
            addDiff(DiffType.UNSUPPORTED, context.ALTER().getText(), null);
        }
        if (context.DATABASE() != null) {
            addDiff(DiffType.UNSUPPORTED, context.DATABASE().getText(), "[ALTER DATABASE] is not supported");
        }
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "ALTER DATABASE", context);
    }

    @Override
    public Node visitDropSchema(ImpalaSqlParser.DropSchemaContext context)
    {
        if (context.CASCADE() != null) {
            addDiff(DiffType.UNSUPPORTED, context.CASCADE().getText(), "[CASCADE] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "CASCADE", context);
        }

        if (context.RESTRICT() == null) {
            addDiff(DiffType.INSERTED, null, "RESTRICT", "add default keyword [RESTRICT]");
        }

        return new DropSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null, false);
    }

    @Override
    public Node visitCreateTable(ImpalaSqlParser.CreateTableContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.comment)).getValue());
        }

        List<Property> properties = new ArrayList<>();

        // external table
        if (context.EXTERNAL() != null) {
            if (context.LOCATION() == null) {
                throw unsupportedError(ErrorType.SYNTAX_ERROR, "External attribute should be used with location", context);
            }
            Identifier name = new Identifier("external");
            Expression value = new Identifier("true");
            properties.add(new Property(name, value));

            addDiff(DiffType.MODIFIED, context.EXTERNAL().getText(), "external = true", "[external] is formatted");
        }

        // handle partitioned by
        List<TableElement> elements = getTableElements(context.tableElement());
        if (context.AS() == null && elements.size() == 0) {
            throw unsupportedError(ErrorType.SYNTAX_ERROR, "Create table should specify at least one column.", context);
        }
        if (context.PARTITIONED() != null) {
            List<ColumnDefinition> columnDefinitions = getColumnDefinitions(context.partitionedBy().columnDefinition());

            List<Expression> expressions = new ArrayList<>();
            Iterator<ColumnDefinition> iterator = columnDefinitions.iterator();
            while (iterator.hasNext()) {
                ColumnDefinition iter = iterator.next();
                expressions.add(new StringLiteral(iter.getName().getValue()));

                // add the partitioned_by column to table columns
                elements.add(new ColumnDefinition(iter.getName(), iter.getType(), true, emptyList(), Optional.empty()));
            }
            Expression value = new ArrayConstructor(expressions);
            properties.add(new Property(new Identifier(PARTITIONED_BY), value));

            addDiff(DiffType.MODIFIED, context.PARTITIONED().getText(), PARTITIONED_BY, "[partitioned by] is formatted");
        }

        // handle sort by
        if (context.SORT() != null) {
            List<Expression> quotedExpressions = new ArrayList<>();
            List<Expression> expressions = getExpressions(context.sortedBy().expression());
            for (int i = 0; i < expressions.size(); i++) {
                quotedExpressions.add(new StringLiteral(expressions.get(i).toString()));
            }
            Expression value = new ArrayConstructor(quotedExpressions);
            properties.add(new Property(new Identifier(SORTED_BY), value));

            addDiff(DiffType.MODIFIED, context.SORT().getText(), SORTED_BY, "[sorted by] is formatted");
        }

        // row format
        if (context.ROW() != null && context.FORMAT() != null) {
            addDiff(DiffType.UNSUPPORTED, context.ROW().getText(), "[ROW FORMAT] is not supported");
            addDiff(DiffType.UNSUPPORTED, context.FORMAT().getText(), null);
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "ROW FORMAT", context);
        }

        // serde properties
        if (context.SERDEPROPERTIES() != null) {
            addDiff(DiffType.UNSUPPORTED, context.SERDEPROPERTIES().getText(), "[WITH SERDEPROPERTIES] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "WITH SERDEPROPERTIES", context);
        }

        // stored as
        if (context.STORED_AS() != null) {
            String storedAsString = ((Identifier) visit(context.stored_as)).getValue();
            Expression value = new StringLiteral(getFileFormat(storedAsString));
            properties.add(new Property(new Identifier(FORMAT), value));

            addDiff(DiffType.MODIFIED, context.STORED_AS().getText(), FORMAT, "[stored as] is formatted");
        }

        // location
        if (context.LOCATION() != null) {
            Expression value = (StringLiteral) visit(context.location);
            properties.add(new Property(new Identifier(LOCATION), value));

            addDiff(DiffType.MODIFIED, context.LOCATION().getText(), LOCATION + " = " + value, "[location] is formatted");
        }

        // cached in
        if (context.CACHED() != null) {
            addDiff(DiffType.UNSUPPORTED, context.CACHED().getText(), "[CACHED IN] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "CACHED IN", context);
        }

        // table properties
        if (context.TBLPROPERTIES() != null) {
            List<Property> tblProperties = visit(context.tblProp.property(), Property.class);
            for (int i = 0; i < tblProperties.size(); i++) {
                Property property = tblProperties.get(i);
                if (property.getName().getValue().equalsIgnoreCase(TRANSACTIONAL)) {
                    Identifier name = new Identifier(TRANSACTIONAL);
                    Expression value = new Identifier(unquote(property.getValue().toString()));
                    properties.add(new Property(name, value));

                    addDiff(DiffType.MODIFIED, property.getName().getValue(), "transactional = ", "[transactional] is formatted");
                }
                else {
                    addDiff(DiffType.UNSUPPORTED, property.getName().getValue(), "[TBLPROPERTIES] has unsupported properties");
                    throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE,
                            property.getName().getValue(),
                            context.tblProp);
                }
            }
        }

        // create table as
        if (context.AS() != null) {
            return new CreateTableAsSelect(
                    getLocation(context),
                    getQualifiedName(context.tblName),
                    (Query) visit(context.query()),
                    context.EXISTS() != null,
                    properties,
                    true,
                    Optional.empty(),
                    comment);
        }

        return new CreateTable(
                getLocation(context),
                getQualifiedName(context.tblName),
                elements,
                context.EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitCreateTableLike(ImpalaSqlParser.CreateTableLikeContext context)
    {
        if (context.PARQUET() != null) {
            addDiff(DiffType.UNSUPPORTED, context.PARQUET().getText(), "[LIKE PARQUET] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "PARQUET", context.parquet);
        }

        // comment
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.comment)).getValue());
        }

        // like clause
        List<TableElement> elements = new ArrayList<>();
        LikeClause likeClause = new LikeClause(getQualifiedName(context.likeTableName), Optional.of(LikeClause.PropertiesOption.INCLUDING));
        elements.add(likeClause);

        List<Property> properties = new ArrayList<>();
        // external
        if (context.EXTERNAL() != null) {
            if (context.LOCATION() == null) {
                throw unsupportedError(ErrorType.SYNTAX_ERROR, "External attribute should be used with location", context);
            }
            Identifier name = new Identifier("external");
            Expression value = new Identifier("true");
            properties.add(new Property(name, value));

            addDiff(DiffType.MODIFIED, context.EXTERNAL().getText(), "external = true", "[external] is formatted");
        }

        // location
        if (context.LOCATION() != null) {
            Identifier name = new Identifier("location");
            Expression value = (StringLiteral) visit(context.location);
            properties.add(new Property(name, value));

            addDiff(DiffType.MODIFIED, context.LOCATION().getText(), LOCATION + " = " + value, "[location] is formatted");
        }

        // stored as
        if (context.STORED_AS() != null) {
            String storedAsString = ((Identifier) visit(context.stored_as)).getValue();
            Expression value = new StringLiteral(getFileFormat(storedAsString));
            properties.add(new Property(new Identifier(FORMAT), value));

            addDiff(DiffType.MODIFIED, context.STORED_AS().getText(), FORMAT, "[stored as] is formatted");
        }

        return new CreateTable(
                getLocation(context),
                getQualifiedName(context.tblName),
                elements,
                context.EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitCreateKuduTable(ImpalaSqlParser.CreateKuduTableContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.KUDU().getText(), "[CREATE KUDU TABLE] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Create Kudu Table", context);
    }

    @Override
    public Node visitCreateKuduTableAsSelect(ImpalaSqlParser.CreateKuduTableAsSelectContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.KUDU().getText(), "[CREATE KUDU TABLE] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Create Kudu Table", context);
    }

    @Override
    public Node visitRenameTable(ImpalaSqlParser.RenameTableContext context)
    {
        return new RenameTable(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to));
    }

    @Override
    public Node visitAddColumns(ImpalaSqlParser.AddColumnsContext context)
    {
        if (context.EXISTS() != null) {
            addDiff(DiffType.UNSUPPORTED, context.EXISTS().getText(), "[IF NOT EXISTS] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "[IF NOT EXISTS] is not supported", context);
        }

        if (context.columnSpecWithKudu().size() > 1) {
            // does not support add multiple column
            addDiff(DiffType.UNSUPPORTED, context.columnSpecWithKudu(1).getText(), "adding [Multiple columns] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "adding [Multiple columns] is not supported", context);
        }

        if (context.columnSpecWithKudu(0).kuduAttributes() != null) {
            addDiff(DiffType.UNSUPPORTED, context.columnSpecWithKudu(0).kuduAttributes().getText(), "[KUDU Attribute] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "[KUDU Attribute] is not supported", context);
        }

        Optional<String> comment = Optional.empty();
        if (context.columnSpecWithKudu(0).COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.columnSpecWithKudu(0).string())).getValue());
        }

        addDiff(DiffType.MODIFIED, context.COLUMNS().getText(), "COLUMN", "[COLUMNS] is modified to column");

        ColumnDefinition columnDefinition = new ColumnDefinition((Identifier) visit(context.columnSpecWithKudu(0).identifier()),
                getType(context.columnSpecWithKudu(0).type()), true, new ArrayList<>(), comment);

        return new AddColumn(getLocation(context), getQualifiedName(context.qualifiedName()), columnDefinition);
    }

    @Override
    public Node visitReplaceColumns(ImpalaSqlParser.ReplaceColumnsContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.REPLACE().getText(), "[REPLACE COLUMNS] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Replace Column", context);
    }

    @Override
    public Node visitAddSingleColumn(ImpalaSqlParser.AddSingleColumnContext context)
    {
        if (context.EXISTS() != null) {
            addDiff(DiffType.UNSUPPORTED, context.EXISTS().getText(), "[IF NOT EXISTS] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "[IF NOT EXISTS] is not supported", context);
        }

        if (context.columnSpecWithKudu().kuduAttributes() != null) {
            addDiff(DiffType.UNSUPPORTED, context.columnSpecWithKudu().kuduAttributes().getText(), "[KUDU Attribute] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "[KUDU Attribute] is not supported", context);
        }

        Optional<String> comment = Optional.empty();
        if (context.columnSpecWithKudu().COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.columnSpecWithKudu().string())).getValue());
        }

        ColumnDefinition columnDefinition = new ColumnDefinition((Identifier) visit(context.columnSpecWithKudu().identifier()),
                getType(context.columnSpecWithKudu().type()), true, new ArrayList<>(), comment);

        return new AddColumn(getLocation(context), getQualifiedName(context.qualifiedName()), columnDefinition);
    }

    @Override
    public Node visitDropSingleColumn(ImpalaSqlParser.DropSingleColumnContext context)
    {
        return new DropColumn(getLocation(context), getQualifiedName(context.qualifiedName()), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitAlterTableOwner(ImpalaSqlParser.AlterTableOwnerContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.OWNER().getText(), "[ALTER OWNER] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Alter Owner", context);
    }

    @Override
    public Node visitAlterTableKuduOnly(ImpalaSqlParser.AlterTableKuduOnlyContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.ALTER(1).getText(), "[ALTER KUDU TABLE] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Alter Kudu table", context);
    }

    @Override
    public Node visitDropTable(ImpalaSqlParser.DropTableContext context)
    {
        if (context.PURGE() != null) {
            addDiff(DiffType.UNSUPPORTED, context.PURGE().getText(), "[PURGE] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "PURGE", context);
        }

        return new DropTable(getLocation(context), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitTruncateTable(ImpalaSqlParser.TruncateTableContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.TRUNCATE().getText(), "[TRUNCATE] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Truncate", context);
    }

    @Override
    public Node visitCreateView(ImpalaSqlParser.CreateViewContext context)
    {
        if (context.EXISTS() != null) {
            addDiff(DiffType.UNSUPPORTED, context.IF().getText(), null);
            addDiff(DiffType.UNSUPPORTED, context.NOT().getText(), null);
            addDiff(DiffType.UNSUPPORTED, context.EXISTS().getText(), "[IF NOT EXISTS] is not supported");

            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "Unsupported attribute: IF NOT EXISTS", context);
        }

        if (context.viewColumns() != null) {
            for (int i = 0; i < context.viewColumns().getChildCount(); i++) {
                String text = context.viewColumns().getChild(i).getText();
                if (text.equals("(")) {
                    continue;
                }
                addDiff(DiffType.UNSUPPORTED, context.viewColumns().getChild(i).getText(), "[COLUMN ALIASES] is not supported");
                break;
            }
            throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, "Unsupported attribute: COLUMN ALIASES", context.viewColumns());
        }

        return new CreateView(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                false,
                Optional.empty());
    }

    @Override
    public Node visitAlterView(ImpalaSqlParser.AlterViewContext context)
    {
        // equal to create replace view
        if (context.viewColumns() != null) {
            for (int i = 0; i < context.viewColumns().getChildCount(); i++) {
                String text = context.viewColumns().getChild(i).getText();
                if (text.equals("(")) {
                    continue;
                }
                addDiff(DiffType.UNSUPPORTED, context.viewColumns().getChild(i).getText(), "[COLUMN ALIASES] is not supported");
                break;
            }
            throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, "Unsupported attribute: COLUMN ALIASES", context.viewColumns());
        }

        // it equals to create or replace
        addDiff(DiffType.INSERTED, "OR REPLACE", "[OR REPLACE] is added");

        return new CreateView(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                true,
                Optional.empty());
    }

    @Override
    public Node visitRenameView(ImpalaSqlParser.RenameViewContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.RENAME().getText(), "[RENAME VIEW] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Rename View", context);
    }

    @Override
    public Node visitAlterViewOwner(ImpalaSqlParser.AlterViewOwnerContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.OWNER().getText(), "[ALTER VIEW OWNER] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Alter View Owner", context);
    }

    @Override
    public Node visitDropView(ImpalaSqlParser.DropViewContext context)
    {
        return new DropView(getLocation(context), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitDescribeDbOrTable(ImpalaSqlParser.DescribeDbOrTableContext context)
    {
        if (context.DATABASE() != null) {
            addDiff(DiffType.UNSUPPORTED, context.DATABASE().getText(), "[DESCRIBE DATABASE] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "DESCRIBE DATABASE", context);
        }

        if (context.FORMATTED() != null || context.EXTENDED() != null) {
            addDiff(DiffType.UNSUPPORTED, context.FORMATTED() != null ? context.FORMATTED().getText() : context.EXTENDED().getText(),
                    null, "[FORMATTED or EXTENDED] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "Unsupported attribute: FORMATTED or EXTENDED", context);
        }
        addDiff(DiffType.MODIFIED, context.DESCRIBE().getText(), "SHOW COLUMNS", "[DESCRIBE] is formatted to SHOW COLUMNS");
        return new ShowColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitComputeStats(ImpalaSqlParser.ComputeStatsContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.COMPUTE().getText(), "[COMPUTE STATS] is not supported");
        addDiff(DiffType.UNSUPPORTED, context.STATS().getText(), null);

        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "COMPUTE STATS", context);
    }

    @Override
    public Node visitComputeIncrementalStats(ImpalaSqlParser.ComputeIncrementalStatsContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.COMPUTE().getText(), "[COMPUTE INCREMENTAL STATS] is not supported");
        addDiff(DiffType.UNSUPPORTED, context.INCREMENTAL().getText(), null);
        addDiff(DiffType.UNSUPPORTED, context.STATS().getText(), null);

        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "COMPUTE INCREMENTAL STATS", context);
    }

    @Override
    public Node visitDropStats(ImpalaSqlParser.DropStatsContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.DROP().getText(), "[DROP STATS] is not supported");
        addDiff(DiffType.UNSUPPORTED, context.STATS().getText(), "[DROP STATS] is not supported");

        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "DROP STATS", context);
    }

    @Override
    public Node visitDropIncrementalStats(ImpalaSqlParser.DropIncrementalStatsContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.DROP().getText(), "[DROP INCREMENTAL STATS] is not supported");
        addDiff(DiffType.UNSUPPORTED, context.INCREMENTAL().getText(), null);
        addDiff(DiffType.UNSUPPORTED, context.STATS().getText(), null);

        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "DROP INCREMENTAL STATS", context);
    }

    @Override
    public Node visitCreateFunction(ImpalaSqlParser.CreateFunctionContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.FUNCTION().getText(), "[CREATE FUNCTION] is not supported");

        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "CREATE FUNCTION", context);
    }

    @Override
    public Node visitRefreshFunction(ImpalaSqlParser.RefreshFunctionContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.REFRESH().getText(), "[REFRESH FUNCTIONS] is not supported");
        addDiff(DiffType.UNSUPPORTED, context.FUNCTIONS().getText(), null);

        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "REFRESH FUNCTIONS", context);
    }

    @Override
    public Node visitDropFunction(ImpalaSqlParser.DropFunctionContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.DROP().getText(), "[DROP FUNCTION] is not supported");
        addDiff(DiffType.UNSUPPORTED, context.FUNCTION().getText(), null);

        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "DROP FUNCTION", context);
    }

    @Override
    public Node visitCreateRole(ImpalaSqlParser.CreateRoleContext context)
    {
        return new CreateRole(
                getLocation(context),
                (Identifier) visit(context.name),
                Optional.empty());
    }

    @Override
    public Node visitDropRole(ImpalaSqlParser.DropRoleContext context)
    {
        return new DropRole(
                getLocation(context),
                (Identifier) visit(context.name));
    }

    @Override
    public Node visitGrantRole(ImpalaSqlParser.GrantRoleContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.GROUP().getText(), "GRANT ROLE TO [GROUP] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "GRANT ROLE TO GROUP", context);
    }

    @Override
    public Node visitGrant(ImpalaSqlParser.GrantContext context)
    {
        Optional<List<String>> privileges;
        if (context.ALL() != null) {
            addDiff(DiffType.UNSUPPORTED, context.ALL().getText(), "GRANT [ALL] is not supported, Because some privileges are not supported. e.g. REFRESH");
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: GRANT ALL. " +
                    "Because some privileges are not supported. e.g. REFRESH", context);
        }

        if (context.privilege().size() > 0) {
            for (ImpalaSqlParser.PrivilegeContext privilegeContext : context.privilege()) {
                if (privilegeContext.REFRESH() != null) {
                    addDiff(DiffType.UNSUPPORTED, privilegeContext.REFRESH().getText(), "[REFRESH] is not supported");
                    throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: GRANT REFRESH. ", context);
                }
                else if (privilegeContext.CREATE() != null) {
                    addDiff(DiffType.UNSUPPORTED, privilegeContext.CREATE().getText(), "GRANT [CREATE] is not supported");
                    throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: GRANT CREATE. ", context);
                }
                else if (privilegeContext.columnName != null) {
                    addDiff(DiffType.UNSUPPORTED, privilegeContext.SELECT().getText(), "GRANT [SELECT(column_name)] is not supported");
                    throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: GRANT SELECT(column_name). ", context);
                }
            }
        }

        if (!context.objectType().getText().equalsIgnoreCase("table")) {
            addDiff(DiffType.UNSUPPORTED, context.objectType().getText(),
                    format("GRANT ON [%s] is not supported", context.objectType().getText().toUpperCase()));
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: GRANT only support 'ON TABLE'. ", context);
        }

        privileges = Optional.of(context.privilege().stream()
                .map(ImpalaSqlParser.PrivilegeContext::getText)
                .collect(toList()));

        return new Grant(
                getLocation(context),
                privileges,
                true,
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.grantee),
                context.OPTION() != null);
    }

    @Override
    public Node visitRevokeRole(ImpalaSqlParser.RevokeRoleContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.GROUP().getText(), "REVOKE ROLE FROM [GROUP] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "REVOKE ROLE FROM GROUP", context);
    }

    @Override
    public Node visitRevoke(ImpalaSqlParser.RevokeContext context)
    {
        Optional<List<String>> privileges;
        if (context.ALL() != null) {
            addDiff(DiffType.UNSUPPORTED, context.ALL().getText(), "REVOKE [ALL] is not supported, Because some privileges are not supported. e.g. REFRESH");

            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: REVOKE ALL. " +
                    "Because some privileges are not supported. e.g. REFRESH", context);
        }

        if (context.privilege().size() > 0) {
            for (ImpalaSqlParser.PrivilegeContext privilegeContext : context.privilege()) {
                if (privilegeContext.REFRESH() != null) {
                    addDiff(DiffType.UNSUPPORTED, privilegeContext.REFRESH().getText(), "[REFRESH] is not supported");
                    throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: REVOKE REFRESH. ", context);
                }
                else if (privilegeContext.CREATE() != null) {
                    addDiff(DiffType.UNSUPPORTED, privilegeContext.CREATE().getText(), "[CREATE] is not supported");
                    throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: REVOKE CREATE. ", context);
                }
                else if (privilegeContext.columnName != null) {
                    addDiff(DiffType.UNSUPPORTED, privilegeContext.SELECT().getText(), "[SELECT(column_name)] is not supported");
                    throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: REVOKE SELECT(column_name). ", context);
                }
            }
        }

        if (!context.objectType().getText().equalsIgnoreCase("table")) {
            addDiff(DiffType.UNSUPPORTED, context.objectType().getText(),
                    format("REVOKE ON [%s] is not supported", context.objectType().getText().toUpperCase()));
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "Unsupported attribute: REVOKE only support 'ON TABLE'. ", context);
        }

        privileges = Optional.of(context.privilege().stream()
                .map(ImpalaSqlParser.PrivilegeContext::getText)
                .collect(toList()));

        return new Revoke(
                getLocation(context),
                context.OPTION() != null,
                privileges,
                true,
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.grantee));
    }

    @Override
    public Node visitInsertInto(ImpalaSqlParser.InsertIntoContext context)
    {
        if (context.with() != null) {
            addDiff(DiffType.UNSUPPORTED, context.with().getChild(0).getText(), "[WITH] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "Unsupported attribute: INSERT does not support WITH.", context);
        }

        if (context.hintClause().size() > 0) {
            addDiff(DiffType.UNSUPPORTED, context.hintClause(0).getText(), "[hint] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, "Unsupported attribute: INSERT does not support hint.", context);
        }

        if (context.PARTITION() != null) {
            addDiff(DiffType.UNSUPPORTED, context.PARTITION().getText(), "[PARTITION] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, "Unsupported attribute: INSERT does not support PARTITION.", context);
        }

        Optional<List<Identifier>> columnAliases = Optional.empty();
        if (context.columnAliases() != null) {
            columnAliases = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new Insert(
                getQualifiedName(context.qualifiedName()),
                columnAliases,
                (Query) visit(context.query()),
                context.OVERWRITE() != null);
    }

    @Override
    public Node visitDelete(ImpalaSqlParser.DeleteContext context)
    {
        return new Delete(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitDeleteTableRef(ImpalaSqlParser.DeleteTableRefContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.relation(0).getText(), "DELETE TABLE WITH [REFERENCE AND JOIN] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "DELETE TABLE WITH REFERENCE AND JOIN.", context);
    }

    @Override
    public Node visitUpdateTable(ImpalaSqlParser.UpdateTableContext context)
    {
        if (context.FROM() != null) {
            addDiff(DiffType.UNSUPPORTED, context.FROM().getText(), "UPDATE WITH [RELATION] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "UPDATE with relation", context);
        }

        List<AssignmentItem> assignmentItems = ImmutableList.of();
        if (context.assignmentList() != null) {
            assignmentItems = visit(context.assignmentList().assignmentItem(), AssignmentItem.class);
        }

        addDiff(DiffType.FUNCTION_WARNING, context.UPDATE().getText(), "UPDATE ",
                "This UPDATE might not work, although syntax is correct. Because openLooKeng currently only support executing UPDATE on specified format.");

        return new Update(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                assignmentItems,
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitUpsert(ImpalaSqlParser.UpsertContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.UPSERT().getText(), "[UPSERT] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "UPSERT", context);
    }

    @Override
    public Node visitShowSchemas(ImpalaSqlParser.ShowSchemasContext context)
    {
        if (context.string().size() > 1) {
            addDiff(DiffType.UNSUPPORTED, context.string(1).getText(), "[LIKE] does not support multiple matchers");
            throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, "Unsupported attribute: LIKE can not have multiple matchers ", context);
        }
        Optional<String> escape = Optional.empty();
        Optional<String> pattern = getTextIfPresent(context.pattern).map(ImpalaAstBuilder::unquote);
        if (pattern.isPresent()) {
            if (pattern.get().contains("_")) {
                pattern = Optional.of(pattern.get().replace("_", "#_"));
                escape = Optional.of("#");

                addDiff(DiffType.MODIFIED, "_", "#_", "[_] is formatted to #_");
                addDiff(DiffType.INSERTED, null, "ESCAPE", "New [ESCAPE] clause");
            }

            addDiff(DiffType.MODIFIED, "*", "%", "[*] is formatted to %");
            pattern = Optional.of(pattern.get().replace("*", "%"));
        }

        if (context.DATABASES() != null) {
            addDiff(DiffType.MODIFIED, context.DATABASES().getText(), "SCHEMAS", "[DATABASES] is updated to SCHEMAS");
        }

        return new ShowSchemas(getLocation(context), Optional.empty(), pattern, escape);
    }

    @Override
    public Node visitShowTables(ImpalaSqlParser.ShowTablesContext context)
    {
        if (context.string().size() > 1) {
            addDiff(DiffType.UNSUPPORTED, context.string(1).getText(), "[LIKE] does not support multiple patter");
            throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, "Unsupported attribute: LIKE can not have multiple pattern ", context);
        }

        Optional<String> escape = Optional.empty();
        Optional<String> pattern = getTextIfPresent(context.pattern).map(ImpalaAstBuilder::unquote);
        if (pattern.isPresent()) {
            if (pattern.get().contains("_")) {
                pattern = Optional.of(pattern.get().replace("_", "#_"));
                escape = Optional.of("#");

                addDiff(DiffType.MODIFIED, "_", "#_", "[_] is formatted to #_");
                addDiff(DiffType.INSERTED, null, "ESCAPE", "New [ESCAPE] clause");
            }

            addDiff(DiffType.MODIFIED, "*", "%", "[*] is formatted to %");
            pattern = Optional.of(pattern.get().replace("*", "%"));
        }
        return new ShowTables(
                getLocation(context),
                Optional.ofNullable(context.qualifiedName())
                        .map(this::getQualifiedName),
                pattern,
                escape);
    }

    @Override
    public Node visitShowFunctions(ImpalaSqlParser.ShowFunctionsContext context)
    {
        if (context.AGGREGATE() != null) {
            addDiff(DiffType.UNSUPPORTED, context.AGGREGATE().getText(), "[AGGREGATE] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "AGGREGATE is not supported", context);
        }
        if (context.ANALYTIC() != null) {
            addDiff(DiffType.UNSUPPORTED, context.ANALYTIC().getText(), "[ANALYTIC] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "ANALYTIC is not supported", context);
        }
        if (context.IN() != null) {
            addDiff(DiffType.UNSUPPORTED, context.IN().getText(), "[IN] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "IN is not supported", context);
        }

        return new ShowFunctions(getLocation(context),
                getTextIfPresent(context.pattern).map(ImpalaAstBuilder::unquote),
                Optional.empty());
    }

    @Override
    public Node visitShowCreateTable(ImpalaSqlParser.ShowCreateTableContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.TABLE, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowCreateView(ImpalaSqlParser.ShowCreateViewContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.VIEW, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowTableStats(ImpalaSqlParser.ShowTableStatsContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.STATS().getText(), "[SHOW TABLE STATS] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SHOW TABLE STATS", context);
    }

    @Override
    public Node visitShowColumnStats(ImpalaSqlParser.ShowColumnStatsContext context)
    {
        return new ShowStats(Optional.of(getLocation(context)), new Table(getQualifiedName(context.qualifiedName())));
    }

    @Override
    public Node visitShowPartitions(ImpalaSqlParser.ShowPartitionsContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.PARTITIONS().getText(), "[SHOW PARTITIONS] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SHOW PARTITIONS", context);
    }

    @Override
    public Node visitShowFiles(ImpalaSqlParser.ShowFilesContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.FILES().getText(), "[SHOW FILES] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SHOW FILES", context);
    }

    @Override
    public Node visitShowRoles(ImpalaSqlParser.ShowRolesContext context)
    {
        return new ShowRoles(
                getLocation(context),
                Optional.empty(),
                context.CURRENT() != null);
    }

    @Override
    public Node visitShowRoleGrant(ImpalaSqlParser.ShowRoleGrantContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.GROUP().getText(), "SHOW ROLE GRANT [GROUP] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SHOW ROLE GRANT GROUP", context);
    }

    @Override
    public Node visitShowGrantRole(ImpalaSqlParser.ShowGrantRoleContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.ROLE().getText(), "SHOW [GRANT ROLE] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SHOW GRANT ROLE", context);
    }

    @Override
    public Node visitShowGrantUser(ImpalaSqlParser.ShowGrantUserContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.USER().getText(), "SHOW [GRANT USER] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SHOW GRANT USER", context);
    }

    @Override
    public Node visitAddComments(ImpalaSqlParser.AddCommentsContext context)
    {
        if (context.DATABASE() != null || context.COLUMN() != null) {
            addDiff(DiffType.UNSUPPORTED,
                    context.DATABASE() != null ? context.DATABASE().getText() : context.COLUMN().getText(),
                    "COMMENT ON [DATABASE/COLUMN] is not supported");

            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "COMMENT ON DATABASE/COLUMN", context);
        }

        Optional<String> comment = Optional.empty();
        if (context.NULL() == null) {
            comment = Optional.of(unquote(context.string().getText()));
        }
        return new Comment(getLocation(context), Comment.Type.TABLE, getQualifiedName(context.qualifiedName()), comment);
    }

    @Override
    public Node visitExplain(ImpalaSqlParser.ExplainContext context)
    {
        return new Explain(getLocation(context), false, false, (Statement) visit(context.statement()), ImmutableList.of());
    }

    @Override
    public Node visitSetSession(ImpalaSqlParser.SetSessionContext context)
    {
        if (context.identifier() != null) {
            addDiff(DiffType.UNSUPPORTED, context.identifier().getText(), "SET [IDENTIFIER] is not support");
            throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SET QUERY OPTION", context);
        }
        addDiff(DiffType.MODIFIED, context.SET().getText(), "SHOW", "[SET] is formatted to SHOW");
        return new ShowSession(getLocation(context));
    }

    @Override
    public Node visitShutdown(ImpalaSqlParser.ShutdownContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.SHUTDOWN().getText(), "[SHUTDOWN] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "SHUTDOWN", context);
    }

    @Override
    public Node visitInvalidateMeta(ImpalaSqlParser.InvalidateMetaContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.INVALIDATE().getText(), "[INVALIDATE METADATA] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "INVALIDATE METADATA", context);
    }

    @Override
    public Node visitLoadData(ImpalaSqlParser.LoadDataContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.LOAD().getText(), "[LOAD DATA] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "LOAD DATA", context);
    }

    @Override
    public Node visitRefreshMeta(ImpalaSqlParser.RefreshMetaContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.REFRESH().getText(), "[REFRESH METADATA] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "REFRESH", context);
    }

    @Override
    public Node visitRefreshAuth(ImpalaSqlParser.RefreshAuthContext context)
    {
        addDiff(DiffType.UNSUPPORTED, context.REFRESH().getText(), "[REFRESH AUTHORIZATION] is not supported");
        throw unsupportedError(ErrorType.UNSUPPORTED_STATEMENT, "REFRESH AUTHORIZATION", context);
    }

    @Override
    public Node visitAssignmentItem(ImpalaSqlParser.AssignmentItemContext context)
    {
        return new AssignmentItem(getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Expression) visit(context.expression()));
    }

    @Override
    public Node visitQuery(ImpalaSqlParser.QueryContext context)
    {
        Query body = (Query) visit(context.queryNoWith());

        return new Query(
                getLocation(context),
                visitIfPresent(context.with(), With.class),
                body.getQueryBody(),
                body.getOrderBy(),
                body.getOffset(),
                body.getLimit());
    }

    @Override
    public Node visitWith(ImpalaSqlParser.WithContext context)
    {
        return new With(getLocation(context), false, visit(context.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitColumnDefinition(ImpalaSqlParser.ColumnDefinitionContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        List<Property> properties = ImmutableList.of();

        return new ColumnDefinition(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                getType(context.type()),
                true,
                properties,
                comment);
    }

    @Override
    public Node visitColumnSpecWithKudu(ImpalaSqlParser.ColumnSpecWithKuduContext context)
    {
        throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "column Specification with Kudu", context);
    }

    @Override
    public Node visitKuduAttributes(ImpalaSqlParser.KuduAttributesContext context)
    {
        throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "kudu Attributes", context);
    }

    @Override
    public Node visitHintClause(ImpalaSqlParser.HintClauseContext ctx)
    {
        return super.visitHintClause(ctx);
    }

    @Override
    public Node visitProperties(ImpalaSqlParser.PropertiesContext ctx)
    {
        return super.visitProperties(ctx);
    }

    @Override
    public Node visitPartitionedBy(ImpalaSqlParser.PartitionedByContext ctx)
    {
        return super.visitPartitionedBy(ctx);
    }

    @Override
    public Node visitSortedBy(ImpalaSqlParser.SortedByContext ctx)
    {
        return super.visitSortedBy(ctx);
    }

    @Override
    public Node visitRowFormat(ImpalaSqlParser.RowFormatContext context)
    {
        throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "ROW FORMAT", context);
    }

    @Override
    public Node visitProperty(ImpalaSqlParser.PropertyContext context)
    {
        return new Property(getLocation(context), (Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitQueryNoWith(ImpalaSqlParser.QueryNoWithContext context)
    {
        QueryBody term = (QueryBody) visit(context.queryTerm());

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        Optional<Node> limit = Optional.empty();
        Optional<Offset> offset = Optional.empty();
        if (context.LIMIT() != null) {
            Optional<String> offsetValue = getTextIfPresent(context.offset);
            Optional<String> rowsValue = getTextIfPresent(context.rows);
            if (offsetValue.isPresent()) {
                offset = Optional.of(new Offset(offsetValue.get()));
            }

            if (rowsValue.isPresent()) {
                limit = Optional.of(new Limit(rowsValue.get()));
            }
        }

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by, offset, limit or fetch,
            // fold the order by, limit, offset or fetch clauses
            // into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                    getLocation(context),
                    Optional.empty(),
                    new QuerySpecification(
                            getLocation(context),
                            query.getSelect(),
                            query.getFrom(),
                            query.getWhere(),
                            query.getGroupBy(),
                            query.getHaving(),
                            orderBy,
                            offset,
                            limit),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        return new Query(
                getLocation(context),
                Optional.empty(),
                term,
                orderBy,
                offset,
                limit);
    }

    @Override
    public Node visitSetOperation(ImpalaSqlParser.SetOperationContext context)
    {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        boolean distinct = context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;

        switch (context.operator.getType()) {
            case ImpalaSqlLexer.UNION:
                return new Union(getLocation(context.UNION()), ImmutableList.of(left, right), distinct);
            case ImpalaSqlLexer.INTERSECT:
                return new Intersect(getLocation(context.INTERSECT()), ImmutableList.of(left, right), distinct);
            case ImpalaSqlLexer.EXCEPT:
                return new Except(getLocation(context.EXCEPT()), left, right, distinct);
        }

        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public Node visitTable(ImpalaSqlParser.TableContext context)
    {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitInlineTable(ImpalaSqlParser.InlineTableContext context)
    {
        addDiff(DiffType.INSERTED, null, "ROW (", "[ROW] is a new keyword");
        return new Values(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitSubquery(ImpalaSqlParser.SubqueryContext context)
    {
        return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
    }

    @Override
    public Node visitSortItem(ImpalaSqlParser.SortItemContext context)
    {
        return new SortItem(
                getLocation(context),
                (Expression) visit(context.expression()),
                Optional.ofNullable(context.ordering)
                        .map(ImpalaAstBuilder::getOrderingType)
                        .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(context.nullOrdering)
                        .map(ImpalaAstBuilder::getNullOrderingType)
                        .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitQuerySpecification(ImpalaSqlParser.QuerySpecificationContext context)
    {
        Optional<Relation> from = Optional.empty();
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

        List<Relation> relations = visit(context.relation(), Relation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(), Optional.empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(
                getLocation(context),
                new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
                from,
                visitIfPresent(context.where, Expression.class),
                visitIfPresent(context.groupBy(), GroupBy.class),
                visitIfPresent(context.having, Expression.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitGroupBy(ImpalaSqlParser.GroupByContext context)
    {
        return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()), visit(context.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(ImpalaSqlParser.SingleGroupingSetContext context)
    {
        return new SimpleGroupBy(getLocation(context), visit(context.groupingSet().expression(), Expression.class));
    }

    @Override
    public Node visitNamedQuery(ImpalaSqlParser.NamedQueryContext context)
    {
        Optional<List<Identifier>> columns = Optional.empty();
        if (context.columnAliases() != null) {
            columns = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new WithQuery(
                getLocation(context),
                (Identifier) visit(context.name),
                (Query) visit(context.query()),
                columns);
    }

    @Override
    public Node visitSelectSingle(ImpalaSqlParser.SelectSingleContext context)
    {
        return new SingleColumn(
                getLocation(context),
                (Expression) visit(context.expression()),
                visitIfPresent(context.identifier(), Identifier.class));
    }

    @Override
    public Node visitSelectAll(ImpalaSqlParser.SelectAllContext context)
    {
        if (context.qualifiedName() != null) {
            return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
        }

        return new AllColumns(getLocation(context));
    }

    @Override
    public Node visitJoinRelation(ImpalaSqlParser.JoinRelationContext context)
    {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.empty());
        }

        if (context.joinType().SEMI() != null) {
            addDiff(DiffType.UNSUPPORTED, context.joinType().SEMI().getText(), "[SEMI] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "SEMI", context);
        }

        if (context.joinType().ANTI() != null) {
            addDiff(DiffType.UNSUPPORTED, context.joinType().ANTI().getText(), "[ANTI] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "ANTI", context);
        }

        if (context.joinType().INNER() != null &&
                (context.joinType().LEFT() != null || context.joinType().RIGHT() != null)) {
            addDiff(DiffType.UNSUPPORTED, context.joinType().INNER().getText(), "[LEFT INNER || RIGHT INNER] is not supported");
            throw unsupportedError(ErrorType.UNSUPPORTED_KEYWORDS, "LEFT INNER || RIGHT INNER", context);
        }

        JoinCriteria criteria;
        right = (Relation) visit(context.rightRelation);
        if (context.joinCriteria().ON() != null) {
            criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
        }
        else if (context.joinCriteria().USING() != null) {
            criteria = new JoinUsing(visit(context.joinCriteria().identifier(), Identifier.class));
        }
        else {
            throw new IllegalArgumentException("Unsupported join criteria");
        }

        Join.Type joinType;
        if (context.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        }
        else if (context.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        }
        else if (context.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        }
        else {
            joinType = Join.Type.INNER;
        }

        return new Join(getLocation(context), joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(ImpalaSqlParser.SampledRelationContext context)
    {
        Relation child = (Relation) visit(context.aliasedRelation());

        if (context.TABLESAMPLE() == null) {
            return child;
        }

        return new SampledRelation(
                getLocation(context),
                child,
                getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
                (Expression) visit(context.percentage));
    }

    @Override
    public Node visitAliasedRelation(ImpalaSqlParser.AliasedRelationContext context)
    {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        List<Identifier> aliases = null;
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(getLocation(context), child, (Identifier) visit(context.identifier()), aliases);
    }

    @Override
    public Node visitTableName(ImpalaSqlParser.TableNameContext context)
    {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(ImpalaSqlParser.SubqueryRelationContext context)
    {
        return new TableSubquery(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitUnnest(ImpalaSqlParser.UnnestContext context)
    {
        return new Unnest(getLocation(context), visit(context.expression(), Expression.class), context.ORDINALITY() != null);
    }

    @Override
    public Node visitLateral(ImpalaSqlParser.LateralContext context)
    {
        return new Lateral(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitParenthesizedRelation(ImpalaSqlParser.ParenthesizedRelationContext context)
    {
        return visit(context.relation());
    }

    @Override
    public Node visitLogicalNot(ImpalaSqlParser.LogicalNotContext context)
    {
        return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitPredicated(ImpalaSqlParser.PredicatedContext context)
    {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Node visitLogicalBinary(ImpalaSqlParser.LogicalBinaryContext context)
    {
        return new LogicalBinaryExpression(
                getLocation(context.operator),
                getLogicalBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitComparison(ImpalaSqlParser.ComparisonContext context)
    {
        return new ComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitQuantifiedComparison(ImpalaSqlParser.QuantifiedComparisonContext context)
    {
        return new QuantifiedComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                getComparisonQuantifier(((TerminalNode) context.comparisonQuantifier().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context.query()), (Query) visit(context.query())));
    }

    @Override
    public Node visitBetween(ImpalaSqlParser.BetweenContext context)
    {
        Expression expression = new BetweenPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.lower),
                (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitInList(ImpalaSqlParser.InListContext context)
    {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new InListExpression(getLocation(context), visit(context.expression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInSubquery(ImpalaSqlParser.InSubqueryContext context)
    {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitLike(ImpalaSqlParser.LikeContext context)
    {
        Expression result = new LikePredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.pattern),
                visitIfPresent(context.escape, Expression.class));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitNullPredicate(ImpalaSqlParser.NullPredicateContext context)
    {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(getLocation(context), child);
        }

        return new IsNotNullPredicate(getLocation(context), child);
    }

    @Override
    public Node visitDistinctFrom(ImpalaSqlParser.DistinctFromContext context)
    {
        Expression expression = new ComparisonExpression(
                getLocation(context),
                ComparisonExpression.Operator.IS_DISTINCT_FROM,
                (Expression) visit(context.value),
                (Expression) visit(context.right));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitConcatenation(ImpalaSqlParser.ConcatenationContext context)
    {
        return new FunctionCall(
                getLocation(context.CONCAT()),
                QualifiedName.of("concat"), ImmutableList.of(
                (Expression) visit(context.left),
                (Expression) visit(context.right)));
    }

    @Override
    public Node visitArithmeticBinary(ImpalaSqlParser.ArithmeticBinaryContext context)
    {
        return new ArithmeticBinaryExpression(
                getLocation(context.operator),
                getArithmeticBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitArithmeticUnary(ImpalaSqlParser.ArithmeticUnaryContext context)
    {
        Expression child = (Expression) visit(context.valueExpression());

        switch (context.operator.getType()) {
            case ImpalaSqlLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(context), child);
            case ImpalaSqlLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(context), child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public Node visitDereference(ImpalaSqlParser.DereferenceContext context)
    {
        return new DereferenceExpression(
                getLocation(context),
                (Expression) visit(context.base),
                (Identifier) visit(context.fieldName));
    }

    @Override
    public Node visitTypeConstructor(ImpalaSqlParser.TypeConstructorContext context)
    {
        String value = ((StringLiteral) visit(context.string())).getValue();

        if (context.DOUBLE_PRECISION() != null) {
            return new GenericLiteral(getLocation(context), "DOUBLE", value);
        }

        String type = context.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(context), value);
        }

        return new GenericLiteral(getLocation(context), type, value);
    }

    @Override
    public Node visitSpecialDateTimeFunction(ImpalaSqlParser.SpecialDateTimeFunctionContext context)
    {
        CurrentTime.Function function = getDateTimeFunctionType(context.name);
        return new CurrentTime(getLocation(context), function);
    }

    @Override
    public Node visitSubstring(ImpalaSqlParser.SubstringContext context)
    {
        return new FunctionCall(getLocation(context), QualifiedName.of("substr"), visit(context.valueExpression(), Expression.class));
    }

    @Override
    public Node visitCast(ImpalaSqlParser.CastContext context)
    {
        boolean isTryCast = context.TRY_CAST() != null;
        return new Cast(getLocation(context), (Expression) visit(context.expression()), getType(context.type()), isTryCast);
    }

    @Override
    public Node visitLambda(ImpalaSqlParser.LambdaContext context)
    {
        List<LambdaArgumentDeclaration> arguments = visit(context.identifier(), Identifier.class).stream()
                .map(LambdaArgumentDeclaration::new)
                .collect(toList());

        Expression body = (Expression) visit(context.expression());

        return new LambdaExpression(getLocation(context), arguments, body);
    }

    @Override
    public Node visitParenthesizedExpression(ImpalaSqlParser.ParenthesizedExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Node visitParameter(ImpalaSqlParser.ParameterContext context)
    {
        Parameter parameter = new Parameter(getLocation(context), parameterPosition);
        parameterPosition++;
        return parameter;
    }

    @Override
    public Node visitNormalize(ImpalaSqlParser.NormalizeContext context)
    {
        Expression str = (Expression) visit(context.valueExpression());
        String normalForm = Optional.ofNullable(context.normalForm()).map(ParserRuleContext::getText).orElse("NFC");
        return new FunctionCall(getLocation(context), QualifiedName.of("normalize"), ImmutableList.of(str, new StringLiteral(getLocation(context), normalForm)));
    }

    @Override
    public Node visitSimpleCase(ImpalaSqlParser.SimpleCaseContext context)
    {
        return new SimpleCaseExpression(
                getLocation(context),
                (Expression) visit(context.valueExpression()),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitColumnReference(ImpalaSqlParser.ColumnReferenceContext context)
    {
        return visit(context.identifier());
    }

    @Override
    public Node visitNullLiteral(ImpalaSqlParser.NullLiteralContext context)
    {
        return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitRowConstructor(ImpalaSqlParser.RowConstructorContext context)
    {
        return new Row(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitSubscript(ImpalaSqlParser.SubscriptContext context)
    {
        return new SubscriptExpression(getLocation(context), (Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitSubqueryExpression(ImpalaSqlParser.SubqueryExpressionContext context)
    {
        return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitBinaryLiteral(ImpalaSqlParser.BinaryLiteralContext context)
    {
        String raw = context.BINARY_LITERAL().getText();
        return new BinaryLiteral(getLocation(context), unquote(raw.substring(1)));
    }

    @Override
    public Node visitExtract(ImpalaSqlParser.ExtractContext context)
    {
        String fieldString = context.identifier().getText();
        Extract.Field field;
        field = Extract.Field.valueOf(fieldString.toUpperCase(Locale.ROOT));
        return new Extract(getLocation(context), (Expression) visit(context.valueExpression()), field);
    }

    @Override
    public Node visitArrayConstructor(ImpalaSqlParser.ArrayConstructorContext context)
    {
        return new ArrayConstructor(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitFunctionCall(ImpalaSqlParser.FunctionCallContext context)
    {
        Optional<Expression> filter = visitIfPresent(context.filter(), Expression.class);
        Optional<Window> window = visitIfPresent(context.over(), Window.class);

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(visit(context.sortItem(), SortItem.class)));
        }

        QualifiedName name = getQualifiedName(context.qualifiedName());

        boolean distinct = isDistinct(context.setQuantifier());

        if (name.toString().equalsIgnoreCase("if")) {
            check(context.expression().size() == 2 || context.expression().size() == 3, "Invalid number of arguments for 'if' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", context);
            check(!distinct, "DISTINCT not valid for 'if' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'if' function", context);

            Expression elseExpression = null;
            if (context.expression().size() == 3) {
                elseExpression = (Expression) visit(context.expression(2));
            }

            return new IfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)),
                    elseExpression);
        }

        if (name.toString().equalsIgnoreCase("nullif")) {
            check(context.expression().size() == 2, "Invalid number of arguments for 'nullif' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
            check(!distinct, "DISTINCT not valid for 'nullif' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'nullif' function", context);

            return new NullIfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)));
        }

        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(context.expression().size() >= 2, "The 'coalesce' function must have at least two arguments", context);
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'coalesce' function", context);

            return new CoalesceExpression(getLocation(context), visit(context.expression(), Expression.class));
        }

        return new FunctionCall(
                Optional.of(getLocation(context)),
                getQualifiedName(context.qualifiedName()),
                window,
                filter,
                orderBy,
                distinct,
                false,
                visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitExists(ImpalaSqlParser.ExistsContext context)
    {
        return new ExistsPredicate(getLocation(context), new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
    }

    @Override
    public Node visitPosition(ImpalaSqlParser.PositionContext context)
    {
        List<Expression> arguments = Lists.reverse(visit(context.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(context), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitSearchedCase(ImpalaSqlParser.SearchedCaseContext context)
    {
        return new SearchedCaseExpression(
                getLocation(context),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitGroupingOperation(ImpalaSqlParser.GroupingOperationContext context)
    {
        List<QualifiedName> arguments = context.qualifiedName().stream()
                .map(this::getQualifiedName)
                .collect(toList());

        return new GroupingOperation(Optional.of(getLocation(context)), arguments);
    }

    @Override
    public Node visitBasicStringLiteral(ImpalaSqlParser.BasicStringLiteralContext context)
    {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitBooleanValue(ImpalaSqlParser.BooleanValueContext context)
    {
        return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitInterval(ImpalaSqlParser.IntervalContext context)
    {
        return new IntervalLiteral(
                getLocation(context),
                context.INTEGER_VALUE().getText(),
                IntervalLiteral.Sign.POSITIVE,
                getIntervalFieldType((Token) context.intervalField().getChild(0).getPayload()),
                Optional.empty());
    }

    @Override
    public Node visitWhenClause(ImpalaSqlParser.WhenClauseContext context)
    {
        return new WhenClause(getLocation(context), (Expression) visit(context.condition), (Expression) visit(context.result));
    }

    @Override
    public Node visitFilter(ImpalaSqlParser.FilterContext context)
    {
        return visit(context.booleanExpression());
    }

    @Override
    public Node visitOver(ImpalaSqlParser.OverContext context)
    {
        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        return new Window(
                getLocation(context),
                visit(context.partition, Expression.class),
                orderBy,
                visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitWindowFrame(ImpalaSqlParser.WindowFrameContext context)
    {
        return new WindowFrame(
                getLocation(context),
                getFrameType(context.frameType),
                (FrameBound) visit(context.start),
                visitIfPresent(context.end, FrameBound.class));
    }

    @Override
    public Node visitUnboundedFrame(ImpalaSqlParser.UnboundedFrameContext context)
    {
        return new FrameBound(getLocation(context), getUnboundedFrameBoundType(context.boundType));
    }

    @Override
    public Node visitCurrentRowBound(ImpalaSqlParser.CurrentRowBoundContext context)
    {
        return new FrameBound(getLocation(context), FrameBoundType.CURRENT_ROW);
    }

    @Override
    public Node visitBoundedFrame(ImpalaSqlParser.BoundedFrameContext context)
    {
        return new FrameBound(getLocation(context), getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitQualifiedArgument(ImpalaSqlParser.QualifiedArgumentContext context)
    {
        return new PathElement(getLocation(context), (Identifier) visit(context.identifier(0)), (Identifier) visit(context.identifier(1)));
    }

    @Override
    public Node visitUnqualifiedArgument(ImpalaSqlParser.UnqualifiedArgumentContext context)
    {
        return new PathElement(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitPathSpecification(ImpalaSqlParser.PathSpecificationContext context)
    {
        return new PathSpecification(getLocation(context), visit(context.pathElement(), PathElement.class));
    }

    @Override
    public Node visitUnquotedIdentifier(ImpalaSqlParser.UnquotedIdentifierContext context)
    {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    @Override
    public Node visitQuotedIdentifier(ImpalaSqlParser.QuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(context), identifier, true);
    }

    @Override
    public Node visitBackQuotedIdentifier(ImpalaSqlParser.BackQuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("``", "`");

        return new Identifier(getLocation(context), identifier, true);
    }

    @Override
    public Node visitDecimalLiteral(ImpalaSqlParser.DecimalLiteralContext context)
    {
        switch (parsingOptions.getDecimalLiteralTreatment()) {
            case AS_DOUBLE:
                return new DoubleLiteral(getLocation(context), context.getText());
            case AS_DECIMAL:
                return new DecimalLiteral(getLocation(context), context.getText());
            case REJECT:
                throw new ParsingException("Unexpected decimal literal: " + context.getText());
        }
        throw new AssertionError("Unreachable");
    }

    @Override
    public Node visitDoubleLiteral(ImpalaSqlParser.DoubleLiteralContext context)
    {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitIntegerLiteral(ImpalaSqlParser.IntegerLiteralContext context)
    {
        return new LongLiteral(getLocation(context), context.getText());
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    private static LikeClause.PropertiesOption getPropertiesOption(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.INCLUDING:
                return LikeClause.PropertiesOption.INCLUDING;
            case ImpalaSqlLexer.EXCLUDING:
                return LikeClause.PropertiesOption.EXCLUDING;
        }
        throw new IllegalArgumentException("Unsupported LIKE option type: " + token.getText());
    }

    private List<TableElement> getTableElements(List<ImpalaSqlParser.TableElementContext> tableElements)
    {
        return tableElements.stream().map(context -> (TableElement) visit(context)).collect(toList());
    }

    private List<ColumnDefinition> getColumnDefinitions(List<ImpalaSqlParser.ColumnDefinitionContext> columnDefinitions)
    {
        return columnDefinitions.stream().map(context -> (ColumnDefinition) visit(context)).collect(toList());
    }

    private List<Expression> getExpressions(List<ImpalaSqlParser.ExpressionContext> expressions)
    {
        return expressions.stream().map(context -> (Expression) visit(context)).collect(toList());
    }

    private String getFileFormat(String hiveFileFormat)
    {
        String key = hiveFileFormat.toUpperCase(Locale.ENGLISH);
        if (IMPALA_TO_HETU_FILE_FORMAT.containsKey(key)) {
            return IMPALA_TO_HETU_FILE_FORMAT.get(key);
        }
        addDiff(DiffType.UNSUPPORTED, hiveFileFormat, String.format("[%s] file format is not supported", key));
        throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, format("Unsupported file format: %s", hiveFileFormat));
    }

    private QualifiedName getQualifiedName(ImpalaSqlParser.QualifiedNameContext context)
    {
        return QualifiedName.of(visit(context.identifier(), Identifier.class));
    }

    private static boolean isDistinct(ImpalaSqlParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static boolean isHexDigit(char c)
    {
        return ((c >= '0') && (c <= '9')) ||
                ((c >= 'A') && (c <= 'F')) ||
                ((c >= 'a') && (c <= 'f'));
    }

    private static boolean isValidUnicodeEscape(char c)
    {
        return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
    }

    private static Optional<String> getTextIfPresent(ParserRuleContext context)
    {
        return Optional.ofNullable(context)
                .map(ParseTree::getText);
    }

    private static Optional<String> getTextIfPresent(Token token)
    {
        return Optional.ofNullable(token)
                .map(Token::getText);
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case ImpalaSqlLexer.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case ImpalaSqlLexer.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case ImpalaSqlLexer.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case ImpalaSqlLexer.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case ImpalaSqlLexer.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case ImpalaSqlLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case ImpalaSqlLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case ImpalaSqlLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case ImpalaSqlLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case ImpalaSqlLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case ImpalaSqlLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static CurrentTime.Function getDateTimeFunctionType(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.CURRENT_TIMESTAMP:
                return CurrentTime.Function.TIMESTAMP;
        }

        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
    }

    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.YEAR:
            case ImpalaSqlLexer.YEARS:
                return IntervalLiteral.IntervalField.YEAR;
            case ImpalaSqlLexer.MONTH:
            case ImpalaSqlLexer.MONTHS:
                return IntervalLiteral.IntervalField.MONTH;
            case ImpalaSqlLexer.DAY:
            case ImpalaSqlLexer.DAYS:
                return IntervalLiteral.IntervalField.DAY;
            case ImpalaSqlLexer.HOUR:
            case ImpalaSqlLexer.HOURS:
                return IntervalLiteral.IntervalField.HOUR;
            case ImpalaSqlLexer.MINUTE:
            case ImpalaSqlLexer.MINUTES:
                return IntervalLiteral.IntervalField.MINUTE;
            case ImpalaSqlLexer.SECOND:
            case ImpalaSqlLexer.SECONDS:
                return IntervalLiteral.IntervalField.SECOND;
        }

        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }

    private static WindowFrameType getFrameType(Token type)
    {
        switch (type.getType()) {
            case ImpalaSqlLexer.RANGE:
                return WindowFrameType.RANGE;
            case ImpalaSqlLexer.ROWS:
                return WindowFrameType.ROWS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    private static FrameBoundType getBoundedFrameBoundType(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.PRECEDING:
                return FrameBoundType.PRECEDING;
            case ImpalaSqlLexer.FOLLOWING:
                return FrameBoundType.FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static FrameBoundType getUnboundedFrameBoundType(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.PRECEDING:
                return FrameBoundType.UNBOUNDED_PRECEDING;
            case ImpalaSqlLexer.FOLLOWING:
                return FrameBoundType.UNBOUNDED_FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static SampledRelation.Type getSamplingMethod(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.BERNOULLI:
                return SampledRelation.Type.BERNOULLI;
            case ImpalaSqlLexer.SYSTEM:
                return SampledRelation.Type.SYSTEM;
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static LogicalBinaryExpression.Operator getLogicalBinaryOperator(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.AND:
                return LogicalBinaryExpression.Operator.AND;
            case ImpalaSqlLexer.OR:
                return LogicalBinaryExpression.Operator.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case ImpalaSqlLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.Ordering getOrderingType(Token token)
    {
        switch (token.getType()) {
            case ImpalaSqlLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case ImpalaSqlLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol)
    {
        switch (symbol.getType()) {
            case ImpalaSqlLexer.ALL:
                return QuantifiedComparisonExpression.Quantifier.ALL;
            case ImpalaSqlLexer.ANY:
                return QuantifiedComparisonExpression.Quantifier.ANY;
            case ImpalaSqlLexer.SOME:
                return QuantifiedComparisonExpression.Quantifier.SOME;
        }

        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }

    private String getType(ImpalaSqlParser.TypeContext type)
    {
        if (type.baseType() != null) {
            String signature = type.baseType().getText();
            if (type.baseType().DOUBLE_PRECISION() != null) {
                signature = "DOUBLE";
            }
            if (!type.typeParameter().isEmpty()) {
                String typeParameterSignature = type
                        .typeParameter()
                        .stream()
                        .map(this::typeParameterToString)
                        .collect(Collectors.joining(","));
                signature += "(" + typeParameterSignature + ")";
            }
            return signature;
        }

        if (type.ARRAY() != null) {
            return "ARRAY(" + getType(type.type(0)) + ")";
        }

        if (type.MAP() != null) {
            return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
        }

        // extract the type name
        String typeName = type.getText().split("<")[0].trim();
        addDiff(DiffType.UNSUPPORTED, typeName, String.format("type [%s] is not supported.", type.getText()));
        throw unsupportedError(ErrorType.UNSUPPORTED_ATTRIBUTE, "Unsupported type specification: " + type.getText());
    }

    private String typeParameterToString(ImpalaSqlParser.TypeParameterContext typeParameter)
    {
        if (typeParameter.INTEGER_VALUE() != null) {
            return typeParameter.INTEGER_VALUE().toString();
        }
        if (typeParameter.type() != null) {
            return getType(typeParameter.type());
        }
        throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
    }

    private List<Identifier> getIdentifiers(List<ImpalaSqlParser.IdentifierContext> identifiers)
    {
        return identifiers.stream().map(context -> (Identifier) visit(context)).collect(toList());
    }

    private enum UnicodeDecodeState
    {
        EMPTY,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    private PrincipalSpecification getPrincipalSpecification(ImpalaSqlParser.PrincipalContext context)
    {
        if (context instanceof ImpalaSqlParser.UnspecifiedPrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, (Identifier) visit(((ImpalaSqlParser.UnspecifiedPrincipalContext) context).identifier()));
        }
        else if (context instanceof ImpalaSqlParser.RolePrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.ROLE, (Identifier) visit(((ImpalaSqlParser.RolePrincipalContext) context).identifier()));
        }
        else {
            throw new IllegalArgumentException("Unsupported principal: " + context);
        }
    }
}
