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
package io.prestosql.cli;

import com.google.common.collect.ImmutableSet;
import io.prestosql.client.ClientSelectedRole;
import io.prestosql.client.ClientSession;
import io.prestosql.client.ErrorLocation;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.StatementSplitter;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CreateCube;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.prestosql.cli.QueryPreprocessor.preprocessQuery;
import static io.prestosql.client.ClientSession.stripTransactionId;
import static io.prestosql.sql.parser.StatementSplitter.Statement;
import static io.prestosql.sql.parser.StatementSplitter.isEmptyStatement;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.LogicalBinaryExpression.and;
import static java.lang.Long.max;
import static java.lang.Long.min;
import static org.jline.terminal.TerminalBuilder.terminal;

public class CubeConsole
{
    private final Console console;

    private static final int SUPPORTED_INDENTIFIER_SIZE = 1;
    private static final Long RANGE_DIVIDER_MINIMUM_VALUE = 1L;
    private static final Long INITIAL_QUERY_RESULT_VALUE = 0L;
    private static final Long INITIAL_QUERY_RESULT_MIN_VALUE = 0L;
    private static final Long INITIAL_QUERY_RESULT_MAX_VALUE = 0L;
    private static final String SELECT_COUNT_STAR_FROM_STRING = "select count(*) from %s where %s";
    private static final String SELECT_COUNT_DISTINCT_FROM_STRING = "select count(distinct %s) from %s where %s";
    private static final String INSERT_INTO_CUBE_STRING = "insert into cube %s where %s";
    private static final String DROP_CUBE_STRING = "drop cube %s";
    private static final String SELECT_MIN_STRING = "select min( %s ) from %s where %s";
    private static final String SELECT_MAX_STRING = "select max( %s ) from %s where %s";
    private static String resultInitCubeQUery;

    public CubeConsole(Console console)
    {
        this.console = console;
    }

    private static void setResultInitCubeQuery(String resultCubeQUery)
    {
        resultInitCubeQUery = resultCubeQUery;
    }

    private static String getResultInitCubeQuery()
    {
        return resultInitCubeQUery;
    }

    public boolean executeCubeCommand(
            QueryRunner queryRunner,
            AtomicBoolean exiting,
            String query,
            ClientOptions.OutputFormat outputFormat,
            boolean ignoreErrors,
            boolean showProgress)
    {
        boolean success = true;
        StatementSplitter splitter = new StatementSplitter(query);
        for (Statement split : splitter.getCompleteStatements()) {
            if (!isEmptyStatement(split.statement())) {
                try (Terminal terminal = terminal()) {
                    String statement = split.statement();
                    if (createCubeCommand(split.statement(), queryRunner, outputFormat, () -> {}, false, showProgress, terminal, System.out, System.err)) {
                        if (!ignoreErrors) {
                            return false;
                        }
                        success = false;
                    }
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            if (exiting.get()) {
                return success;
            }
        }
        if (!isEmptyStatement(splitter.getPartialStatement())) {
            System.err.println("Non-terminated statement: " + splitter.getPartialStatement());
            return false;
        }
        return success;
    }

    /**
     * Process the Create Cube Query
     *
     * @param queryRunner queryRunner
     * @param outputFormat outputFormat
     * @param schemaChanged schemaChanged
     * @param usePager usePager
     * @param schemaChanged schemaChanged
     * @param showProgress showProgress
     * @param terminal terminal
     * @param out out
     * @param errorChannel errorChannel
     * @return boolean after processing the create cube query command.
     */
    public boolean createCubeCommand(String query, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel)
    {
        boolean success = true;
        SqlParser parser = new SqlParser();
        QualifiedName cubeName = null;
        try {
            CreateCube createCube = (CreateCube) parser.createStatement(query, new ParsingOptions());
            cubeName = createCube.getCubeName();
            QualifiedName sourceTableName = createCube.getSourceTableName();
            String whereClause = createCube.getWhere().get().toString();
            Optional<Expression> expression = createCube.getWhere();
            Set<FunctionCall> aggregations = createCube.getAggregations();
            List<Identifier> groupingSet = createCube.getGroupingSet();
            List<Property> properties = createCube.getProperties();
            boolean notExists = createCube.isNotExists();
            CreateCube modifiedCreateCube = new CreateCube(cubeName, sourceTableName, groupingSet, aggregations, notExists, properties, Optional.empty());
            String queryCreateCube = SqlFormatter.formatSql(modifiedCreateCube, Optional.empty());

            success = console.runQuery(queryRunner, queryCreateCube, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            if (!success) {
                return success;
            }
            //we check whether the create cube expression can be processed
            if (isSupportedExpression(expression)) {
                if (createCube.getWhere().get() instanceof BetweenPredicate) {
                    //we process the between predicate in the create cube query where clause
                    success = processBetweenPredicate(createCube, queryRunner, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel, parser);
                }
                if (createCube.getWhere().get() instanceof ComparisonExpression) {
                    //we process the comparison expression in the create cube query where clause
                    success = processComparisonExpression(createCube, queryRunner, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel, parser);
                }
            }
            else {
                //if we donot support processing the create cube query with multiple inserts, then only a single insert is run internally.
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, whereClause);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            if (!success) {
                //roll back mechanism for unsuccessful create cube query
                String dropCubeQuery = String.format(DROP_CUBE_STRING, cubeName);
                console.runQuery(queryRunner, dropCubeQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
        }
        catch (ParsingException e) {
            if (cubeName != null) {
                //roll back mechanism for unsuccessful create cube query
                String dropCubeQuery = String.format(DROP_CUBE_STRING, cubeName);
                console.runQuery(queryRunner, dropCubeQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            System.out.println(e.getMessage());
            Query.renderErrorLocation(query, new ErrorLocation(e.getLineNumber(), e.getColumnNumber()), errorChannel);
            success = false;
        }
        catch (Exception e) {
            if (cubeName != null) {
                //roll back mechanism for unsuccessful create cube query
                String dropCubeQuery = String.format(DROP_CUBE_STRING, cubeName);
                console.runQuery(queryRunner, dropCubeQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            // Add blank line after progress bar
            System.out.println();
            System.out.println(e.getMessage());
            success = false;
        }
        return success;
    }

    /**
     * Process the Create Cube Query with Between Predicate in where clause.
     *
     * @param createCube createCube
     * @param queryRunner queryRunner
     * @param outputFormat outputFormat
     * @param schemaChanged schemaChanged
     * @param usePager usePager
     * @param showProgress showProgress
     * @param terminal terminal
     * @param out out
     * @param errorChannel errorChannel
     * @param parser parser
     * @return boolean
     */
    private boolean processBetweenPredicate(CreateCube createCube, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel, SqlParser parser)
    {
        String whereClause = createCube.getWhere().get().toString();
        QualifiedName sourceTableName = createCube.getSourceTableName();
        QualifiedName cubeName = createCube.getCubeName();
        BetweenPredicate betweenPredicate = (BetweenPredicate) (createCube.getWhere().get());
        String columnName = betweenPredicate.getValue().toString();
        Long minValue = Long.parseLong(betweenPredicate.getMin().toString());
        Long maxValue = Long.parseLong(betweenPredicate.getMax().toString());
        Long rangeValue = maxValue - minValue;
        boolean success = true;

        //initial query to get the total number of rows in the table
        String initialQuery = String.format(SELECT_COUNT_STAR_FROM_STRING, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, initialQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);

        Long valueCountStarQuery = INITIAL_QUERY_RESULT_VALUE;
        String resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountStarQuery = Long.parseLong(resultInitCubeQuery);
        }
        //initial query to get the total number of distinct column values in the table
        String countDistinctQuery = String.format(SELECT_COUNT_DISTINCT_FROM_STRING, columnName, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, countDistinctQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);

        Long valueCountDistinctQuery = INITIAL_QUERY_RESULT_VALUE;
        resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountDistinctQuery = Long.parseLong(resultInitCubeQuery);
        }

        //the range is divided for running the multiple insert queries.
        Long maxRowsProcessLimit = Long.parseLong(console.getClientOptions().maxBatchProcessSize);
        Long rangeDivider = max(min(valueCountDistinctQuery, (valueCountStarQuery / maxRowsProcessLimit)), RANGE_DIVIDER_MINIMUM_VALUE);
        Long rangePartitionValue = rangeValue / rangeDivider;

        if ((minValue + rangePartitionValue) < maxValue) {
            //this loop process the multiple insert query statements
            while (((minValue + rangePartitionValue) < maxValue) && success) {
                Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, betweenPredicate.getValue(), parser.createExpression(Long.toString(minValue), new ParsingOptions()));
                Expression upperBound = new ComparisonExpression(LESS_THAN, betweenPredicate.getValue(), parser.createExpression(Long.toString(minValue + rangePartitionValue), new ParsingOptions()));
                minValue = minValue + rangePartitionValue;
                Expression finalBinaryExpression = and(lowerBound, upperBound);
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalBinaryExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            if (!success) {
                return success;
            }
            if (minValue == maxValue) {
                Expression equalExpression = new ComparisonExpression(EQUAL, betweenPredicate.getValue(), parser.createExpression(Long.toString(minValue), new ParsingOptions()));
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, equalExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            else if (minValue < maxValue) {
                Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, betweenPredicate.getValue(), parser.createExpression(Long.toString(minValue), new ParsingOptions()));
                Expression upperBound = new ComparisonExpression(LESS_THAN_OR_EQUAL, betweenPredicate.getValue(), parser.createExpression(Long.toString(maxValue), new ParsingOptions()));
                Expression finalBinaryExpression = and(lowerBound, upperBound);
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalBinaryExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
        }
        else {
            //if the range is within the processing size limit then we run a single insert query only
            String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, whereClause);
            success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        }
        return success;
    }

    /**
     * Process the Create Cube Query with Comparison Expression in where clause.
     *
     * @param createCube createCube
     * @param queryRunner queryRunner
     * @param outputFormat outputFormat
     * @param schemaChanged schemaChanged
     * @param usePager usePager
     * @param showProgress showProgress
     * @param terminal terminal
     * @param out out
     * @param errorChannel errorChannel
     * @param parser parser
     * @return boolean
     */
    private boolean processComparisonExpression(CreateCube createCube, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel, SqlParser parser)
    {
        ComparisonExpression comparisonExpression = (ComparisonExpression) (createCube.getWhere().get());
        ComparisonExpression.Operator operator = comparisonExpression.getOperator();
        Expression left = comparisonExpression.getLeft();
        Expression right = comparisonExpression.getRight();

        if (!(left instanceof SymbolReference) && right instanceof SymbolReference) {
            comparisonExpression = new ComparisonExpression(operator.flip(), right, left);
        }

        if (left instanceof Literal && !(right instanceof Literal)) {
            comparisonExpression = new ComparisonExpression(operator.flip(), right, left);
        }
        operator = comparisonExpression.getOperator();
        if (operator.equals(LESS_THAN) || operator.equals(LESS_THAN_OR_EQUAL)) {
            return processComparisonExpressionOperatorLess(createCube, queryRunner, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel, parser, comparisonExpression);
        }
        else if (operator.equals(GREATER_THAN) || operator.equals(GREATER_THAN_OR_EQUAL)) {
            return processComparisonExpressionOperatorGreater(createCube, queryRunner, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel, parser, comparisonExpression);
        }
        else if (operator.equals(EQUAL)) {
            return processComparisonExpressionOperatorEqual(createCube, queryRunner, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel, parser, comparisonExpression);
        }
        return true;
    }

    /**
     * Process the Create Cube Query with Comparison Expression with Greater Than or Greater Than or equal in where clause.
     *
     * @param createCube createCube
     * @param queryRunner queryRunner
     * @param outputFormat outputFormat
     * @param schemaChanged schemaChanged
     * @param usePager usePager
     * @param showProgress showProgress
     * @param terminal terminal
     * @param out out
     * @param errorChannel errorChannel
     * @param parser parser
     * @param comparisonExpression comparisonExpression
     * @return boolean
     */
    private boolean processComparisonExpressionOperatorGreater(CreateCube createCube, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel, SqlParser parser, ComparisonExpression comparisonExpression)
    {
        String whereClause = createCube.getWhere().get().toString();
        QualifiedName sourceTableName = createCube.getSourceTableName();
        QualifiedName cubeName = createCube.getCubeName();
        Expression columnName = comparisonExpression.getLeft();
        ComparisonExpression.Operator operator = comparisonExpression.getOperator();
        Long rightValue = Long.parseLong(comparisonExpression.getRight().toString());
        boolean success = true;

        //initial query to get the total number of rows in the table
        String initialQuery = String.format(SELECT_COUNT_STAR_FROM_STRING, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, initialQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        Long valueCountStarQuery = INITIAL_QUERY_RESULT_VALUE;
        String resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountStarQuery = Long.parseLong(resultInitCubeQuery);
        }

        //initial query to get the total number of distinct column values in the table
        String countDistinctQuery = String.format(SELECT_COUNT_DISTINCT_FROM_STRING, columnName, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, countDistinctQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        Long valueCountDistinctQuery = INITIAL_QUERY_RESULT_VALUE;
        resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountDistinctQuery = Long.parseLong(resultInitCubeQuery);
        }

        //initial query to get the max value of column in the table
        String queryMaxValue = String.format(SELECT_MAX_STRING, columnName, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, queryMaxValue, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        Long maxValueQueryResult = INITIAL_QUERY_RESULT_MAX_VALUE;
        resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            maxValueQueryResult = Long.parseLong(resultInitCubeQuery);
        }

        //the range is divided for running the multiple insert queries.
        Long maxRowsProcessLimit = Long.parseLong(console.getClientOptions().maxBatchProcessSize); // this will be user input
        Long rangeDivider = max(min(valueCountDistinctQuery, (valueCountStarQuery / maxRowsProcessLimit)), RANGE_DIVIDER_MINIMUM_VALUE);
        Long rangeValue = maxValueQueryResult - rightValue;
        Long rangePartitionValue = rangeValue / rangeDivider;

        if ((rightValue + rangePartitionValue) < maxValueQueryResult) {
            //this loop process the multiple insert query statements
            while (((rightValue + rangePartitionValue) < maxValueQueryResult) && success) {
                Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, columnName, parser.createExpression(Long.toString(rightValue), new ParsingOptions()));
                Expression upperBound = new ComparisonExpression(LESS_THAN, columnName, parser.createExpression(Long.toString(rightValue + rangePartitionValue), new ParsingOptions()));
                rightValue = rightValue + rangePartitionValue;
                Expression finalBinaryExpression = and(lowerBound, upperBound);
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalBinaryExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            if (!success) {
                return success;
            }
            if (rightValue == maxValueQueryResult && operator.equals(GREATER_THAN_OR_EQUAL)) {
                Expression equalExpression = new ComparisonExpression(EQUAL, columnName, parser.createExpression(Long.toString(rightValue), new ParsingOptions()));
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, equalExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            else if (rightValue < maxValueQueryResult) {
                Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, columnName, parser.createExpression(Long.toString(rightValue), new ParsingOptions()));
                Expression upperBound = new ComparisonExpression(LESS_THAN_OR_EQUAL, columnName, parser.createExpression(Long.toString(maxValueQueryResult), new ParsingOptions()));
                Expression finalBinaryExpression = and(lowerBound, upperBound);
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalBinaryExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
        }
        else {
            //if the range is within the processing size limit then we run a single insert query only
            String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, whereClause);
            success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        }
        return success;
    }

    /**
     * Process the Create Cube Query with Comparison Expression with Equal in where clause.
     *
     * @param createCube createCube
     * @param queryRunner queryRunner
     * @param outputFormat outputFormat
     * @param schemaChanged schemaChanged
     * @param usePager usePager
     * @param showProgress showProgress
     * @param terminal terminal
     * @param out out
     * @param errorChannel errorChannel
     * @param parser parser
     * @param comparisonExpression comparisonExpression
     * @return boolean
     */
    private boolean processComparisonExpressionOperatorEqual(CreateCube createCube, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel, SqlParser parser, ComparisonExpression comparisonExpression)
    {
        String whereClause = createCube.getWhere().get().toString();
        QualifiedName sourceTableName = createCube.getSourceTableName();
        QualifiedName cubeName = createCube.getCubeName();
        Expression columnName = comparisonExpression.getLeft();
        Long rightValue = Long.parseLong(comparisonExpression.getRight().toString());
        boolean success = true;

        //initial query to get the total number of rows in the table
        String initialQuery = String.format(SELECT_COUNT_STAR_FROM_STRING, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, initialQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        Long valueCountStarQuery = INITIAL_QUERY_RESULT_VALUE;
        String resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountStarQuery = Long.parseLong(resultInitCubeQuery);
        }

        //initial query to get the total number of distinct column values in the table
        String countDistinctQuery = String.format(SELECT_COUNT_DISTINCT_FROM_STRING, columnName, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, countDistinctQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        Long valueCountDistinctQuery = INITIAL_QUERY_RESULT_VALUE;
        resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountDistinctQuery = Long.parseLong(resultInitCubeQuery);
        }

        //the max rows that can be processed by the cluster
        Long maxRowsProcessLimit = Long.parseLong(console.getClientOptions().maxBatchProcessSize); // this will be user input

        if (valueCountStarQuery < maxRowsProcessLimit) {
            //this loop process the multiple insert query statements
            Expression equalExpression = new ComparisonExpression(EQUAL, columnName, parser.createExpression(Long.toString(rightValue), new ParsingOptions()));
            String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, equalExpression);
            success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        }
        else {
            //if the range is within the processing size limit then we run a single insert query only
            String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, whereClause);
            success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        }
        return success;
    }

    /**
     * Process the Create Cube Query with Comparison Expression with Greater Than or Greater Than or equal in where clause.
     *
     * @param createCube createCube
     * @param queryRunner queryRunner
     * @param outputFormat outputFormat
     * @param schemaChanged schemaChanged
     * @param usePager usePager
     * @param showProgress showProgress
     * @param terminal terminal
     * @param out out
     * @param errorChannel errorChannel
     * @param parser parser
     * @param comparisonExpression comparisonExpression
     * @return boolean
     */
    private boolean processComparisonExpressionOperatorLess(CreateCube createCube, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel, SqlParser parser, ComparisonExpression comparisonExpression)
    {
        String whereClause = createCube.getWhere().get().toString();
        QualifiedName sourceTableName = createCube.getSourceTableName();
        QualifiedName cubeName = createCube.getCubeName();
        Expression columnName = comparisonExpression.getLeft();
        ComparisonExpression.Operator operator = comparisonExpression.getOperator();
        Long rightValue = Long.parseLong(comparisonExpression.getRight().toString());

        //initial query to get the total number of rows in the table
        String initialQuery = String.format(SELECT_COUNT_STAR_FROM_STRING, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, initialQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);

        Long valueCountStarQuery = INITIAL_QUERY_RESULT_VALUE;
        String resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountStarQuery = Long.parseLong(resultInitCubeQuery);
        }

        //initial query to get the total number of distinct column values in the table
        String countDistinctQuery = String.format(SELECT_COUNT_DISTINCT_FROM_STRING, columnName, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, countDistinctQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);

        Long valueCountDistinctQuery = INITIAL_QUERY_RESULT_VALUE;
        resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            valueCountDistinctQuery = Long.parseLong(resultInitCubeQuery);
        }

        //initial query to get the max value of column in the table
        String queryMinValue = String.format(SELECT_MIN_STRING, columnName, sourceTableName.toString(), whereClause);
        processCubeInitialQuery(queryRunner, queryMinValue, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);

        Long minValueQueryResult = INITIAL_QUERY_RESULT_MIN_VALUE;
        resultInitCubeQuery = getResultInitCubeQuery();
        if (resultInitCubeQuery != null) {
            minValueQueryResult = Long.parseLong(resultInitCubeQuery);
        }

        //the range is divided for running the multiple insert queries.
        Long maxRowsProcessLimit = Long.parseLong(console.getClientOptions().maxBatchProcessSize); // this will be user input
        Long rangeDivider = max(min(valueCountDistinctQuery, (valueCountStarQuery / maxRowsProcessLimit)), RANGE_DIVIDER_MINIMUM_VALUE);
        Long rangeValue = rightValue - minValueQueryResult;
        Long rangePartitionValue = rangeValue / rangeDivider;
        boolean success = true;

        if ((minValueQueryResult + rangePartitionValue) < rightValue) {
            //this loop process the multiple insert query statements
            while (((minValueQueryResult + rangePartitionValue) < rightValue) && success) {
                Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, columnName, parser.createExpression(Long.toString(minValueQueryResult), new ParsingOptions()));
                Expression upperBound = new ComparisonExpression(LESS_THAN, columnName, parser.createExpression(Long.toString(minValueQueryResult + rangePartitionValue), new ParsingOptions()));
                minValueQueryResult = minValueQueryResult + rangePartitionValue;
                Expression finalBinaryExpression = and(lowerBound, upperBound);
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalBinaryExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            if (!success) {
                return success;
            }
            if (minValueQueryResult == rightValue && operator.equals(LESS_THAN_OR_EQUAL)) {
                Expression equalExpression = new ComparisonExpression(EQUAL, columnName, parser.createExpression(Long.toString(minValueQueryResult), new ParsingOptions()));
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, equalExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
            else if (minValueQueryResult < rightValue) {
                Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, columnName, parser.createExpression(Long.toString(minValueQueryResult), new ParsingOptions()));
                Expression upperBound = new ComparisonExpression(LESS_THAN_OR_EQUAL, columnName, parser.createExpression(Long.toString(rightValue), new ParsingOptions()));
                Expression finalBinaryExpression = and(lowerBound, upperBound);
                String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalBinaryExpression);
                success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
            }
        }
        else {
            //if the range is within the processing size limit then we run a single insert query only
            String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, whereClause);
            success = console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel);
        }
        return success;
    }

    /**
     * Gets whether the expression is supported for processing the create cube query
     *
     * @param expression expression
     * @return void
     */
    private boolean isSupportedExpression(Optional<Expression> expression)
    {
        boolean supportedExpression = false;
        if (expression.isPresent()) {
            ImmutableSet.Builder<Identifier> identifierBuilder = new ImmutableSet.Builder<>();
            new DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<Identifier>>() {
                @Override
                protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<Identifier> builder)
                {
                    builder.add(node);
                    return null;
                }
            }.process(expression.get(), identifierBuilder);

            int sizeIdentifiers = identifierBuilder.build().asList().size();
            if (sizeIdentifiers == SUPPORTED_INDENTIFIER_SIZE) {
                if (expression.get() instanceof BetweenPredicate) {
                    BetweenPredicate betweenPredicate = (BetweenPredicate) (expression.get());
                    if (betweenPredicate.getMin() instanceof LongLiteral || betweenPredicate.getMax() instanceof LongLiteral) {
                        supportedExpression = true;
                    }
                }
                if (expression.get() instanceof ComparisonExpression) {
                    supportedExpression = true;
                }
            }
        }
        return supportedExpression;
    }

    private static boolean processCubeInitialQuery(
            QueryRunner queryRunner,
            String sql,
            ClientOptions.OutputFormat outputFormat,
            Runnable schemaChanged,
            boolean usePager,
            boolean showProgress,
            Terminal terminal,
            PrintStream out,
            PrintStream errorChannel)
    {
        String finalSql;
        try {
            finalSql = preprocessQuery(
                    Optional.ofNullable(queryRunner.getSession().getCatalog()),
                    Optional.ofNullable(queryRunner.getSession().getSchema()),
                    sql);
        }
        catch (QueryPreprocessorException e) {
            System.err.println(e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace(System.err);
            }
            return false;
        }

        try (Query query = queryRunner.startQuery(finalSql)) {
            boolean success = query.renderCubeOutput(terminal, out, errorChannel, outputFormat, usePager, showProgress);
            if (success) {
                setResultInitCubeQuery(query.getcubeInitQueryResult());
            }
            ClientSession session = queryRunner.getSession();

            // update catalog and schema if present
            if (query.getSetCatalog().isPresent() || query.getSetSchema().isPresent()) {
                session = ClientSession.builder(session)
                        .withCatalog(query.getSetCatalog().orElse(session.getCatalog()))
                        .withSchema(query.getSetSchema().orElse(session.getSchema()))
                        .build();
            }

            // update transaction ID if necessary
            if (query.isClearTransactionId()) {
                session = stripTransactionId(session);
            }

            ClientSession.Builder builder = ClientSession.builder(session);

            if (query.getStartedTransactionId() != null) {
                builder = builder.withTransactionId(query.getStartedTransactionId());
            }

            // update path if present
            if (query.getSetPath().isPresent()) {
                builder = builder.withPath(query.getSetPath().get());
            }

            // update session properties if present
            if (!query.getSetSessionProperties().isEmpty() || !query.getResetSessionProperties().isEmpty()) {
                Map<String, String> sessionProperties = new HashMap<>(session.getProperties());
                sessionProperties.putAll(query.getSetSessionProperties());
                sessionProperties.keySet().removeAll(query.getResetSessionProperties());
                builder = builder.withProperties(sessionProperties);
            }

            // update session roles
            if (!query.getSetRoles().isEmpty()) {
                Map<String, ClientSelectedRole> roles = new HashMap<>(session.getRoles());
                roles.putAll(query.getSetRoles());
                builder = builder.withRoles(roles);
            }

            // update prepared statements if present
            if (!query.getAddedPreparedStatements().isEmpty() || !query.getDeallocatedPreparedStatements().isEmpty()) {
                Map<String, String> preparedStatements = new HashMap<>(session.getPreparedStatements());
                preparedStatements.putAll(query.getAddedPreparedStatements());
                preparedStatements.keySet().removeAll(query.getDeallocatedPreparedStatements());
                builder = builder.withPreparedStatements(preparedStatements);
            }

            session = builder.build();
            queryRunner.setSession(session);

            if (query.getSetCatalog().isPresent() || query.getSetSchema().isPresent()) {
                schemaChanged.run();
            }

            return success;
        }
        catch (RuntimeException e) {
            System.err.println("Error running command: " + e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace(System.err);
            }
            return false;
        }
    }
}
