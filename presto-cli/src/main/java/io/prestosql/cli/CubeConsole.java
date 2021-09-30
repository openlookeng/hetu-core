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
package io.prestosql.cli;

import com.google.common.collect.ImmutableSet;
import io.prestosql.client.ClientSelectedRole;
import io.prestosql.client.ClientSession;
import io.prestosql.client.ErrorLocation;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CreateCube;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimestampLiteral;
import org.jline.terminal.Terminal;

import java.io.PrintStream;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.cli.QueryPreprocessor.preprocessQuery;
import static io.prestosql.client.ClientSession.stripTransactionId;

public class CubeConsole
{
    private final Console console;

    private static final int SUPPORTED_INDENTIFIER_SIZE = 1;
    private static final Long INITIAL_QUERY_RESULT_VALUE = 0L;
    private static final String SELECT_COUNT_DISTINCT_FROM_STRING = "select count(distinct %s) from %s where %s";
    private static final String SELECT_COLUMN_ROW_COUNT_FROM_STRING = "select %s, count(*) from %s where %s group by %s order by %s";
    private static final String INSERT_INTO_CUBE_STRING = "insert into cube %s where %s";
    private static final String DROP_CUBE_STRING = "drop cube %s";
    private static final String SELECT_DATA_TYPE_STRING = "select data_type from %s.information_schema.columns where table_name='%s' and column_name='%s'";
    private static final String DATATYPE_DOUBLE = "double";
    private static final String DATATYPE_REAL = "real";
    private static final String DATATYPE_DECIMAL = "decimal";
    private static final String DATATYPE_DATE = "date";
    private static final String DATATYPE_TIMESTAMP = "timestamp";
    private static final String DATATYPE_VARCHAR = "varchar";
    private static final String QUOTE_STRING = "'";
    private static final String DATATYPE_REAL_QUOTE = "real '";
    private static final String DATATYPE_DATE_QUOTE = "date '";
    private static final String DATATYPE_TIMESTAMP_QUOTE = "timestamp '";
    private static final String DATATYPE_TINYINT_QUOTE = "tinyint '";
    private static final String DATATYPE_BIGINT_QUOTE = "bigint '";
    private static final String DATATYPE_SMALLINT_QUOTE = "smallint '";
    private static final String DATATYPE_INTEGER = "integer";
    private static final String DATATYPE_TINYINT = "tinyint";
    private static final String DATATYPE_BIGINT = "bigint";
    private static final String DATATYPE_SMALLINT = "smallint";
    private static final String NOT_EQUAL_OPERATOR = "<>";
    private static final int EMPTY_ROW_BUFFER_ITERATION_ITEMS = 0;
    private static final int INDEX_AT_MIN_POSITION = 0;
    private static final int INDEX_AT_MAX_POSITION = 1;
    private static final long MAX_BUFFERED_ROWS = 10000000000L;
    private static int rowBufferListSize;
    private static final double rowBufferTempMultiplier = 1.3;
    private static String resultInitCubeQuery;
    private List<List<?>> rowBufferIterationItems;
    private String cubeColumnDataType;
    private static final String INPUT_REGEX = "[\\[\\]\"\'`\\\\<>%=!~*.]";

    List<String> supportedDataTypes = new ArrayList<>(Arrays.asList(DATATYPE_INTEGER, DATATYPE_TINYINT,
            DATATYPE_BIGINT, DATATYPE_SMALLINT, DATATYPE_DATE, DATATYPE_TIMESTAMP, DATATYPE_DOUBLE, DATATYPE_REAL, DATATYPE_VARCHAR));

    public CubeConsole(Console console)
    {
        this.console = console;
    }

    private static void setResultInitCubeQuery(String resultCubeQuery)
    {
        resultInitCubeQuery = resultCubeQuery;
    }

    public void setListRowBufferIterationItems(List<List<?>> rowBufferIterationItems)
    {
        this.rowBufferIterationItems = rowBufferIterationItems;
    }

    public List<List<?>> getListRowBufferIterationItems()
    {
        return rowBufferIterationItems;
    }

    private static String getResultInitCubeQuery()
    {
        return resultInitCubeQuery;
    }

    public String getCubeColumnDataType()
    {
        return cubeColumnDataType;
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
            CreateCube createCube = (CreateCube) parser.createStatement(query, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE));
            cubeName = createCube.getCubeName();
            QualifiedName sourceTableName = createCube.getSourceTableName();
            String whereClause = createCube.getWhere().get().toString();
            Set<FunctionCall> aggregations = createCube.getAggregations();
            List<Identifier> groupingSet = createCube.getGroupingSet();
            List<Property> properties = createCube.getProperties();
            boolean notExists = createCube.isNotExists();
            CreateCube modifiedCreateCube = new CreateCube(cubeName, sourceTableName, groupingSet, aggregations, notExists, properties, Optional.empty(), createCube.getSourceFilter().orElse(null));
            String queryCreateCube = SqlFormatter.formatSql(modifiedCreateCube, Optional.empty());

            if (!console.runQuery(queryRunner, queryCreateCube, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                return false;
            }
            //we check whether the create cube expression can be processed
            if (isSupportedExpression(createCube, queryRunner, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
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
        String whereClause = createCube.getWhere().get().toString();
        QualifiedName sourceTableName = createCube.getSourceTableName();
        QualifiedName cubeName = createCube.getCubeName();
        ComparisonExpression comparisonExpression = (ComparisonExpression) (createCube.getWhere().get());
        ComparisonExpression.Operator operator = comparisonExpression.getOperator();
        Expression left = comparisonExpression.getLeft();
        Expression right = comparisonExpression.getRight();
        boolean notEqualOperator = false;

        if (operator.getValue().equalsIgnoreCase(NOT_EQUAL_OPERATOR)) {
            notEqualOperator = true;
        }

        if (!(left instanceof SymbolReference) && right instanceof SymbolReference) {
            comparisonExpression = new ComparisonExpression(operator.flip(), right, left);
        }

        if (left instanceof Literal && !(right instanceof Literal)) {
            comparisonExpression = new ComparisonExpression(operator.flip(), right, left);
        }
        Expression columnName = comparisonExpression.getLeft();

        //Run Query
        String rowCountsDistinctValuesQuery = String.format(SELECT_COLUMN_ROW_COUNT_FROM_STRING, columnName, sourceTableName.toString(), whereClause, columnName, columnName);
        if (!processCubeInitialQuery(queryRunner, rowCountsDistinctValuesQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
            return false;
        }
        List<List<?>> rowBufferIterationItems = getListRowBufferIterationItems();

        if (rowBufferIterationItems != null && rowBufferIterationItems.size() != EMPTY_ROW_BUFFER_ITERATION_ITEMS) {
            //this loop process the multiple insert query statements
            int end = rowBufferIterationItems.size() - 1;

            for (int i = 0; i <= end; i++) {
                List<?> rowBufferItems = rowBufferIterationItems.get(i);
                Expression finalPredicate;
                Expression userBoundaryPredicate = null;
                String queryInsert;
                String minItem = rowBufferItems.get(INDEX_AT_MIN_POSITION).toString();
                String maxItem = rowBufferItems.get(INDEX_AT_MAX_POSITION).toString();

                switch (cubeColumnDataType) {
                    case DATATYPE_DOUBLE: {
                        finalPredicate = new BetweenPredicate(columnName, parser.createExpression(DATATYPE_DOUBLE + " " + QUOTE_STRING + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_DOUBLE + " " + QUOTE_STRING + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_REAL: {
                        finalPredicate = new BetweenPredicate(columnName, parser.createExpression(DATATYPE_REAL_QUOTE + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_REAL_QUOTE + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_DECIMAL: {
                        finalPredicate = new BetweenPredicate(columnName, parser.createExpression(DATATYPE_DECIMAL + " " + QUOTE_STRING + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_DECIMAL + " " + QUOTE_STRING + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_DATE: {
                        finalPredicate = new BetweenPredicate(left, parser.createExpression(DATATYPE_DATE_QUOTE + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_DATE_QUOTE + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_TIMESTAMP: {
                        finalPredicate = new BetweenPredicate(left, parser.createExpression(DATATYPE_TIMESTAMP_QUOTE + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_TIMESTAMP_QUOTE + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_TINYINT: {
                        finalPredicate = new BetweenPredicate(left, parser.createExpression(DATATYPE_TINYINT_QUOTE + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_TINYINT_QUOTE + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_BIGINT: {
                        finalPredicate = new BetweenPredicate(left, parser.createExpression(DATATYPE_BIGINT_QUOTE + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_BIGINT_QUOTE + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_SMALLINT: {
                        finalPredicate = new BetweenPredicate(left, parser.createExpression(DATATYPE_SMALLINT_QUOTE + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(DATATYPE_SMALLINT_QUOTE + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    case DATATYPE_VARCHAR: {
                        finalPredicate = new BetweenPredicate(left, parser.createExpression(QUOTE_STRING + minItem + QUOTE_STRING, new ParsingOptions()), parser.createExpression(QUOTE_STRING + maxItem + QUOTE_STRING, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                    default: {
                        finalPredicate = new BetweenPredicate(left, parser.createExpression(minItem, new ParsingOptions()), parser.createExpression(maxItem, new ParsingOptions()));
                        userBoundaryPredicate = new ComparisonExpression(operator, left, right);
                        break;
                    }
                }
                if (notEqualOperator) {
                    finalPredicate = new ComparisonExpression(ComparisonExpression.Operator.NOT_EQUAL, left, right);
                    queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalPredicate);
                }
                else if (i == end && userBoundaryPredicate != null) {
                    queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, userBoundaryPredicate);
                }
                else {
                    queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalPredicate);
                }

                if (!console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                    return false;
                }
            }
        }
        else {
            //if the range is within the processing size limit then we run a single insert query only
            String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, whereClause);
            if (!console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                return false;
            }
        }
        return true;
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
        Expression betweenPredicateValue = betweenPredicate.getValue();
        String columnName = betweenPredicate.getValue().toString();
        BetweenPredicate betweenExpression = ((BetweenPredicate) createCube.getWhere().get());
        Expression left = betweenExpression.getMin();
        Expression right = betweenExpression.getMax();

        //Run Query
        String rowCountsDistinctValuesQuery = String.format(SELECT_COLUMN_ROW_COUNT_FROM_STRING, columnName, sourceTableName.toString(), whereClause, columnName, columnName);
        if (!processCubeInitialQuery(queryRunner, rowCountsDistinctValuesQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
            return false;
        }
        List<List<?>> rowBufferIterationItems = getListRowBufferIterationItems();

        if (rowBufferIterationItems != null) {
            //this loop process the multiple insert query statements
            int end = rowBufferIterationItems.size() - 1;
            for (int i = 0; i <= end; i++) {
                List<?> rowBufferItems = rowBufferIterationItems.get(i);
                Expression finalPredicate;
                String queryInsert;
                String minItem = rowBufferItems.get(INDEX_AT_MIN_POSITION).toString();
                String maxItem = rowBufferItems.get(INDEX_AT_MAX_POSITION).toString();
                Expression finalPredicateMinExp = null;
                Expression finalPredicateMaxExp = null;

                switch (cubeColumnDataType) {
                    case DATATYPE_DOUBLE: {
                        double parsedMinVal = Double.valueOf(minItem);
                        double parsedMaxVal = Double.valueOf(maxItem);
                        double userMinVal = Double.valueOf(left.toString().replace(DATATYPE_DOUBLE, "").replaceAll(QUOTE_STRING, ""));
                        double userMaxVal = Double.valueOf(right.toString().replace(DATATYPE_DOUBLE, "").replaceAll(QUOTE_STRING, ""));

                        if (parsedMinVal > userMinVal) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_DOUBLE + QUOTE_STRING + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal < userMaxVal) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_DOUBLE + QUOTE_STRING + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_REAL: {
                        double parsedMinVal = Double.valueOf(minItem);
                        double parsedMaxVal = Double.valueOf(maxItem);
                        double userMinVal = Double.valueOf(left.toString().replace(DATATYPE_REAL_QUOTE, "").replaceAll(QUOTE_STRING, ""));
                        double userMaxVal = Double.valueOf(right.toString().replace(DATATYPE_REAL_QUOTE, "").replaceAll(QUOTE_STRING, ""));

                        if (parsedMinVal > userMinVal) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_REAL + QUOTE_STRING + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal < userMaxVal) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_REAL + QUOTE_STRING + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_DECIMAL: {
                        double parsedMinVal = Double.valueOf(minItem);
                        double parsedMaxVal = Double.valueOf(maxItem);
                        double userMinVal = Double.valueOf(left.toString().replace(DATATYPE_DECIMAL, "").replaceAll(QUOTE_STRING, ""));
                        double userMaxVal = Double.valueOf(right.toString().replace(DATATYPE_DECIMAL, "").replaceAll(QUOTE_STRING, ""));

                        if (parsedMinVal > userMinVal) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_DECIMAL + QUOTE_STRING + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal < userMaxVal) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_DECIMAL + QUOTE_STRING + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_DATE: {
                        LocalDate parsedMinVal = LocalDate.parse(minItem);
                        LocalDate parsedMaxVal = LocalDate.parse(maxItem);
                        LocalDate userMinVal = LocalDate.parse(left.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_DATE_QUOTE, ""));
                        LocalDate userMaxVal = LocalDate.parse(right.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_DATE_QUOTE, ""));

                        if (parsedMinVal.compareTo(userMinVal) > 0) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_DATE_QUOTE + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal.compareTo(userMaxVal) < 0) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_DATE_QUOTE + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_TIMESTAMP: {
                        Timestamp parsedMinVal = Timestamp.valueOf(minItem);
                        Timestamp parsedMaxVal = Timestamp.valueOf(maxItem);
                        Timestamp userMinVal = Timestamp.valueOf(left.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_TIMESTAMP_QUOTE, ""));
                        Timestamp userMaxVal = Timestamp.valueOf(right.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_TIMESTAMP_QUOTE, ""));

                        if (parsedMinVal.compareTo(userMinVal) > 0) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_TIMESTAMP_QUOTE + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal.compareTo(userMaxVal) < 0) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_TIMESTAMP_QUOTE + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_TINYINT: {
                        int parsedMinVal = Integer.parseInt(minItem);
                        int parsedMaxVal = Integer.parseInt(maxItem);
                        int userMinVal = Integer.parseInt(left.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_TINYINT_QUOTE, ""));
                        int userMaxVal = Integer.parseInt(right.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_TINYINT_QUOTE, ""));

                        if (parsedMinVal > userMinVal) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_TINYINT_QUOTE + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal < userMaxVal) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_TINYINT_QUOTE + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_BIGINT: {
                        BigInteger parsedMinVal = new BigInteger(minItem);
                        BigInteger parsedMaxVal = new BigInteger(maxItem);
                        BigInteger userMinVal = new BigInteger(left.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_BIGINT_QUOTE, ""));
                        BigInteger userMaxVal = new BigInteger(right.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_BIGINT_QUOTE, ""));

                        if (parsedMinVal.compareTo(userMinVal) > 0) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_BIGINT_QUOTE + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal.compareTo(userMaxVal) < 0) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_BIGINT_QUOTE + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_SMALLINT: {
                        int parsedMinVal = Integer.parseInt(minItem);
                        int parsedMaxVal = Integer.parseInt(maxItem);
                        int userMinVal = Integer.parseInt(left.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_SMALLINT_QUOTE, ""));
                        int userMaxVal = Integer.parseInt(right.toString().substring(0, right.toString().length() - 1).replaceFirst(DATATYPE_SMALLINT_QUOTE, ""));

                        if (parsedMinVal > userMinVal) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(DATATYPE_SMALLINT_QUOTE + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal < userMaxVal) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(DATATYPE_SMALLINT_QUOTE + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    case DATATYPE_VARCHAR: {
                        String parsedMinVal = minItem;
                        String parsedMaxVal = maxItem;
                        String userMinVal = left.toString().substring(0, right.toString().length() - 1).replaceFirst(QUOTE_STRING, "");
                        String userMaxVal = right.toString().substring(0, right.toString().length() - 1).replaceFirst(QUOTE_STRING, "");

                        if (parsedMinVal.compareTo(userMinVal) > 0) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(QUOTE_STRING + parsedMinVal + QUOTE_STRING, new ParsingOptions());
                        }
                        if (parsedMaxVal.compareTo(userMaxVal) < 0) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(QUOTE_STRING + parsedMaxVal + QUOTE_STRING, new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                    default: {
                        int parsedMinVal = Integer.parseInt(minItem);
                        int parsedMaxVal = Integer.parseInt(maxItem);
                        int userMinVal = Integer.parseInt(left.toString());
                        int userMaxVal = Integer.parseInt(right.toString());

                        if (parsedMinVal > userMinVal) {
                            finalPredicateMinExp = left;
                        }
                        else {
                            finalPredicateMinExp = parser.createExpression(rowBufferItems.get(INDEX_AT_MIN_POSITION).toString(), new ParsingOptions());
                        }
                        if (parsedMaxVal < userMaxVal) {
                            finalPredicateMaxExp = right;
                        }
                        else {
                            finalPredicateMaxExp = parser.createExpression(rowBufferItems.get(INDEX_AT_MAX_POSITION).toString(), new ParsingOptions());
                        }

                        finalPredicate = new BetweenPredicate(betweenPredicateValue, finalPredicateMinExp, finalPredicateMaxExp);
                        break;
                    }
                }
                queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, finalPredicate);
                if (!console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                    return false;
                }
            }
        }
        else {
            //if the range is within the processing size limit then we run a single insert query only
            String queryInsert = String.format(INSERT_INTO_CUBE_STRING, cubeName, whereClause);
            if (!console.runQuery(queryRunner, queryInsert, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets whether the expression is supported for processing the create cube query
     *
     * @param createCube createCube
     * @return void
     */
    private boolean isSupportedExpression(CreateCube createCube, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel)
    {
        boolean supportedExpression = false;
        boolean success = true;
        Optional<Expression> expression = createCube.getWhere();
        if (expression.isPresent()) {
            ImmutableSet.Builder<Identifier> identifierBuilder = new ImmutableSet.Builder<>();
            new DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<Identifier>>()
            {
                @Override
                protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<Identifier> builder)
                {
                    builder.add(node);
                    return null;
                }
            }.process(expression.get(), identifierBuilder);

            int sizeIdentifiers = identifierBuilder.build().asList().size();
            if (sizeIdentifiers == SUPPORTED_INDENTIFIER_SIZE) {
                String whereClause = createCube.getWhere().get().toString();
                QualifiedName sourceTableName = createCube.getSourceTableName();
                if (expression.get() instanceof BetweenPredicate) {
                    BetweenPredicate betweenPredicate = (BetweenPredicate) (expression.get());
                    String columnName = betweenPredicate.getValue().toString();

                    String columnDataTypeQuery;
                    String catalogName;
                    String tableName = sourceTableName.getSuffix();

                    checkArgument(tableName.matches("[\\p{Alnum}_]+"), "Invalid table name");
                    if (hasInvalidSymbol(columnName)) {
                        return false;
                    }

                    if (queryRunner.getSession().getCatalog() != null) {
                        catalogName = queryRunner.getSession().getCatalog();
                        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
                        columnDataTypeQuery = String.format(SELECT_DATA_TYPE_STRING, catalogName, tableName, columnName);
                    }
                    else if (sourceTableName.getPrefix().isPresent() && sourceTableName.getPrefix().get().getPrefix().isPresent()) {
                        catalogName = sourceTableName.getPrefix().get().getPrefix().get().toString();
                        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
                        columnDataTypeQuery = String.format(SELECT_DATA_TYPE_STRING, catalogName, tableName, columnName);
                    }
                    else {
                        return false;
                    }
                    if (!processCubeInitialQuery(queryRunner, columnDataTypeQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                        return false;
                    }
                    String resultInitCubeQuery;
                    resultInitCubeQuery = getResultInitCubeQuery();
                    if (resultInitCubeQuery != null) {
                        cubeColumnDataType = resultInitCubeQuery;
                    }

                    if (cubeColumnDataType.contains(DATATYPE_DECIMAL)) {
                        cubeColumnDataType = DATATYPE_DECIMAL;
                    }

                    if (cubeColumnDataType.contains(DATATYPE_VARCHAR)) {
                        cubeColumnDataType = DATATYPE_VARCHAR;
                    }

                    if (!isSupportedDatatype(cubeColumnDataType)) {
                        return false;
                    }

                    if (betweenPredicate.getMin() instanceof LongLiteral || betweenPredicate.getMin() instanceof LongLiteral
                            || betweenPredicate.getMin() instanceof TimestampLiteral || betweenPredicate.getMin() instanceof TimestampLiteral
                            || betweenPredicate.getMin() instanceof GenericLiteral || betweenPredicate.getMin() instanceof StringLiteral
                            || betweenPredicate.getMin() instanceof DoubleLiteral) {
                        if (betweenPredicate.getMax() instanceof LongLiteral || betweenPredicate.getMax() instanceof LongLiteral
                                || betweenPredicate.getMax() instanceof TimestampLiteral || betweenPredicate.getMax() instanceof TimestampLiteral
                                || betweenPredicate.getMax() instanceof GenericLiteral || betweenPredicate.getMax() instanceof StringLiteral
                                || betweenPredicate.getMax() instanceof DoubleLiteral) {
                            //initial query to get the total number of distinct column values in the table
                            String countDistinctQuery = String.format(SELECT_COUNT_DISTINCT_FROM_STRING, columnName, sourceTableName.toString(), whereClause);
                            if (!processCubeInitialQuery(queryRunner, countDistinctQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                                return false;
                            }
                            Long valueCountDistinctQuery = INITIAL_QUERY_RESULT_VALUE;
                            resultInitCubeQuery = getResultInitCubeQuery();
                            if (resultInitCubeQuery != null) {
                                valueCountDistinctQuery = Long.parseLong(resultInitCubeQuery);
                            }
                            if (valueCountDistinctQuery < MAX_BUFFERED_ROWS && valueCountDistinctQuery * rowBufferTempMultiplier < Integer.MAX_VALUE) {
                                supportedExpression = true;
                                rowBufferListSize = (int) ((valueCountDistinctQuery).intValue() * rowBufferTempMultiplier);
                            }
                        }
                    }
                }
                if (expression.get() instanceof ComparisonExpression) {
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
                    if (comparisonExpression.getRight() instanceof LongLiteral) {
                        supportedExpression = true;
                    }

                    String catalogName;
                    String tableName = sourceTableName.getSuffix();
                    String columnName = comparisonExpression.getLeft().toString();
                    String columnDataTypeQuery;

                    checkArgument(tableName.matches("[\\p{Alnum}_]+"), "Invalid table name");
                    if (hasInvalidSymbol(columnName)) {
                        return false;
                    }

                    if (queryRunner.getSession().getCatalog() != null) {
                        catalogName = queryRunner.getSession().getCatalog();
                        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
                        columnDataTypeQuery = String.format(SELECT_DATA_TYPE_STRING, catalogName, tableName, columnName);
                    }
                    else if (sourceTableName.getPrefix().isPresent() && sourceTableName.getPrefix().get().getPrefix().isPresent()) {
                        catalogName = sourceTableName.getPrefix().get().getPrefix().get().toString();
                        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
                        columnDataTypeQuery = String.format(SELECT_DATA_TYPE_STRING, catalogName, tableName, columnName);
                    }

                    else {
                        return false;
                    }
                    if (!processCubeInitialQuery(queryRunner, columnDataTypeQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                        return false;
                    }
                    String resultInitCubeQuery;
                    resultInitCubeQuery = getResultInitCubeQuery();
                    if (resultInitCubeQuery != null) {
                        cubeColumnDataType = resultInitCubeQuery.toLowerCase(Locale.ENGLISH);
                    }

                    if (cubeColumnDataType.contains(DATATYPE_DECIMAL)) {
                        cubeColumnDataType = DATATYPE_DECIMAL;
                    }

                    if (cubeColumnDataType.contains(DATATYPE_VARCHAR)) {
                        cubeColumnDataType = DATATYPE_VARCHAR;
                    }

                    if (!isSupportedDatatype(cubeColumnDataType)) {
                        return false;
                    }

                    if (comparisonExpression.getRight() instanceof GenericLiteral ||
                            comparisonExpression.getRight() instanceof StringLiteral ||
                            comparisonExpression.getRight() instanceof DoubleLiteral ||
                            comparisonExpression.getRight() instanceof LongLiteral ||
                            comparisonExpression.getRight() instanceof TimestampLiteral) {
                        //initial query to get the total number of distinct column values in the table
                        String countDistinctQuery = String.format(SELECT_COUNT_DISTINCT_FROM_STRING, columnName, sourceTableName.toString(), whereClause);
                        if (!processCubeInitialQuery(queryRunner, countDistinctQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
                            return false;
                        }
                        Long valueCountDistinctQuery = INITIAL_QUERY_RESULT_VALUE;
                        resultInitCubeQuery = getResultInitCubeQuery();
                        if (resultInitCubeQuery != null) {
                            valueCountDistinctQuery = Long.parseLong(resultInitCubeQuery);
                        }
                        if (valueCountDistinctQuery < MAX_BUFFERED_ROWS) {
                            supportedExpression = true;
                        }
                    }
                }
            }
        }
        return supportedExpression;
    }

    private boolean isSupportedDatatype(String dataType)
    {
        if (dataType.contains(DATATYPE_DECIMAL)) {
            return true;
        }

        if (supportedDataTypes.contains(dataType)) {
            return true;
        }
        return false;
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

        try (CubeQuery query = queryRunner.startCubeQuery(finalSql)) {
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

    public String getMaxBatchProcessSize()
    {
        return console.getClientOptions().getMaxBatchProcessSize();
    }

    public int getRowBufferListSize()
    {
        return rowBufferListSize;
    }

    private boolean hasInvalidSymbol(String sql)
    {
        if (sql == null) {
            return false;
        }

        Pattern pattern = Pattern.compile(INPUT_REGEX);
        Matcher matcher = pattern.matcher(sql);
        return matcher.find();
    }
}
