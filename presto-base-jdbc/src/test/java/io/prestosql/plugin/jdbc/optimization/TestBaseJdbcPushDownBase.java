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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTransactionHandle;
import io.prestosql.utils.HetuConfig;
import org.h2.Driver;

import java.lang.invoke.MethodHandle;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BOOLEAN;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_REAL;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule.FULL_PUSHDOWN;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.TestingIdType.ID;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestBaseJdbcPushDownBase
{
    protected static final String connectionUrl = "testUrl";
    protected static final JdbcClient testClient = new TestPushwonClient();
    protected static final CatalogName catalogName = new CatalogName("catalog");
    protected static final Metadata metadata = MetadataManager.createTestMetadataManager();
    protected static final JdbcTableHandle testTable = new JdbcTableHandle(
            new SchemaTableName("schema", "table"), null, null, "table");
    protected static final JdbcTableHandle testLeftTable = new JdbcTableHandle(
            new SchemaTableName("schema", "left_table"), null, null, "left_table");
    protected static final JdbcTableHandle testRightTable = new JdbcTableHandle(
            new SchemaTableName("schema", "right_table"), null, null, "right_table");

    protected static final JdbcColumnHandle leftId = bigintColumn("leftid");
    protected static final JdbcColumnHandle leftValue = varcharColumn("leftvalue");
    protected static final JdbcColumnHandle rightId = bigintColumn("rightid");
    protected static final JdbcColumnHandle rightValue = varcharColumn("rightvalue");

    protected static final JdbcColumnHandle booleanCol = booleanColumn("booleanCol");
    protected static final JdbcColumnHandle intCol = integerColumn("intCol");
    protected static final JdbcColumnHandle realCol = realColumn("realCol");
    protected static final JdbcColumnHandle doubleCol = doubleColumn("doubleCol");
    protected static final JdbcColumnHandle decimalCol = decimalColumn("decimalCol");

    protected static final JdbcColumnHandle regionId = integerColumn("regionid");
    protected static final JdbcColumnHandle city = varcharColumn("city");
    protected static final JdbcColumnHandle fare = doubleColumn("fare");
    protected static final JdbcColumnHandle amount = bigintColumn("amount");

    protected static final JdbcColumnHandle startValueColumn = bigintColumn("startValue");
    protected static final JdbcColumnHandle endValueColumn = bigintColumn("endValue");

    protected static final Symbol startValue = symbol("startValue");
    protected static final Symbol endValue = symbol("endValue");

    protected static final Map<Symbol, Type> types = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("regionid"), IntegerType.INTEGER)
            .put(new Symbol("city"), VarcharType.VARCHAR)
            .put(new Symbol("fare"), DoubleType.DOUBLE)
            .put(new Symbol("amount"), BigintType.BIGINT)
            .put(new Symbol("booleanCol"), BooleanType.BOOLEAN)
            .put(new Symbol("intCol"), IntegerType.INTEGER)
            .put(new Symbol("realCol"), RealType.REAL)
            .put(new Symbol("doubleCol"), DoubleType.DOUBLE)
            .put(new Symbol("decimalCol"), DecimalType.createDecimalType(10, 2))
            .put(new Symbol("timeCol"), TimestampType.TIMESTAMP)
            .put(new Symbol("leftid"), BigintType.BIGINT)
            .put(new Symbol("leftvalue"), VarcharType.VARCHAR)
            .put(new Symbol("rightid"), BigintType.BIGINT)
            .put(new Symbol("rightvalue"), VarcharType.VARCHAR)
            .put(new Symbol("startValue"), BigintType.BIGINT)
            .put(new Symbol("endValue"), BigintType.BIGINT)
            .build();

    protected final TypeProvider typeProvider = TypeProvider.copyOf(types);

    protected static class SessionHolder
    {
        private final ConnectorSession connectorSession;
        private final Session session;

        public SessionHolder()
        {
            connectorSession = SESSION;
            session = TestingSession.testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties().getSessionProperties(), new HetuConfig())).build();
        }

        public ConnectorSession getConnectorSession()
        {
            return connectorSession;
        }

        public Session getSession()
        {
            return session;
        }
    }

    protected static Symbol symbol(String name)
    {
        return new Symbol(name);
    }

    protected static VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(name, types.get(symbol(name)));
    }

    protected static VariableReferenceExpression variable(String name, Type type)
    {
        return new VariableReferenceExpression(name, type);
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql,
                new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)));
    }

    protected RowExpression toRowExpression(Expression expression, Session session)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = ExpressionAnalyzer.analyzeExpressions(
                session,
                metadata,
                new SqlParser(),
                typeProvider,
                ImmutableList.of(expression),
                ImmutableList.of(),
                WarningCollector.NOOP,
                false
        ).getExpressionTypes();
        return SqlToRowExpressionTranslator.translate(expression, FunctionKind.SCALAR, expressionTypes, ImmutableMap.of(), metadata, session, false);
    }

    protected TableScanNode tableScan(PlanBuilder planBuilder, JdbcTableHandle connectorTableHandle, JdbcColumnHandle... columnHandles)
    {
        List<Symbol> symbols = Arrays.stream(columnHandles).map(column -> new Symbol(column.getColumnName().toLowerCase(Locale.ENGLISH))).collect(toImmutableList());
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < symbols.size(); i++) {
            assignments.put(symbols.get(i), columnHandles[i]);
        }
        TableHandle tableHandle = new TableHandle(
                catalogName,
                connectorTableHandle,
                TestingTransactionHandle.create(),
                Optional.empty());
        return planBuilder.tableScan(
                tableHandle,
                symbols,
                assignments.build());
    }

    protected FilterNode filter(PlanBuilder planBuilder, PlanNode source, RowExpression predicate)
    {
        return planBuilder.filter(predicate, source);
    }

    protected ProjectNode project(PlanBuilder planBuilder, PlanNode source, List<String> columnNames)
    {
        Map<String, Symbol> incomingColumns = source.getOutputSymbols().stream().collect(toMap(Symbol::getName, identity()));
        Assignments.Builder assignmentBuilder = Assignments.builder();
        columnNames.forEach(columnName -> {
            Symbol symbol = requireNonNull(incomingColumns.get(columnName), "Couldn't find the incoming column " + columnName);
            assignmentBuilder.put(symbol, new VariableReferenceExpression(columnName, types.get(new Symbol(columnName))));
        });
        return planBuilder.project(assignmentBuilder.build(), source);
    }

    protected LimitNode limit(PlanBuilder pb, long count, PlanNode source)
    {
        return new LimitNode(pb.getIdAllocator().getNextId(), source, count, false);
    }

    protected RowExpression getRowExpression(String sqlExpression, SessionHolder sessionHolder)
    {
        return toRowExpression(expression(sqlExpression), sessionHolder.getSession());
    }

    protected PlanBuilder createPlanBuilder()
    {
        return new PlanBuilder(new PlanNodeIdAllocator(), metadata);
    }

    protected static JdbcColumnHandle booleanColumn(String name)
    {
        return new JdbcColumnHandle(name, JDBC_BOOLEAN, BooleanType.BOOLEAN, true);
    }

    protected static JdbcColumnHandle integerColumn(String name)
    {
        return new JdbcColumnHandle(name, JDBC_INTEGER, IntegerType.INTEGER, true);
    }

    protected static JdbcColumnHandle bigintColumn(String name)
    {
        return new JdbcColumnHandle(name, JDBC_BIGINT, BigintType.BIGINT, true);
    }

    private static JdbcColumnHandle realColumn(String name)
    {
        return new JdbcColumnHandle(name, JDBC_REAL, RealType.REAL, true);
    }

    private static JdbcColumnHandle doubleColumn(String name)
    {
        return new JdbcColumnHandle(name, JDBC_DOUBLE, DoubleType.DOUBLE, true);
    }

    private static JdbcColumnHandle varcharColumn(String name)
    {
        return new JdbcColumnHandle(name, JDBC_VARCHAR, VarcharType.VARCHAR, true);
    }

    private static JdbcColumnHandle decimalColumn(String name)
    {
        return new JdbcColumnHandle(
                name,
                new JdbcTypeHandle(Types.DECIMAL, Optional.of("decimal(10,2)"), 10, 2, Optional.empty()),
                DecimalType.createDecimalType(10, 2),
                true);
    }

    protected static class TestPushwonClient
            extends BaseJdbcClient
    {
        public TestPushwonClient()
        {
            super(new BaseJdbcConfig(), "`", new DriverConnectionFactory(new Driver(), connectionUrl, Optional.empty(), Optional.empty(), new Properties()));
        }

        @Override
        public Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types)
        {
            Map<String, ColumnHandle> columns = new HashMap<>();
            for (Map.Entry<String, Type> entry : types.entrySet()) {
                ColumnHandle columnHandle;
                String name = entry.getKey();
                Type type = entry.getValue();
                if (type instanceof BigintType) {
                    columnHandle = bigintColumn(name);
                }
                else if (type instanceof IntegerType) {
                    columnHandle = integerColumn(name);
                }
                else if (type instanceof DoubleType) {
                    columnHandle = doubleColumn(name);
                }
                else if (type instanceof VarcharType) {
                    columnHandle = varcharColumn(name);
                }
                else {
                    throw new RuntimeException(format("Unknown column type [%s]", type));
                }
                columns.put(name, columnHandle);
            }

            return columns;
        }

        @Override
        public Optional<QueryGenerator<JdbcQueryGeneratorResult>> getQueryGenerator(RowExpressionService rowExpressionService)
        {
            JdbcPushDownParameter pushDownParameter = new JdbcPushDownParameter("'", false, FULL_PUSHDOWN);
            return Optional.of(new BaseJdbcQueryGenerator(pushDownParameter, new BaseJdbcRowExpressionConverter(rowExpressionService), new BaseJdbcSqlStatementWriter(pushDownParameter)));
        }
    }

    protected static class TestTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            for (Type type : getTypes()) {
                if (signature.getBase().equals(type.getTypeSignature().getBase())) {
                    return type;
                }
            }
            throw new TypeNotFoundException(signature);
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return getType(new TypeSignature(baseTypeName, typeParameters));
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, VARCHAR, VARBINARY, TIMESTAMP, DATE, ID, HYPER_LOG_LOG);
        }

        @Override
        public Collection<ParametricType> getParametricTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
        {
            throw new UnsupportedOperationException();
        }
    }
}
