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
package io.prestosql.elasticsearch.optimization;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.units.Duration;
import io.prestosql.elasticsearch.ElasticsearchClient;
import io.prestosql.elasticsearch.ElasticsearchColumnHandle;
import io.prestosql.elasticsearch.ElasticsearchConfig;
import io.prestosql.elasticsearch.ElasticsearchConnector;
import io.prestosql.elasticsearch.ElasticsearchMetadata;
import io.prestosql.elasticsearch.ElasticsearchPageSourceProvider;
import io.prestosql.elasticsearch.ElasticsearchSplitManager;
import io.prestosql.elasticsearch.ElasticsearchTableHandle;
import io.prestosql.elasticsearch.ElasticsearchTransactionHandle;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.type.InternalTypeManager;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.prestosql.elasticsearch.ElasticsearchConfig.Security.PASSWORD;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ElasticSearchPlanOptimizerTest
{
    static ElasticsearchConfig elasticsearchConfig;

    static ElasticSearchRowExpressionConverter elasticSearchRowExpressionConverter;

    static FunctionMetadataManager functionMetadataManager;

    static CatalogName catalogName = new CatalogName("elasticsearch");

    static ConnectorTableHandle favouriteCandyConnectorTableHandle = new ElasticsearchTableHandle("default", "favourite_candy", Optional.empty());

    static StandardFunctionResolution standardFunctionResolution;

    static ElasticSearchPlanOptimizer elasticSearchPlanOptimizer;

    static ElasticSearchPlanOptimizerProvider elasticSearchPlanOptimizerProvider;

    static ElasticsearchConnector elasticsearchConnector;

    static ElasticsearchMetadata elasticsearchMetadata;

    static ElasticsearchClient elasticsearchClient;

    static LifeCycleManager lifeCycleManager;

    static ElasticsearchSplitManager elasticsearchSplitManager;

    static ElasticsearchPageSourceProvider elasticsearchPageSourceProvider;

    static PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    static ConnectorTransactionHandle elasticsearchTransactionHandle = ElasticsearchTransactionHandle.INSTANCE;

    static TableHandle favouriteCandyTableHandle = new TableHandle(catalogName, favouriteCandyConnectorTableHandle, elasticsearchTransactionHandle, Optional.empty());
    static TypeSignature returnTypeBoolean = new TypeSignature("boolean");

    static {
        // elastic search config for test
        elasticsearchConfig = new ElasticsearchConfig()
                .setHost("example.com")
                .setPort(9999)
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setRequestTimeout(new Duration(1, SECONDS))
                .setConnectTimeout(new Duration(10, SECONDS))
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setNodeRefreshInterval(new Duration(10, MINUTES))
                .setMaxHttpConnections(100)
                .setHttpThreadCount(30)
                .setTlsEnabled(true)
                .setVerifyHostnames(false)
                .setSecurity(PASSWORD)
                .setPushDownEnabled(true);

        functionMetadataManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        standardFunctionResolution = new FunctionResolution(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        elasticSearchRowExpressionConverter = new ElasticSearchRowExpressionConverter(functionMetadataManager, standardFunctionResolution);
        elasticSearchPlanOptimizer = new ElasticSearchPlanOptimizer(elasticsearchConfig, elasticSearchRowExpressionConverter);
        elasticSearchPlanOptimizerProvider = new ElasticSearchPlanOptimizerProvider(elasticSearchPlanOptimizer);

        elasticsearchClient = new ElasticsearchClient(elasticsearchConfig, Optional.empty());
        elasticsearchMetadata = new ElasticsearchMetadata(new InternalTypeManager(FunctionAndTypeManager.createTestFunctionAndTypeManager()), elasticsearchClient, elasticsearchConfig);

        lifeCycleManager = new LifeCycleManager(Collections.emptyList(), null);
        elasticsearchSplitManager = new ElasticsearchSplitManager(elasticsearchClient);
        elasticsearchPageSourceProvider = new ElasticsearchPageSourceProvider(elasticsearchClient);

        elasticsearchConnector = new ElasticsearchConnector(lifeCycleManager, elasticsearchMetadata, elasticsearchSplitManager, elasticsearchPageSourceProvider, elasticSearchPlanOptimizerProvider);
    }

    private static void assertConstantOptimizationSuccess(String expectedOp, Object value, Type type)
    {
        ElasticSearchConverterContext visitConstantContext = new ElasticSearchConverterContext();
        ConstantExpression constantExpression = new ConstantExpression(value, type);
        String visitConstantResult = elasticSearchRowExpressionConverter.visitConstant(constantExpression, visitConstantContext);
        assertFalse(visitConstantContext.isHasConversionFailed());
        assertEquals(visitConstantResult, expectedOp);
    }

    private static void assertOptimizedQuerySuccess(PlanNode optimize, String expectedOp)
    {
        ProjectNode optimizedProjectNode = (ProjectNode) optimize;
        FilterNode projectNodeSource = (FilterNode) optimizedProjectNode.getSource();
        TableScanNode newTableScanNode = (TableScanNode) projectNodeSource.getSource();
        TableHandle newTableScanNodeTable = newTableScanNode.getTable();
        ElasticsearchTableHandle newTableScanNodeTableConnectorHandle = (ElasticsearchTableHandle) newTableScanNodeTable.getConnectorHandle();
        Optional<String> query = newTableScanNodeTableConnectorHandle.getQuery();
        assertTrue(query.isPresent());
        assertEquals(query.get(), expectedOp);
    }

    private static void assertOptimizedQueryFail(PlanNode optimize)
    {
        ProjectNode optimizedProjectNode = (ProjectNode) optimize;
        FilterNode projectNodeSource = (FilterNode) optimizedProjectNode.getSource();
        TableScanNode newTableScanNode = (TableScanNode) projectNodeSource.getSource();
        TableHandle newTableScanNodeTable = newTableScanNode.getTable();
        ElasticsearchTableHandle newTableScanNodeTableConnectorHandle = (ElasticsearchTableHandle) newTableScanNodeTable.getConnectorHandle();
        Optional<String> query = newTableScanNodeTableConnectorHandle.getQuery();
        assertFalse(query.isPresent());
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("elasticsearch.host", "example.com")
                .put("elasticsearch.port", "9999")
                .put("elasticsearch.default-schema-name", "test")
                .put("elasticsearch.scroll-size", "4000")
                .put("elasticsearch.scroll-timeout", "20s")
                .put("elasticsearch.request-timeout", "1s")
                .put("elasticsearch.connect-timeout", "10s")
                .put("elasticsearch.max-retry-time", "10s")
                .put("elasticsearch.node-refresh-interval", "10m")
                .put("elasticsearch.max-http-connections", "100")
                .put("elasticsearch.http-thread-count", "30")
                .put("elasticsearch.tls.enabled", "true")
                .put("elasticsearch.tls.keystore-path", keystoreFile.toString())
                .put("elasticsearch.tls.keystore-password", "keystore-password")
                .put("elasticsearch.tls.truststore-path", truststoreFile.toString())
                .put("elasticsearch.tls.truststore-password", "truststore-password")
                .put("elasticsearch.tls.verify-hostnames", "false")
                .put("elasticsearch.security", "PASSWORD")
                .put("elasticsearch.pushdown.enabled", "true")
                .build();

        ElasticsearchConfig expected = new ElasticsearchConfig()
                .setHost("example.com")
                .setPort(9999)
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setRequestTimeout(new Duration(1, SECONDS))
                .setConnectTimeout(new Duration(10, SECONDS))
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setNodeRefreshInterval(new Duration(10, MINUTES))
                .setMaxHttpConnections(100)
                .setHttpThreadCount(30)
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTrustStorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setVerifyHostnames(false)
                .setSecurity(PASSWORD)
                .setPushDownEnabled(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testElasticsearchprovider()
    {
        Connector connector = elasticsearchConnector;
        ConnectorPlanOptimizerProvider connectorPlanOptimizerProvider = connector.getConnectorPlanOptimizerProvider();
        Set<ConnectorPlanOptimizer> logicalPlanOptimizers = connectorPlanOptimizerProvider.getLogicalPlanOptimizers();
        assertEquals(1, logicalPlanOptimizers.size());
        assertTrue(logicalPlanOptimizers.contains(elasticSearchPlanOptimizer));
        Set<ConnectorPlanOptimizer> physicalPlanOptimizers = connectorPlanOptimizerProvider.getPhysicalPlanOptimizers();
        assertEquals(0, physicalPlanOptimizers.size());
    }

    @Test
    public void testPlanVisitorOptimizePushDownEnableForEquals()
    {
        // Note: query => select candy from elasticsearch.default.favorite_candy where first_name='Lisa';
        PlanNodeId planNodeId = idAllocator.getNextId();

        Symbol candySymbol = new Symbol("candy");
        Symbol firstNameSymbol = new Symbol("first_name");
        Map<Symbol, ColumnHandle> assignments1 = new HashMap<>(2);
        assignments1.put(candySymbol, new ElasticsearchColumnHandle("candy", VarcharType.VARCHAR));
        assignments1.put(firstNameSymbol, new ElasticsearchColumnHandle("first_name", VarcharType.VARCHAR));

        PlanNode source = new TableScanNode(planNodeId, favouriteCandyTableHandle, new ArrayList<>(assignments1.keySet()), assignments1, TupleDomain.all(), Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);

        ArrayList<TypeSignatureParameter> typeSignatureParameters = new ArrayList<>();
        typeSignatureParameters.add(TypeSignatureParameter.of(2147483647));
        TypeSignature typeSignature = new TypeSignature("varchar", typeSignatureParameters);
        TypeSignature typeSignature1 = new TypeSignature("varchar", typeSignatureParameters);
        List<TypeSignature> typeSignatures = new ArrayList<>();
        typeSignatures.add(typeSignature1);
        typeSignatures.add(typeSignature);
        Signature equalFunctionSignature = new Signature(new QualifiedObjectName("presto", "default", "$operator$equal"), FunctionKind.SCALAR, returnTypeBoolean, typeSignatures);
        FunctionHandle functionHandle = new BuiltInFunctionHandle(equalFunctionSignature);
        List<RowExpression> arguments = new ArrayList<>();
        arguments.add(new VariableReferenceExpression("first_name", VarcharType.VARCHAR));
        arguments.add(new ConstantExpression(new StringLiteral("Lisa").getSlice(), VarcharType.VARCHAR));

        RowExpression equalCallExpression = new CallExpression("EQUAL", functionHandle, BooleanType.BOOLEAN, arguments, Optional.empty());
        PlanNode filterNode = new FilterNode(idAllocator.getNextId(), source, equalCallExpression);

        Assignments assignments = new Assignments((Collections.singletonMap(candySymbol, new VariableReferenceExpression("candy", VarcharType.VARCHAR))));
        PlanNode projectNode = new ProjectNode(planNodeId, filterNode, assignments);

        PlanNode optimize = elasticSearchPlanOptimizer.optimize(projectNode, null, null, null, idAllocator);
        assertOptimizedQuerySuccess(optimize, "(first_name = 'Lisa')");
    }

    @Test
    public void testPlanVisitorOptimizePushDownEnableForOR()
    {
        // Note: query => select candy from elasticsearch.default.favorite_candy where first_name='Lisa' OR first_name!='Lis' ;
        PlanNodeId planNodeId = idAllocator.getNextId();

        Symbol candySymbol = new Symbol("candy");
        Symbol firstNameSymbol = new Symbol("first_name");
        Map<Symbol, ColumnHandle> assignments1 = new HashMap<>(2);
        assignments1.put(candySymbol, new ElasticsearchColumnHandle("candy", VarcharType.VARCHAR));
        assignments1.put(firstNameSymbol, new ElasticsearchColumnHandle("first_name", VarcharType.VARCHAR));

        PlanNode source = new TableScanNode(planNodeId, favouriteCandyTableHandle, new ArrayList<>(assignments1.keySet()), assignments1, TupleDomain.all(), Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);

        // Note: constructing first_name = 'Lisa'
        ArrayList<TypeSignatureParameter> typeSignatureParameters = new ArrayList<>();
        typeSignatureParameters.add(TypeSignatureParameter.of(2147483647));
        TypeSignature typeSignature = new TypeSignature("varchar", typeSignatureParameters);
        TypeSignature typeSignature1 = new TypeSignature("varchar", typeSignatureParameters);
        List<TypeSignature> typeSignatures = new ArrayList<>();
        typeSignatures.add(typeSignature1);
        typeSignatures.add(typeSignature);
        Signature equalFunctionSignature = new Signature(new QualifiedObjectName("presto", "default", "$operator$equal"), FunctionKind.SCALAR, returnTypeBoolean, typeSignatures);
        FunctionHandle functionHandle = new BuiltInFunctionHandle(equalFunctionSignature);
        List<RowExpression> arguments = new ArrayList<>();
        arguments.add(new VariableReferenceExpression("first_name", VarcharType.VARCHAR));
        arguments.add(new ConstantExpression(new StringLiteral("Lisa").getSlice(), VarcharType.VARCHAR));
        RowExpression equalCallExpression = new CallExpression("EQUAL", functionHandle, BooleanType.BOOLEAN, arguments, Optional.empty());
        List<RowExpression> arguments1 = new ArrayList<>();

        // Note: constructing first='Lis'
        arguments1.add(new VariableReferenceExpression("first_name", VarcharType.VARCHAR));
        arguments1.add(new ConstantExpression(new StringLiteral("Lis").getSlice(), VarcharType.VARCHAR));
        RowExpression equalCallExpression1 = new CallExpression("EQUAL", functionHandle, BooleanType.BOOLEAN, arguments1, Optional.empty());

        // Note: constructing first_name = 'Lisa' OR first='Lis'
        RowExpression specialForm = new SpecialForm(SpecialForm.Form.OR, BooleanType.BOOLEAN, equalCallExpression, equalCallExpression1);

        PlanNode filterNode = new FilterNode(idAllocator.getNextId(), source, specialForm);

        Assignments assignments = new Assignments((Collections.singletonMap(candySymbol, new VariableReferenceExpression("candy", VarcharType.VARCHAR))));
        PlanNode projectNode = new ProjectNode(planNodeId, filterNode, assignments);

        PlanNode optimize = elasticSearchPlanOptimizer.optimize(projectNode, null, null, null, idAllocator);
        assertOptimizedQuerySuccess(optimize, "(first_name = 'Lisa') OR (first_name = 'Lis')");
    }

    @Test
    public void testPlanVisitorOptimizePushDownEnableForUnsupported()
    {
        // Note: query => A dummy query which is unsupported
        PlanNodeId planNodeId = idAllocator.getNextId();

        Symbol candySymbol = new Symbol("candy");
        Symbol firstNameSymbol = new Symbol("first_name");
        Map<Symbol, ColumnHandle> assignments1 = new HashMap<>(2);
        assignments1.put(candySymbol, new ElasticsearchColumnHandle("candy", VarcharType.VARCHAR));
        assignments1.put(firstNameSymbol, new ElasticsearchColumnHandle("first_name", VarcharType.VARCHAR));

        PlanNode source = new TableScanNode(planNodeId, favouriteCandyTableHandle, new ArrayList<>(assignments1.keySet()), assignments1, TupleDomain.all(), Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);

        // Note: constructing first_name = 'Lisa'
        ArrayList<TypeSignatureParameter> typeSignatureParameters = new ArrayList<>();
        typeSignatureParameters.add(TypeSignatureParameter.of(2147483647));
        TypeSignature typeSignature = new TypeSignature("varchar", typeSignatureParameters);
        TypeSignature typeSignature1 = new TypeSignature("varchar", typeSignatureParameters);
        List<TypeSignature> typeSignatures = new ArrayList<>();
        typeSignatures.add(typeSignature1);
        typeSignatures.add(typeSignature);
        Signature equalFunctionSignature = new Signature(new QualifiedObjectName("presto", "default", "$operator$equal"), FunctionKind.SCALAR, returnTypeBoolean, typeSignatures);
        FunctionHandle functionHandle = new BuiltInFunctionHandle(equalFunctionSignature);
        List<RowExpression> arguments = new ArrayList<>();
        arguments.add(new VariableReferenceExpression("first_name", VarcharType.VARCHAR));
        arguments.add(new ConstantExpression(new StringLiteral("Lisa").getSlice(), VarcharType.VARCHAR));
        RowExpression equalCallExpression = new CallExpression("EQUAL", functionHandle, BooleanType.BOOLEAN, arguments, Optional.empty());
        List<RowExpression> arguments1 = new ArrayList<>();

        arguments1.add(new VariableReferenceExpression("first_name", VarcharType.VARCHAR));
        arguments1.add(new ConstantExpression(new StringLiteral("Lis").getSlice(), VarcharType.VARCHAR));
        RowExpression equalCallExpression1 = new CallExpression("EQUAL", functionHandle, BooleanType.BOOLEAN, arguments1, Optional.empty());

        RowExpression specialForm = new SpecialForm(SpecialForm.Form.IF, BooleanType.BOOLEAN, equalCallExpression, equalCallExpression1);

        PlanNode filterNode = new FilterNode(idAllocator.getNextId(), source, specialForm);

        Assignments assignments = new Assignments((Collections.singletonMap(candySymbol, new VariableReferenceExpression("candy", VarcharType.VARCHAR))));
        PlanNode projectNode = new ProjectNode(planNodeId, filterNode, assignments);

        PlanNode optimize = elasticSearchPlanOptimizer.optimize(projectNode, null, null, null, idAllocator);
        assertOptimizedQueryFail(optimize);
    }

    @Test
    public void testUnsupportedOptimizationRowExpression()
    {
        ElasticSearchConverterContext visitLambdaContext = new ElasticSearchConverterContext();
        elasticSearchRowExpressionConverter.visitLambda(null, visitLambdaContext);
        assertTrue(visitLambdaContext.isHasConversionFailed());

        ElasticSearchConverterContext visitInputReferenceContext = new ElasticSearchConverterContext();
        elasticSearchRowExpressionConverter.visitLambda(null, visitInputReferenceContext);
        assertTrue(visitInputReferenceContext.isHasConversionFailed());
    }

    private static void assertOptimizationSuccessSpecialForm(String expectedOp, SpecialForm.Form formType, Type returnType, RowExpression... arguments)
    {
        SpecialForm specialForm = new SpecialForm(formType, returnType, arguments);
        String actualOp = elasticSearchRowExpressionConverter.visitSpecialForm(specialForm, new ElasticSearchConverterContext());
        assertEquals(actualOp, expectedOp);
    }

    @Test
    public void testRowExpressionSpecialForm()
    {
        VariableReferenceExpression variableReferenceExpression = new VariableReferenceExpression("name", VarcharType.VARCHAR);

        ConstantExpression constantExpression1 = new ConstantExpression(10, IntegerType.INTEGER);
        ConstantExpression constantExpression2 = new ConstantExpression(15, IntegerType.INTEGER);

        //test is_null rowExpression conversion
        assertOptimizationSuccessSpecialForm("NOT _exists_:name", SpecialForm.Form.IS_NULL, BooleanType.BOOLEAN, variableReferenceExpression);
        //testing between rowExpression conversion
        assertOptimizationSuccessSpecialForm("(name: (>=10 AND <=15))", SpecialForm.Form.BETWEEN, BooleanType.BOOLEAN, variableReferenceExpression, constantExpression1, constantExpression2);
        //testing in rowExpression conversion
        assertOptimizationSuccessSpecialForm("name:10 OR name:15", SpecialForm.Form.IN, BooleanType.BOOLEAN, variableReferenceExpression, constantExpression1, constantExpression2);
    }

    @Test
    public void testRowExpressionSpecialFormDereference()
    {
        ConstantExpression constantExpression3 = new ConstantExpression(1, IntegerType.INTEGER);
        //testing dereference rowExpression conversion
        ArrayList<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(Optional.of("inner1"), IntegerType.INTEGER));
        fields.add(new RowType.Field(Optional.of("inner2"), IntegerType.INTEGER));
        RowType rowType = RowType.from(fields);
        VariableReferenceExpression variableReferenceExpression1 = new VariableReferenceExpression("name", rowType);
        assertOptimizationSuccessSpecialForm("name.inner2", SpecialForm.Form.DEREFERENCE, IntegerType.INTEGER, variableReferenceExpression1, constantExpression3);
    }

    @Test
    public void testRowExpressionConverterVisitConstant()
    {
        assertConstantOptimizationSuccess("null", null, VarcharType.VARCHAR);
        assertConstantOptimizationSuccess("false", false, BooleanType.BOOLEAN);
        assertConstantOptimizationSuccess("true", true, BooleanType.BOOLEAN);
        assertConstantOptimizationSuccess("10", 10, IntegerType.INTEGER);
        assertConstantOptimizationSuccess("10.0", 10.0, DoubleType.DOUBLE);
        assertConstantOptimizationSuccess("0.000000", 1L, RealType.REAL);
        assertConstantOptimizationSuccess("'value'", new StringLiteral("value").getSlice(), VarcharType.VARCHAR);
    }
}
