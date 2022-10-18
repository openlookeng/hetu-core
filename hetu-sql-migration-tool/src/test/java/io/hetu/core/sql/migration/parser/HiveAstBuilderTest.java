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
package io.hetu.core.sql.migration.parser;

import io.hetu.core.migration.source.hive.HiveSqlParser;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class HiveAstBuilderTest
{
    @Mock
    private ParsingOptions mockParsingOptions;

    private HiveAstBuilder hiveAstBuilderUnderTest;

    @BeforeMethod
    public void setUp()
    {
        initMocks(this);
        hiveAstBuilderUnderTest = new HiveAstBuilder(mockParsingOptions);
    }

    @Test
    public void testAddDiff1()
    {
        // Setup
        // Run the test
        hiveAstBuilderUnderTest.addDiff(DiffType.DELETED, "source", "target", "message");

        // Verify the results
    }

    @Test
    public void testAddDiff2()
    {
        // Setup
        // Run the test
        hiveAstBuilderUnderTest.addDiff(DiffType.DELETED, "source", "message");

        // Verify the results
    }

    @Test
    public void testVisitSingleStatement()
    {
        // Setup
        final HiveSqlParser.SingleStatementContext context = new HiveSqlParser.SingleStatementContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSingleStatement(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitStandaloneExpression()
    {
        // Setup
        final HiveSqlParser.StandaloneExpressionContext context = new HiveSqlParser.StandaloneExpressionContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitStandaloneExpression(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitStandalonePathSpecification()
    {
        // Setup
        final HiveSqlParser.StandalonePathSpecificationContext context = new HiveSqlParser.StandalonePathSpecificationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitStandalonePathSpecification(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUse()
    {
        // Setup
        final HiveSqlParser.UseContext context = new HiveSqlParser.UseContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitUse(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateSchema()
    {
        // Setup
        final HiveSqlParser.CreateSchemaContext context = new HiveSqlParser.CreateSchemaContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateSchema(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropSchema()
    {
        // Setup
        final HiveSqlParser.DropSchemaContext context = new HiveSqlParser.DropSchemaContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropSchema(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowSchemas()
    {
        // Setup
        final HiveSqlParser.ShowSchemasContext context = new HiveSqlParser.ShowSchemasContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowSchemas(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterSchema()
    {
        // Setup
        final HiveSqlParser.AlterSchemaContext context = new HiveSqlParser.AlterSchemaContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterSchema(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDescribeSchema()
    {
        // Setup
        final HiveSqlParser.DescribeSchemaContext context = new HiveSqlParser.DescribeSchemaContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDescribeSchema(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateView()
    {
        // Setup
        final HiveSqlParser.CreateViewContext context = new HiveSqlParser.CreateViewContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterView()
    {
        // Setup
        final HiveSqlParser.AlterViewContext context = new HiveSqlParser.AlterViewContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowViews()
    {
        // Setup
        final HiveSqlParser.ShowViewsContext context = new HiveSqlParser.ShowViewsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowViews(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropView()
    {
        // Setup
        final HiveSqlParser.DropViewContext context = new HiveSqlParser.DropViewContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateTable()
    {
        // Setup
        final HiveSqlParser.CreateTableContext context = new HiveSqlParser.CreateTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateTableAsSelect()
    {
        // Setup
        final HiveSqlParser.CreateTableAsSelectContext context = new HiveSqlParser.CreateTableAsSelectContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateTableAsSelect(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateTableLike()
    {
        // Setup
        final HiveSqlParser.CreateTableLikeContext context = new HiveSqlParser.CreateTableLikeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateTableLike(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowTables()
    {
        // Setup
        final HiveSqlParser.ShowTablesContext context = new HiveSqlParser.ShowTablesContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowTables(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowCreateTable()
    {
        // Setup
        final HiveSqlParser.ShowCreateTableContext context = new HiveSqlParser.ShowCreateTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowCreateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRenameTable()
    {
        // Setup
        final HiveSqlParser.RenameTableContext context = new HiveSqlParser.RenameTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitRenameTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCommentTable()
    {
        // Setup
        final HiveSqlParser.CommentTableContext context = new HiveSqlParser.CommentTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCommentTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableAddConstraint()
    {
        // Setup
        final HiveSqlParser.AlterTableAddConstraintContext context = new HiveSqlParser.AlterTableAddConstraintContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableAddConstraint(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableChangeConstraint()
    {
        // Setup
        final HiveSqlParser.AlterTableChangeConstraintContext context = new HiveSqlParser.AlterTableChangeConstraintContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableChangeConstraint(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableDropConstraint()
    {
        // Setup
        final HiveSqlParser.AlterTableDropConstraintContext context = new HiveSqlParser.AlterTableDropConstraintContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableDropConstraint(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableSerde()
    {
        // Setup
        final HiveSqlParser.AlterTableSerdeContext context = new HiveSqlParser.AlterTableSerdeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableSerde(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterRemoveSerde()
    {
        // Setup
        final HiveSqlParser.AlterRemoveSerdeContext context = new HiveSqlParser.AlterRemoveSerdeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterRemoveSerde(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableStorage()
    {
        // Setup
        final HiveSqlParser.AlterTableStorageContext context = new HiveSqlParser.AlterTableStorageContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableStorage(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableSkewed()
    {
        // Setup
        final HiveSqlParser.AlterTableSkewedContext context = new HiveSqlParser.AlterTableSkewedContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableSkewed(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableNotSkewed()
    {
        // Setup
        final HiveSqlParser.AlterTableNotSkewedContext context = new HiveSqlParser.AlterTableNotSkewedContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableNotSkewed(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableNotAsDirectories()
    {
        // Setup
        final HiveSqlParser.AlterTableNotAsDirectoriesContext context = new HiveSqlParser.AlterTableNotAsDirectoriesContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableNotAsDirectories(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableSetSkewedLocation()
    {
        // Setup
        final HiveSqlParser.AlterTableSetSkewedLocationContext context = new HiveSqlParser.AlterTableSetSkewedLocationContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableSetSkewedLocation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableAddPartition()
    {
        // Setup
        final HiveSqlParser.AlterTableAddPartitionContext context = new HiveSqlParser.AlterTableAddPartitionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableAddPartition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableRenamePartition()
    {
        // Setup
        final HiveSqlParser.AlterTableRenamePartitionContext context = new HiveSqlParser.AlterTableRenamePartitionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableRenamePartition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableExchangePartition()
    {
        // Setup
        final HiveSqlParser.AlterTableExchangePartitionContext context = new HiveSqlParser.AlterTableExchangePartitionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableExchangePartition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableRecoverPartitions()
    {
        // Setup
        final HiveSqlParser.AlterTableRecoverPartitionsContext context = new HiveSqlParser.AlterTableRecoverPartitionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableRecoverPartitions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableDropPartition()
    {
        // Setup
        final HiveSqlParser.AlterTableDropPartitionContext context = new HiveSqlParser.AlterTableDropPartitionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableDropPartition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableArchivePartition()
    {
        // Setup
        final HiveSqlParser.AlterTableArchivePartitionContext context = new HiveSqlParser.AlterTableArchivePartitionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableArchivePartition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTablePartitionFileFormat()
    {
        // Setup
        final HiveSqlParser.AlterTablePartitionFileFormatContext context = new HiveSqlParser.AlterTablePartitionFileFormatContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTablePartitionFileFormat(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTablePartitionLocation()
    {
        // Setup
        final HiveSqlParser.AlterTablePartitionLocationContext context = new HiveSqlParser.AlterTablePartitionLocationContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTablePartitionLocation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTablePartitionTouch()
    {
        // Setup
        final HiveSqlParser.AlterTablePartitionTouchContext context = new HiveSqlParser.AlterTablePartitionTouchContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTablePartitionTouch(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTablePartitionProtections()
    {
        // Setup
        final HiveSqlParser.AlterTablePartitionProtectionsContext context = new HiveSqlParser.AlterTablePartitionProtectionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTablePartitionProtections(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTablePartitionCompact()
    {
        // Setup
        final HiveSqlParser.AlterTablePartitionCompactContext context = new HiveSqlParser.AlterTablePartitionCompactContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTablePartitionCompact(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTablePartitionConcatenate()
    {
        // Setup
        final HiveSqlParser.AlterTablePartitionConcatenateContext context = new HiveSqlParser.AlterTablePartitionConcatenateContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTablePartitionConcatenate(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTablePartitionUpdateColumns()
    {
        // Setup
        final HiveSqlParser.AlterTablePartitionUpdateColumnsContext context = new HiveSqlParser.AlterTablePartitionUpdateColumnsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTablePartitionUpdateColumns(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableChangeColumn()
    {
        // Setup
        final HiveSqlParser.AlterTableChangeColumnContext context = new HiveSqlParser.AlterTableChangeColumnContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterTableChangeColumn(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowTableExtended()
    {
        // Setup
        final HiveSqlParser.ShowTableExtendedContext context = new HiveSqlParser.ShowTableExtendedContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowTableExtended(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowTableProperties()
    {
        // Setup
        final HiveSqlParser.ShowTablePropertiesContext context = new HiveSqlParser.ShowTablePropertiesContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowTableProperties(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTruncateTable()
    {
        // Setup
        final HiveSqlParser.TruncateTableContext context = new HiveSqlParser.TruncateTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitTruncateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitMsckRepairTable()
    {
        // Setup
        final HiveSqlParser.MsckRepairTableContext context = new HiveSqlParser.MsckRepairTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitMsckRepairTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateMaterializedView()
    {
        // Setup
        final HiveSqlParser.CreateMaterializedViewContext context = new HiveSqlParser.CreateMaterializedViewContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateMaterializedView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropMaterializedView()
    {
        // Setup
        final HiveSqlParser.DropMaterializedViewContext context = new HiveSqlParser.DropMaterializedViewContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropMaterializedView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterMaterializedView()
    {
        // Setup
        final HiveSqlParser.AlterMaterializedViewContext context = new HiveSqlParser.AlterMaterializedViewContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterMaterializedView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowMaterializedViews()
    {
        // Setup
        final HiveSqlParser.ShowMaterializedViewsContext context = new HiveSqlParser.ShowMaterializedViewsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowMaterializedViews(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateFunction()
    {
        // Setup
        final HiveSqlParser.CreateFunctionContext context = new HiveSqlParser.CreateFunctionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropFunction()
    {
        // Setup
        final HiveSqlParser.DropFunctionContext context = new HiveSqlParser.DropFunctionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitReloadFunctions()
    {
        // Setup
        final HiveSqlParser.ReloadFunctionsContext context = new HiveSqlParser.ReloadFunctionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitReloadFunctions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateCube()
    {
        // Setup
        final HiveSqlParser.CreateCubeContext context = new HiveSqlParser.CreateCubeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateCube(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInsertCube()
    {
        // Setup
        final HiveSqlParser.InsertCubeContext context = new HiveSqlParser.InsertCubeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInsertCube(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInsertOverwriteCube()
    {
        // Setup
        final HiveSqlParser.InsertOverwriteCubeContext context = new HiveSqlParser.InsertOverwriteCubeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInsertOverwriteCube(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropCube()
    {
        // Setup
        final HiveSqlParser.DropCubeContext context = new HiveSqlParser.DropCubeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropCube(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowCubes()
    {
        // Setup
        final HiveSqlParser.ShowCubesContext context = new HiveSqlParser.ShowCubesContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowCubes(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateIndex()
    {
        // Setup
        final HiveSqlParser.CreateIndexContext context = new HiveSqlParser.CreateIndexContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateIndex(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropIndex()
    {
        // Setup
        final HiveSqlParser.DropIndexContext context = new HiveSqlParser.DropIndexContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropIndex(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterIndex()
    {
        // Setup
        final HiveSqlParser.AlterIndexContext context = new HiveSqlParser.AlterIndexContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAlterIndex(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowIndex()
    {
        // Setup
        final HiveSqlParser.ShowIndexContext context = new HiveSqlParser.ShowIndexContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowIndex(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowPartitions()
    {
        // Setup
        final HiveSqlParser.ShowPartitionsContext context = new HiveSqlParser.ShowPartitionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowPartitions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDescribePartition()
    {
        // Setup
        final HiveSqlParser.DescribePartitionContext context = new HiveSqlParser.DescribePartitionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDescribePartition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDescribeFunction()
    {
        // Setup
        final HiveSqlParser.DescribeFunctionContext context = new HiveSqlParser.DescribeFunctionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDescribeFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateMacro()
    {
        // Setup
        final HiveSqlParser.CreateMacroContext context = new HiveSqlParser.CreateMacroContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateMacro(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropMacro()
    {
        // Setup
        final HiveSqlParser.DropMacroContext context = new HiveSqlParser.DropMacroContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropMacro(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowRoleGrant()
    {
        // Setup
        final HiveSqlParser.ShowRoleGrantContext context = new HiveSqlParser.ShowRoleGrantContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowRoleGrant(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowPrincipals()
    {
        // Setup
        final HiveSqlParser.ShowPrincipalsContext context = new HiveSqlParser.ShowPrincipalsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowPrincipals(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowLocks()
    {
        // Setup
        final HiveSqlParser.ShowLocksContext context = new HiveSqlParser.ShowLocksContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowLocks(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowConf()
    {
        // Setup
        final HiveSqlParser.ShowConfContext context = new HiveSqlParser.ShowConfContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowConf(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowTransactions()
    {
        // Setup
        final HiveSqlParser.ShowTransactionsContext context = new HiveSqlParser.ShowTransactionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowTransactions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowCompactions()
    {
        // Setup
        final HiveSqlParser.ShowCompactionsContext context = new HiveSqlParser.ShowCompactionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowCompactions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAbortTransactions()
    {
        // Setup
        final HiveSqlParser.AbortTransactionsContext context = new HiveSqlParser.AbortTransactionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAbortTransactions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLoadData()
    {
        // Setup
        final HiveSqlParser.LoadDataContext context = new HiveSqlParser.LoadDataContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitLoadData(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitMerge()
    {
        // Setup
        final HiveSqlParser.MergeContext context = new HiveSqlParser.MergeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitMerge(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExportData()
    {
        // Setup
        final HiveSqlParser.ExportDataContext context = new HiveSqlParser.ExportDataContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitExportData(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitImportData()
    {
        // Setup
        final HiveSqlParser.ImportDataContext context = new HiveSqlParser.ImportDataContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitImportData(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropTable()
    {
        // Setup
        final HiveSqlParser.DropTableContext context = new HiveSqlParser.DropTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowColumns()
    {
        // Setup
        final HiveSqlParser.ShowColumnsContext context = new HiveSqlParser.ShowColumnsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowColumns(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDescribeTable()
    {
        // Setup
        final HiveSqlParser.DescribeTableContext context = new HiveSqlParser.DescribeTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDescribeTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAddReplaceColumn()
    {
        // Setup
        final HiveSqlParser.AddReplaceColumnContext context = new HiveSqlParser.AddReplaceColumnContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAddReplaceColumn(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInsertInto()
    {
        // Setup
        final HiveSqlParser.InsertIntoContext context = new HiveSqlParser.InsertIntoContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInsertInto(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInsertOverwrite()
    {
        // Setup
        final HiveSqlParser.InsertOverwriteContext context = new HiveSqlParser.InsertOverwriteContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInsertOverwrite(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInsertFilesystem()
    {
        // Setup
        final HiveSqlParser.InsertFilesystemContext context = new HiveSqlParser.InsertFilesystemContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInsertFilesystem(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUpdateTable()
    {
        // Setup
        final HiveSqlParser.UpdateTableContext context = new HiveSqlParser.UpdateTableContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitUpdateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowFunctions()
    {
        // Setup
        final HiveSqlParser.ShowFunctionsContext context = new HiveSqlParser.ShowFunctionsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowFunctions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateRole()
    {
        // Setup
        final HiveSqlParser.CreateRoleContext context = new HiveSqlParser.CreateRoleContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCreateRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropRole()
    {
        // Setup
        final HiveSqlParser.DropRoleContext context = new HiveSqlParser.DropRoleContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDropRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGrantRoles()
    {
        // Setup
        final HiveSqlParser.GrantRolesContext context = new HiveSqlParser.GrantRolesContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitGrantRoles(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRevokeRoles()
    {
        // Setup
        final HiveSqlParser.RevokeRolesContext context = new HiveSqlParser.RevokeRolesContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitRevokeRoles(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSetRole()
    {
        // Setup
        final HiveSqlParser.SetRoleContext context = new HiveSqlParser.SetRoleContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSetRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowRoles()
    {
        // Setup
        final HiveSqlParser.ShowRolesContext context = new HiveSqlParser.ShowRolesContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowRoles(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGrant()
    {
        // Setup
        final HiveSqlParser.GrantContext context = new HiveSqlParser.GrantContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitGrant(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRevoke()
    {
        // Setup
        final HiveSqlParser.RevokeContext context = new HiveSqlParser.RevokeContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitRevoke(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowGrants()
    {
        // Setup
        final HiveSqlParser.ShowGrantsContext context = new HiveSqlParser.ShowGrantsContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitShowGrants(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDelete()
    {
        // Setup
        final HiveSqlParser.DeleteContext context = new HiveSqlParser.DeleteContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDelete(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExplain()
    {
        // Setup
        final HiveSqlParser.ExplainContext context = new HiveSqlParser.ExplainContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitExplain(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSetSession()
    {
        // Setup
        final HiveSqlParser.SetSessionContext context = new HiveSqlParser.SetSessionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSetSession(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitResetSession()
    {
        // Setup
        final HiveSqlParser.ResetSessionContext context = new HiveSqlParser.ResetSessionContext(
                new HiveSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitResetSession(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAssignmentItem()
    {
        // Setup
        final HiveSqlParser.AssignmentItemContext context = new HiveSqlParser.AssignmentItemContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAssignmentItem(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitProperty()
    {
        // Setup
        final HiveSqlParser.PropertyContext context = new HiveSqlParser.PropertyContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitProperty(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuery()
    {
        // Setup
        final HiveSqlParser.QueryContext context = new HiveSqlParser.QueryContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitQuery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitWith()
    {
        // Setup
        final HiveSqlParser.WithContext context = new HiveSqlParser.WithContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitWith(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNamedQuery()
    {
        // Setup
        final HiveSqlParser.NamedQueryContext context = new HiveSqlParser.NamedQueryContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitNamedQuery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQueryNoWith()

    {
        // Setup
        final HiveSqlParser.QueryNoWithContext context = new HiveSqlParser.QueryNoWithContext(new ParserRuleContext(),
                0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitQueryNoWith(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuerySpecification()
    {
        // Setup
        final HiveSqlParser.QuerySpecificationContext context = new HiveSqlParser.QuerySpecificationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitQuerySpecification(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGroupBy()
    {
        // Setup
        final HiveSqlParser.GroupByContext context = new HiveSqlParser.GroupByContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitGroupBy(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSingleGroupingSet()
    {
        // Setup
        final HiveSqlParser.SingleGroupingSetContext context = new HiveSqlParser.SingleGroupingSetContext(
                new HiveSqlParser.GroupingElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSingleGroupingSet(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRollup()
    {
        // Setup
        final HiveSqlParser.RollupContext context = new HiveSqlParser.RollupContext(
                new HiveSqlParser.GroupingElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitRollup(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCube()
    {
        // Setup
        final HiveSqlParser.CubeContext context = new HiveSqlParser.CubeContext(
                new HiveSqlParser.GroupingElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCube(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitMultipleGroupingSets()
    {
        // Setup
        final HiveSqlParser.MultipleGroupingSetsContext context = new HiveSqlParser.MultipleGroupingSetsContext(
                new HiveSqlParser.GroupingElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitMultipleGroupingSets(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSetOperation()
    {
        // Setup
        final HiveSqlParser.SetOperationContext context = new HiveSqlParser.SetOperationContext(
                new HiveSqlParser.QueryTermContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSetOperation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSelectAll()
    {
        // Setup
        final HiveSqlParser.SelectAllContext context = new HiveSqlParser.SelectAllContext(
                new HiveSqlParser.SelectItemContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSelectAll(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSelectSingle()
    {
        // Setup
        final HiveSqlParser.SelectSingleContext context = new HiveSqlParser.SelectSingleContext(
                new HiveSqlParser.SelectItemContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSelectSingle(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTable()
    {
        // Setup
        final HiveSqlParser.TableContext context = new HiveSqlParser.TableContext(
                new HiveSqlParser.QueryPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubquery()
    {
        // Setup
        final HiveSqlParser.SubqueryContext context = new HiveSqlParser.SubqueryContext(
                new HiveSqlParser.QueryPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSubquery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInlineTable()
    {
        // Setup
        final HiveSqlParser.InlineTableContext context = new HiveSqlParser.InlineTableContext(
                new HiveSqlParser.QueryPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInlineTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLogicalNot()
    {
        // Setup
        final HiveSqlParser.LogicalNotContext context = new HiveSqlParser.LogicalNotContext(
                new HiveSqlParser.BooleanExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitLogicalNot(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLogicalBinary()
    {
        // Setup
        final HiveSqlParser.LogicalBinaryContext context = new HiveSqlParser.LogicalBinaryContext(
                new HiveSqlParser.BooleanExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitLogicalBinary(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitJoinRelation()
    {
        // Setup
        final HiveSqlParser.JoinRelationContext context = new HiveSqlParser.JoinRelationContext(
                new HiveSqlParser.RelationContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitJoinRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSampledRelation()
    {
        // Setup
        final HiveSqlParser.SampledRelationContext context = new HiveSqlParser.SampledRelationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSampledRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAliasedRelation()
    {
        // Setup
        final HiveSqlParser.AliasedRelationContext context = new HiveSqlParser.AliasedRelationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitAliasedRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTableName()
    {
        // Setup
        final HiveSqlParser.TableNameContext context = new HiveSqlParser.TableNameContext(
                new HiveSqlParser.RelationPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitTableName(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubqueryRelation()
    {
        // Setup
        final HiveSqlParser.SubqueryRelationContext context = new HiveSqlParser.SubqueryRelationContext(
                new HiveSqlParser.RelationPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSubqueryRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitPredicated()
    {
        // Setup
        final HiveSqlParser.PredicatedContext context = new HiveSqlParser.PredicatedContext(
                new HiveSqlParser.BooleanExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitPredicated(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExpressionPredicated()
    {
        // Setup
        final HiveSqlParser.ExpressionPredicatedContext context = new HiveSqlParser.ExpressionPredicatedContext(
                new HiveSqlParser.BooleanExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitExpressionPredicated(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitComparison()
    {
        // Setup
        final HiveSqlParser.ComparisonContext context = new HiveSqlParser.ComparisonContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitComparison(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDistinctFrom()
    {
        // Setup
        final HiveSqlParser.DistinctFromContext context = new HiveSqlParser.DistinctFromContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDistinctFrom(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBetween()
    {
        // Setup
        final HiveSqlParser.BetweenContext context = new HiveSqlParser.BetweenContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitBetween(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNullPredicate()
    {
        // Setup
        final HiveSqlParser.NullPredicateContext context = new HiveSqlParser.NullPredicateContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitNullPredicate(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLike()
    {
        // Setup
        final HiveSqlParser.LikeContext context = new HiveSqlParser.LikeContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitLike(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRlike()
    {
        // Setup
        final HiveSqlParser.RlikeContext context = new HiveSqlParser.RlikeContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitRlike(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRegexp()
    {
        // Setup
        final HiveSqlParser.RegexpContext context = new HiveSqlParser.RegexpContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitRegexp(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInList()
    {
        // Setup
        final HiveSqlParser.InListContext context = new HiveSqlParser.InListContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInList(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInSubquery()
    {
        // Setup
        final HiveSqlParser.InSubqueryContext context = new HiveSqlParser.InSubqueryContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInSubquery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExists()
    {
        // Setup
        final HiveSqlParser.ExistsContext context = new HiveSqlParser.ExistsContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitExists(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuantifiedComparison()
    {
        // Setup
        final HiveSqlParser.QuantifiedComparisonContext context = new HiveSqlParser.QuantifiedComparisonContext(
                new HiveSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitQuantifiedComparison(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitArithmeticUnary()
    {
        // Setup
        final HiveSqlParser.ArithmeticUnaryContext context = new HiveSqlParser.ArithmeticUnaryContext(
                new HiveSqlParser.ValueExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitArithmeticUnary(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitArithmeticBinary()
    {
        // Setup
        final HiveSqlParser.ArithmeticBinaryContext context = new HiveSqlParser.ArithmeticBinaryContext(
                new HiveSqlParser.ValueExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitArithmeticBinary(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitArithmeticBit()
    {
        // Setup
        final HiveSqlParser.ArithmeticBitContext context = new HiveSqlParser.ArithmeticBitContext(
                new HiveSqlParser.ValueExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitArithmeticBit(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitConcatenation()
    {
        // Setup
        final HiveSqlParser.ConcatenationContext context = new HiveSqlParser.ConcatenationContext(
                new HiveSqlParser.ValueExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitConcatenation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitParenthesizedExpression()
    {
        // Setup
        final HiveSqlParser.ParenthesizedExpressionContext context = new HiveSqlParser.ParenthesizedExpressionContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitParenthesizedExpression(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRowConstructor()
    {
        // Setup
        final HiveSqlParser.RowConstructorContext context = new HiveSqlParser.RowConstructorContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitRowConstructor(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitArrayConstructor()
    {
        // Setup
        final HiveSqlParser.ArrayConstructorContext context = new HiveSqlParser.ArrayConstructorContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitArrayConstructor(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCast()
    {
        // Setup
        final HiveSqlParser.CastContext context = new HiveSqlParser.CastContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCast(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSpecialDateTimeFunction()
    {
        // Setup
        final HiveSqlParser.SpecialDateTimeFunctionContext context = new HiveSqlParser.SpecialDateTimeFunctionContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSpecialDateTimeFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCurrentUser()
    {
        // Setup
        final HiveSqlParser.CurrentUserContext context = new HiveSqlParser.CurrentUserContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCurrentUser(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExtract()
    {
        // Setup
        final HiveSqlParser.ExtractContext context = new HiveSqlParser.ExtractContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitExtract(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubstring()
    {
        // Setup
        final HiveSqlParser.SubstringContext context = new HiveSqlParser.SubstringContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSubstring(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitPosition()
    {
        // Setup
        final HiveSqlParser.PositionContext context = new HiveSqlParser.PositionContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitPosition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNormalize()
    {
        // Setup
        final HiveSqlParser.NormalizeContext context = new HiveSqlParser.NormalizeContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitNormalize(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubscript()
    {
        // Setup
        final HiveSqlParser.SubscriptContext context = new HiveSqlParser.SubscriptContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSubscript(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubqueryExpression()
    {
        // Setup
        final HiveSqlParser.SubqueryExpressionContext context = new HiveSqlParser.SubqueryExpressionContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSubqueryExpression(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDereference()
    {
        // Setup
        final HiveSqlParser.DereferenceContext context = new HiveSqlParser.DereferenceContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDereference(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitColumnReference()
    {
        // Setup
        final HiveSqlParser.ColumnReferenceContext context = new HiveSqlParser.ColumnReferenceContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitColumnReference(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSimpleCase()
    {
        // Setup
        final HiveSqlParser.SimpleCaseContext context = new HiveSqlParser.SimpleCaseContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSimpleCase(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSearchedCase()
    {
        // Setup
        final HiveSqlParser.SearchedCaseContext context = new HiveSqlParser.SearchedCaseContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSearchedCase(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitWhenClause()
    {
        // Setup
        final HiveSqlParser.WhenClauseContext context = new HiveSqlParser.WhenClauseContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitWhenClause(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitFunctionCall()
    {
        // Setup
        final HiveSqlParser.FunctionCallContext context = new HiveSqlParser.FunctionCallContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitFunctionCall(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitOver()
    {
        // Setup
        final HiveSqlParser.OverContext context = new HiveSqlParser.OverContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitOver(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitColumnDefinition()
    {
        // Setup
        final HiveSqlParser.ColumnDefinitionContext context = new HiveSqlParser.ColumnDefinitionContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitColumnDefinition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSortItem()
    {
        // Setup
        final HiveSqlParser.SortItemContext context = new HiveSqlParser.SortItemContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitSortItem(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitWindowFrame()
    {
        // Setup
        final HiveSqlParser.WindowFrameContext context = new HiveSqlParser.WindowFrameContext(new ParserRuleContext(),
                0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitWindowFrame(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUnboundedFrame()
    {
        // Setup
        final HiveSqlParser.UnboundedFrameContext context = new HiveSqlParser.UnboundedFrameContext(
                new HiveSqlParser.FrameBoundContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitUnboundedFrame(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBoundedFrame()
    {
        // Setup
        final HiveSqlParser.BoundedFrameContext context = new HiveSqlParser.BoundedFrameContext(
                new HiveSqlParser.FrameBoundContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitBoundedFrame(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCurrentRowBound()
    {
        // Setup
        final HiveSqlParser.CurrentRowBoundContext context = new HiveSqlParser.CurrentRowBoundContext(
                new HiveSqlParser.FrameBoundContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitCurrentRowBound(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGroupingOperation()
    {
        // Setup
        final HiveSqlParser.GroupingOperationContext context = new HiveSqlParser.GroupingOperationContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitGroupingOperation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUnquotedIdentifier()
    {
        // Setup
        final HiveSqlParser.UnquotedIdentifierContext context = new HiveSqlParser.UnquotedIdentifierContext(
                new HiveSqlParser.IdentifierContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitUnquotedIdentifier(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuotedIdentifier()
    {
        // Setup
        final HiveSqlParser.QuotedIdentifierContext context = new HiveSqlParser.QuotedIdentifierContext(
                new HiveSqlParser.IdentifierContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitQuotedIdentifier(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBackQuotedIdentifier()
    {
        // Setup
        final HiveSqlParser.BackQuotedIdentifierContext context = new HiveSqlParser.BackQuotedIdentifierContext(
                new HiveSqlParser.IdentifierContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitBackQuotedIdentifier(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDigitIdentifier()
    {
        // Setup
        final HiveSqlParser.DigitIdentifierContext context = new HiveSqlParser.DigitIdentifierContext(
                new HiveSqlParser.IdentifierContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDigitIdentifier(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNullLiteral()
    {
        // Setup
        final HiveSqlParser.NullLiteralContext context = new HiveSqlParser.NullLiteralContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitNullLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBasicStringLiteral()
    {
        // Setup
        final HiveSqlParser.BasicStringLiteralContext context = new HiveSqlParser.BasicStringLiteralContext(
                new HiveSqlParser.StringContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitBasicStringLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBinaryLiteral()
    {
        // Setup
        final HiveSqlParser.BinaryLiteralContext context = new HiveSqlParser.BinaryLiteralContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitBinaryLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTypeConstructor()
    {
        // Setup
        final HiveSqlParser.TypeConstructorContext context = new HiveSqlParser.TypeConstructorContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitTypeConstructor(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitIntegerLiteral()
    {
        // Setup
        final HiveSqlParser.IntegerLiteralContext context = new HiveSqlParser.IntegerLiteralContext(
                new HiveSqlParser.NumberContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitIntegerLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDecimalLiteral()
    {
        // Setup
        final HiveSqlParser.DecimalLiteralContext context = new HiveSqlParser.DecimalLiteralContext(
                new HiveSqlParser.NumberContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;
        when(mockParsingOptions.getDecimalLiteralTreatment()).thenReturn(
                ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE);

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDecimalLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDoubleLiteral()
    {
        // Setup
        final HiveSqlParser.DoubleLiteralContext context = new HiveSqlParser.DoubleLiteralContext(
                new HiveSqlParser.NumberContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitDoubleLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBooleanValue()
    {
        // Setup
        final HiveSqlParser.BooleanValueContext context = new HiveSqlParser.BooleanValueContext(new ParserRuleContext(),
                0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitBooleanValue(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInterval()
    {
        // Setup
        final HiveSqlParser.IntervalContext context = new HiveSqlParser.IntervalContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitInterval(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitParameter()
    {
        // Setup
        final HiveSqlParser.ParameterContext context = new HiveSqlParser.ParameterContext(
                new HiveSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitParameter(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQualifiedArgument()
    {
        // Setup
        final HiveSqlParser.QualifiedArgumentContext context = new HiveSqlParser.QualifiedArgumentContext(
                new HiveSqlParser.PathElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitQualifiedArgument(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUnqualifiedArgument()
    {
        // Setup
        final HiveSqlParser.UnqualifiedArgumentContext context = new HiveSqlParser.UnqualifiedArgumentContext(
                new HiveSqlParser.PathElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitUnqualifiedArgument(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitPathSpecification()
    {
        // Setup
        final HiveSqlParser.PathSpecificationContext context = new HiveSqlParser.PathSpecificationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.visitPathSpecification(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDefaultResult()
    {
        // Setup
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.defaultResult();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAggregateResult()
    {
        // Setup
        final Node aggregate = null;
        final Node nextResult = null;
        final Node expectedResult = null;

        // Run the test
        final Node result = hiveAstBuilderUnderTest.aggregateResult(aggregate, nextResult);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetLocation1()
    {
        // Setup
        final TerminalNode terminalNode = null;

        // Run the test
        final NodeLocation result = HiveAstBuilder.getLocation(terminalNode);

        // Verify the results
    }

    @Test
    public void testGetLocation2()
    {
        // Setup
        final ParserRuleContext parserRuleContext = new ParserRuleContext();

        // Run the test
        final NodeLocation result = HiveAstBuilder.getLocation(parserRuleContext);

        // Verify the results
    }

    @Test
    public void testGetLocation3()
    {
        // Setup
        final Token token = null;

        // Run the test
        final NodeLocation result = HiveAstBuilder.getLocation(token);

        // Verify the results
    }
}
