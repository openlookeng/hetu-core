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

import io.hetu.core.migration.source.impala.ImpalaSqlParser;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.tree.Node;
import org.antlr.v4.runtime.ParserRuleContext;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ImpalaAstBuilderTest
{
    @Mock
    private ParsingOptions mockParsingOptions;

    private ImpalaAstBuilder impalaAstBuilderUnderTest;

    @BeforeMethod
    public void setUp()
    {
        initMocks(this);
        impalaAstBuilderUnderTest = new ImpalaAstBuilder(mockParsingOptions);
    }

    @Test
    public void testAddDiff1()
    {
        // Setup
        // Run the test
        impalaAstBuilderUnderTest.addDiff(DiffType.DELETED, "source", "target", "message");

        // Verify the results
    }

    @Test
    public void testAddDiff2()
    {
        // Setup
        // Run the test
        impalaAstBuilderUnderTest.addDiff(DiffType.DELETED, "source", "message");

        // Verify the results
    }

    @Test
    public void testVisitSingleStatement()
    {
        // Setup
        final ImpalaSqlParser.SingleStatementContext context = new ImpalaSqlParser.SingleStatementContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSingleStatement(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitStandaloneExpression()
    {
        // Setup
        final ImpalaSqlParser.StandaloneExpressionContext context = new ImpalaSqlParser.StandaloneExpressionContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitStandaloneExpression(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitStandalonePathSpecification()
    {
        // Setup
        final ImpalaSqlParser.StandalonePathSpecificationContext context = new ImpalaSqlParser.StandalonePathSpecificationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitStandalonePathSpecification(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitStatementDefault()
    {
        // Setup
        final ImpalaSqlParser.StatementDefaultContext ctx = new ImpalaSqlParser.StatementDefaultContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitStatementDefault(ctx);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUse()
    {
        // Setup
        final ImpalaSqlParser.UseContext context = new ImpalaSqlParser.UseContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitUse(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateSchema()
    {
        // Setup
        final ImpalaSqlParser.CreateSchemaContext context = new ImpalaSqlParser.CreateSchemaContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateSchema(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterSchema()
    {
        // Setup
        final ImpalaSqlParser.AlterSchemaContext context = new ImpalaSqlParser.AlterSchemaContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAlterSchema(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropSchema()
    {
        // Setup
        final ImpalaSqlParser.DropSchemaContext context = new ImpalaSqlParser.DropSchemaContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropSchema(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateTable()
    {
        // Setup
        final ImpalaSqlParser.CreateTableContext context = new ImpalaSqlParser.CreateTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateTableLike()
    {
        // Setup
        final ImpalaSqlParser.CreateTableLikeContext context = new ImpalaSqlParser.CreateTableLikeContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateTableLike(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateKuduTable()
    {
        // Setup
        final ImpalaSqlParser.CreateKuduTableContext context = new ImpalaSqlParser.CreateKuduTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateKuduTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateKuduTableAsSelect()
    {
        // Setup
        final ImpalaSqlParser.CreateKuduTableAsSelectContext context = new ImpalaSqlParser.CreateKuduTableAsSelectContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateKuduTableAsSelect(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRenameTable()
    {
        // Setup
        final ImpalaSqlParser.RenameTableContext context = new ImpalaSqlParser.RenameTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRenameTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAddColumns()
    {
        // Setup
        final ImpalaSqlParser.AddColumnsContext context = new ImpalaSqlParser.AddColumnsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAddColumns(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitReplaceColumns()
    {
        // Setup
        final ImpalaSqlParser.ReplaceColumnsContext context = new ImpalaSqlParser.ReplaceColumnsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitReplaceColumns(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAddSingleColumn()
    {
        // Setup
        final ImpalaSqlParser.AddSingleColumnContext context = new ImpalaSqlParser.AddSingleColumnContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAddSingleColumn(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropSingleColumn()
    {
        // Setup
        final ImpalaSqlParser.DropSingleColumnContext context = new ImpalaSqlParser.DropSingleColumnContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropSingleColumn(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableOwner()
    {
        // Setup
        final ImpalaSqlParser.AlterTableOwnerContext context = new ImpalaSqlParser.AlterTableOwnerContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAlterTableOwner(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterTableKuduOnly()
    {
        // Setup
        final ImpalaSqlParser.AlterTableKuduOnlyContext context = new ImpalaSqlParser.AlterTableKuduOnlyContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAlterTableKuduOnly(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropTable()
    {
        // Setup
        final ImpalaSqlParser.DropTableContext context = new ImpalaSqlParser.DropTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTruncateTable()
    {
        // Setup
        final ImpalaSqlParser.TruncateTableContext context = new ImpalaSqlParser.TruncateTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitTruncateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateView()
    {
        // Setup
        final ImpalaSqlParser.CreateViewContext context = new ImpalaSqlParser.CreateViewContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterView()
    {
        // Setup
        final ImpalaSqlParser.AlterViewContext context = new ImpalaSqlParser.AlterViewContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAlterView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRenameView()
    {
        // Setup
        final ImpalaSqlParser.RenameViewContext context = new ImpalaSqlParser.RenameViewContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRenameView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAlterViewOwner()
    {
        // Setup
        final ImpalaSqlParser.AlterViewOwnerContext context = new ImpalaSqlParser.AlterViewOwnerContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAlterViewOwner(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropView()
    {
        // Setup
        final ImpalaSqlParser.DropViewContext context = new ImpalaSqlParser.DropViewContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDescribeDbOrTable()
    {
        // Setup
        final ImpalaSqlParser.DescribeDbOrTableContext context = new ImpalaSqlParser.DescribeDbOrTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDescribeDbOrTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitComputeStats()
    {
        // Setup
        final ImpalaSqlParser.ComputeStatsContext context = new ImpalaSqlParser.ComputeStatsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitComputeStats(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitComputeIncrementalStats()
    {
        // Setup
        final ImpalaSqlParser.ComputeIncrementalStatsContext context = new ImpalaSqlParser.ComputeIncrementalStatsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitComputeIncrementalStats(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropStats()
    {
        // Setup
        final ImpalaSqlParser.DropStatsContext context = new ImpalaSqlParser.DropStatsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropStats(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropIncrementalStats()
    {
        // Setup
        final ImpalaSqlParser.DropIncrementalStatsContext context = new ImpalaSqlParser.DropIncrementalStatsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropIncrementalStats(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateFunction()
    {
        // Setup
        final ImpalaSqlParser.CreateFunctionContext context = new ImpalaSqlParser.CreateFunctionContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRefreshFunction()
    {
        // Setup
        final ImpalaSqlParser.RefreshFunctionContext context = new ImpalaSqlParser.RefreshFunctionContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRefreshFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropFunction()
    {
        // Setup
        final ImpalaSqlParser.DropFunctionContext context = new ImpalaSqlParser.DropFunctionContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCreateRole()
    {
        // Setup
        final ImpalaSqlParser.CreateRoleContext context = new ImpalaSqlParser.CreateRoleContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCreateRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDropRole()
    {
        // Setup
        final ImpalaSqlParser.DropRoleContext context = new ImpalaSqlParser.DropRoleContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDropRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGrantRole()
    {
        // Setup
        final ImpalaSqlParser.GrantRoleContext context = new ImpalaSqlParser.GrantRoleContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitGrantRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGrant()
    {
        // Setup
        final ImpalaSqlParser.GrantContext context = new ImpalaSqlParser.GrantContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitGrant(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRevokeRole()
    {
        // Setup
        final ImpalaSqlParser.RevokeRoleContext context = new ImpalaSqlParser.RevokeRoleContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRevokeRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRevoke()
    {
        // Setup
        final ImpalaSqlParser.RevokeContext context = new ImpalaSqlParser.RevokeContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRevoke(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInsertInto()
    {
        // Setup
        final ImpalaSqlParser.InsertIntoContext context = new ImpalaSqlParser.InsertIntoContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitInsertInto(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDelete()
    {
        // Setup
        final ImpalaSqlParser.DeleteContext context = new ImpalaSqlParser.DeleteContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDelete(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDeleteTableRef()
    {
        // Setup
        final ImpalaSqlParser.DeleteTableRefContext context = new ImpalaSqlParser.DeleteTableRefContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDeleteTableRef(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUpdateTable()
    {
        // Setup
        final ImpalaSqlParser.UpdateTableContext context = new ImpalaSqlParser.UpdateTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitUpdateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUpsert()
    {
        // Setup
        final ImpalaSqlParser.UpsertContext context = new ImpalaSqlParser.UpsertContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitUpsert(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowSchemas()
    {
        // Setup
        final ImpalaSqlParser.ShowSchemasContext context = new ImpalaSqlParser.ShowSchemasContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowSchemas(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowTables()
    {
        // Setup
        final ImpalaSqlParser.ShowTablesContext context = new ImpalaSqlParser.ShowTablesContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowTables(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowFunctions()
    {
        // Setup
        final ImpalaSqlParser.ShowFunctionsContext context = new ImpalaSqlParser.ShowFunctionsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowFunctions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowCreateTable()
    {
        // Setup
        final ImpalaSqlParser.ShowCreateTableContext context = new ImpalaSqlParser.ShowCreateTableContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowCreateTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowCreateView()
    {
        // Setup
        final ImpalaSqlParser.ShowCreateViewContext context = new ImpalaSqlParser.ShowCreateViewContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowCreateView(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowTableStats()
    {
        // Setup
        final ImpalaSqlParser.ShowTableStatsContext context = new ImpalaSqlParser.ShowTableStatsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowTableStats(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowColumnStats()
    {
        // Setup
        final ImpalaSqlParser.ShowColumnStatsContext context = new ImpalaSqlParser.ShowColumnStatsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowColumnStats(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowPartitions()
    {
        // Setup
        final ImpalaSqlParser.ShowPartitionsContext context = new ImpalaSqlParser.ShowPartitionsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowPartitions(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowFiles()
    {
        // Setup
        final ImpalaSqlParser.ShowFilesContext context = new ImpalaSqlParser.ShowFilesContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowFiles(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowRoles()
    {
        // Setup
        final ImpalaSqlParser.ShowRolesContext context = new ImpalaSqlParser.ShowRolesContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowRoles(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowRoleGrant()
    {
        // Setup
        final ImpalaSqlParser.ShowRoleGrantContext context = new ImpalaSqlParser.ShowRoleGrantContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowRoleGrant(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowGrantRole()
    {
        // Setup
        final ImpalaSqlParser.ShowGrantRoleContext context = new ImpalaSqlParser.ShowGrantRoleContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowGrantRole(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShowGrantUser()
    {
        // Setup
        final ImpalaSqlParser.ShowGrantUserContext context = new ImpalaSqlParser.ShowGrantUserContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShowGrantUser(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAddComments()
    {
        // Setup
        final ImpalaSqlParser.AddCommentsContext context = new ImpalaSqlParser.AddCommentsContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAddComments(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExplain()
    {
        // Setup
        final ImpalaSqlParser.ExplainContext context = new ImpalaSqlParser.ExplainContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitExplain(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSetSession()
    {
        // Setup
        final ImpalaSqlParser.SetSessionContext context = new ImpalaSqlParser.SetSessionContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSetSession(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitShutdown()
    {
        // Setup
        final ImpalaSqlParser.ShutdownContext context = new ImpalaSqlParser.ShutdownContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitShutdown(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInvalidateMeta()
    {
        // Setup
        final ImpalaSqlParser.InvalidateMetaContext context = new ImpalaSqlParser.InvalidateMetaContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitInvalidateMeta(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLoadData()
    {
        // Setup
        final ImpalaSqlParser.LoadDataContext context = new ImpalaSqlParser.LoadDataContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitLoadData(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRefreshMeta()
    {
        // Setup
        final ImpalaSqlParser.RefreshMetaContext context = new ImpalaSqlParser.RefreshMetaContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRefreshMeta(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRefreshAuth()
    {
        // Setup
        final ImpalaSqlParser.RefreshAuthContext context = new ImpalaSqlParser.RefreshAuthContext(
                new ImpalaSqlParser.StatementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRefreshAuth(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAssignmentItem()
    {
        // Setup
        final ImpalaSqlParser.AssignmentItemContext context = new ImpalaSqlParser.AssignmentItemContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAssignmentItem(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuery()
    {
        // Setup
        final ImpalaSqlParser.QueryContext context = new ImpalaSqlParser.QueryContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitQuery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitWith()
    {
        // Setup
        final ImpalaSqlParser.WithContext context = new ImpalaSqlParser.WithContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitWith(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitColumnDefinition()
    {
        // Setup
        final ImpalaSqlParser.ColumnDefinitionContext context = new ImpalaSqlParser.ColumnDefinitionContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitColumnDefinition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitColumnSpecWithKudu()
    {
        // Setup
        final ImpalaSqlParser.ColumnSpecWithKuduContext context = new ImpalaSqlParser.ColumnSpecWithKuduContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitColumnSpecWithKudu(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitKuduAttributes()
    {
        // Setup
        final ImpalaSqlParser.KuduAttributesContext context = new ImpalaSqlParser.KuduAttributesContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitKuduAttributes(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitHintClause()
    {
        // Setup
        final ImpalaSqlParser.HintClauseContext ctx = new ImpalaSqlParser.HintClauseContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitHintClause(ctx);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitProperties()
    {
        // Setup
        final ImpalaSqlParser.PropertiesContext ctx = new ImpalaSqlParser.PropertiesContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitProperties(ctx);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitPartitionedBy()
    {
        // Setup
        final ImpalaSqlParser.PartitionedByContext ctx = new ImpalaSqlParser.PartitionedByContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitPartitionedBy(ctx);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSortedBy()
    {
        // Setup
        final ImpalaSqlParser.SortedByContext ctx = new ImpalaSqlParser.SortedByContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSortedBy(ctx);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRowFormat()
    {
        // Setup
        final ImpalaSqlParser.RowFormatContext context = new ImpalaSqlParser.RowFormatContext(new ParserRuleContext(),
                0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRowFormat(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitProperty()
    {
        // Setup
        final ImpalaSqlParser.PropertyContext context = new ImpalaSqlParser.PropertyContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitProperty(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQueryNoWith()
    {
        // Setup
        final ImpalaSqlParser.QueryNoWithContext context = new ImpalaSqlParser.QueryNoWithContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitQueryNoWith(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSetOperation()
    {
        // Setup
        final ImpalaSqlParser.SetOperationContext context = new ImpalaSqlParser.SetOperationContext(
                new ImpalaSqlParser.QueryTermContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSetOperation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTable()
    {
        // Setup
        final ImpalaSqlParser.TableContext context = new ImpalaSqlParser.TableContext(
                new ImpalaSqlParser.QueryPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInlineTable()
    {
        // Setup
        final ImpalaSqlParser.InlineTableContext context = new ImpalaSqlParser.InlineTableContext(
                new ImpalaSqlParser.QueryPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitInlineTable(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubquery()
    {
        // Setup
        final ImpalaSqlParser.SubqueryContext context = new ImpalaSqlParser.SubqueryContext(
                new ImpalaSqlParser.QueryPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSubquery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSortItem()
    {
        // Setup
        final ImpalaSqlParser.SortItemContext context = new ImpalaSqlParser.SortItemContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSortItem(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuerySpecification()
    {
        // Setup
        final ImpalaSqlParser.QuerySpecificationContext context = new ImpalaSqlParser.QuerySpecificationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitQuerySpecification(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGroupBy()
    {
        // Setup
        final ImpalaSqlParser.GroupByContext context = new ImpalaSqlParser.GroupByContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitGroupBy(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSingleGroupingSet()
    {
        // Setup
        final ImpalaSqlParser.SingleGroupingSetContext context = new ImpalaSqlParser.SingleGroupingSetContext(
                new ImpalaSqlParser.GroupingElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSingleGroupingSet(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNamedQuery()
    {
        // Setup
        final ImpalaSqlParser.NamedQueryContext context = new ImpalaSqlParser.NamedQueryContext(new ParserRuleContext(),
                0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitNamedQuery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSelectSingle()
    {
        // Setup
        final ImpalaSqlParser.SelectSingleContext context = new ImpalaSqlParser.SelectSingleContext(
                new ImpalaSqlParser.SelectItemContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSelectSingle(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSelectAll()
    {
        // Setup
        final ImpalaSqlParser.SelectAllContext context = new ImpalaSqlParser.SelectAllContext(
                new ImpalaSqlParser.SelectItemContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSelectAll(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitJoinRelation()
    {
        // Setup
        final ImpalaSqlParser.JoinRelationContext context = new ImpalaSqlParser.JoinRelationContext(
                new ImpalaSqlParser.RelationContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitJoinRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSampledRelation()
    {
        // Setup
        final ImpalaSqlParser.SampledRelationContext context = new ImpalaSqlParser.SampledRelationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSampledRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitAliasedRelation()
    {
        // Setup
        final ImpalaSqlParser.AliasedRelationContext context = new ImpalaSqlParser.AliasedRelationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitAliasedRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTableName()
    {
        // Setup
        final ImpalaSqlParser.TableNameContext context = new ImpalaSqlParser.TableNameContext(
                new ImpalaSqlParser.RelationPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitTableName(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubqueryRelation()
    {
        // Setup
        final ImpalaSqlParser.SubqueryRelationContext context = new ImpalaSqlParser.SubqueryRelationContext(
                new ImpalaSqlParser.RelationPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSubqueryRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUnnest()
    {
        // Setup
        final ImpalaSqlParser.UnnestContext context = new ImpalaSqlParser.UnnestContext(
                new ImpalaSqlParser.RelationPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitUnnest(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLateral()
    {
        // Setup
        final ImpalaSqlParser.LateralContext context = new ImpalaSqlParser.LateralContext(
                new ImpalaSqlParser.RelationPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitLateral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitParenthesizedRelation()
    {
        // Setup
        final ImpalaSqlParser.ParenthesizedRelationContext context = new ImpalaSqlParser.ParenthesizedRelationContext(
                new ImpalaSqlParser.RelationPrimaryContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitParenthesizedRelation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLogicalNot()
    {
        // Setup
        final ImpalaSqlParser.LogicalNotContext context = new ImpalaSqlParser.LogicalNotContext(
                new ImpalaSqlParser.BooleanExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitLogicalNot(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitPredicated()
    {
        // Setup
        final ImpalaSqlParser.PredicatedContext context = new ImpalaSqlParser.PredicatedContext(
                new ImpalaSqlParser.BooleanExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitPredicated(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLogicalBinary()
    {
        // Setup
        final ImpalaSqlParser.LogicalBinaryContext context = new ImpalaSqlParser.LogicalBinaryContext(
                new ImpalaSqlParser.BooleanExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitLogicalBinary(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitComparison()
    {
        // Setup
        final ImpalaSqlParser.ComparisonContext context = new ImpalaSqlParser.ComparisonContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitComparison(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuantifiedComparison()
    {
        // Setup
        final ImpalaSqlParser.QuantifiedComparisonContext context = new ImpalaSqlParser.QuantifiedComparisonContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitQuantifiedComparison(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBetween()
    {
        // Setup
        final ImpalaSqlParser.BetweenContext context = new ImpalaSqlParser.BetweenContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitBetween(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInList()
    {
        // Setup
        final ImpalaSqlParser.InListContext context = new ImpalaSqlParser.InListContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitInList(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInSubquery()
    {
        // Setup
        final ImpalaSqlParser.InSubqueryContext context = new ImpalaSqlParser.InSubqueryContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitInSubquery(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLike()
    {
        // Setup
        final ImpalaSqlParser.LikeContext context = new ImpalaSqlParser.LikeContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitLike(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNullPredicate()
    {
        // Setup
        final ImpalaSqlParser.NullPredicateContext context = new ImpalaSqlParser.NullPredicateContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitNullPredicate(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDistinctFrom()
    {
        // Setup
        final ImpalaSqlParser.DistinctFromContext context = new ImpalaSqlParser.DistinctFromContext(
                new ImpalaSqlParser.PredicateContext(new ParserRuleContext(), 0, new ParserRuleContext()));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDistinctFrom(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitConcatenation()
    {
        // Setup
        final ImpalaSqlParser.ConcatenationContext context = new ImpalaSqlParser.ConcatenationContext(
                new ImpalaSqlParser.ValueExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitConcatenation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitArithmeticBinary()
    {
        // Setup
        final ImpalaSqlParser.ArithmeticBinaryContext context = new ImpalaSqlParser.ArithmeticBinaryContext(
                new ImpalaSqlParser.ValueExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitArithmeticBinary(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitArithmeticUnary()
    {
        // Setup
        final ImpalaSqlParser.ArithmeticUnaryContext context = new ImpalaSqlParser.ArithmeticUnaryContext(
                new ImpalaSqlParser.ValueExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitArithmeticUnary(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDereference()
    {
        // Setup
        final ImpalaSqlParser.DereferenceContext context = new ImpalaSqlParser.DereferenceContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDereference(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitTypeConstructor()
    {
        // Setup
        final ImpalaSqlParser.TypeConstructorContext context = new ImpalaSqlParser.TypeConstructorContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitTypeConstructor(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSpecialDateTimeFunction()
    {
        // Setup
        final ImpalaSqlParser.SpecialDateTimeFunctionContext context = new ImpalaSqlParser.SpecialDateTimeFunctionContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSpecialDateTimeFunction(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubstring()
    {
        // Setup
        final ImpalaSqlParser.SubstringContext context = new ImpalaSqlParser.SubstringContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSubstring(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCast()
    {
        // Setup
        final ImpalaSqlParser.CastContext context = new ImpalaSqlParser.CastContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCast(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitLambda()
    {
        // Setup
        final ImpalaSqlParser.LambdaContext context = new ImpalaSqlParser.LambdaContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitLambda(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitParenthesizedExpression()
    {
        // Setup
        final ImpalaSqlParser.ParenthesizedExpressionContext context = new ImpalaSqlParser.ParenthesizedExpressionContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitParenthesizedExpression(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitParameter()
    {
        // Setup
        final ImpalaSqlParser.ParameterContext context = new ImpalaSqlParser.ParameterContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitParameter(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNormalize()
    {
        // Setup
        final ImpalaSqlParser.NormalizeContext context = new ImpalaSqlParser.NormalizeContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitNormalize(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSimpleCase()
    {
        // Setup
        final ImpalaSqlParser.SimpleCaseContext context = new ImpalaSqlParser.SimpleCaseContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSimpleCase(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitColumnReference()
    {
        // Setup
        final ImpalaSqlParser.ColumnReferenceContext context = new ImpalaSqlParser.ColumnReferenceContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitColumnReference(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitNullLiteral()
    {
        // Setup
        final ImpalaSqlParser.NullLiteralContext context = new ImpalaSqlParser.NullLiteralContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitNullLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitRowConstructor()
    {
        // Setup
        final ImpalaSqlParser.RowConstructorContext context = new ImpalaSqlParser.RowConstructorContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitRowConstructor(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubscript()
    {
        // Setup
        final ImpalaSqlParser.SubscriptContext context = new ImpalaSqlParser.SubscriptContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSubscript(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSubqueryExpression()
    {
        // Setup
        final ImpalaSqlParser.SubqueryExpressionContext context = new ImpalaSqlParser.SubqueryExpressionContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSubqueryExpression(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBinaryLiteral()
    {
        // Setup
        final ImpalaSqlParser.BinaryLiteralContext context = new ImpalaSqlParser.BinaryLiteralContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitBinaryLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExtract()
    {
        // Setup
        final ImpalaSqlParser.ExtractContext context = new ImpalaSqlParser.ExtractContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitExtract(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitArrayConstructor()
    {
        // Setup
        final ImpalaSqlParser.ArrayConstructorContext context = new ImpalaSqlParser.ArrayConstructorContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitArrayConstructor(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitFunctionCall()
    {
        // Setup
        final ImpalaSqlParser.FunctionCallContext context = new ImpalaSqlParser.FunctionCallContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitFunctionCall(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitExists()
    {
        // Setup
        final ImpalaSqlParser.ExistsContext context = new ImpalaSqlParser.ExistsContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitExists(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitPosition()
    {
        // Setup
        final ImpalaSqlParser.PositionContext context = new ImpalaSqlParser.PositionContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitPosition(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitSearchedCase()
    {
        // Setup
        final ImpalaSqlParser.SearchedCaseContext context = new ImpalaSqlParser.SearchedCaseContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitSearchedCase(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitGroupingOperation()
    {
        // Setup
        final ImpalaSqlParser.GroupingOperationContext context = new ImpalaSqlParser.GroupingOperationContext(
                new ImpalaSqlParser.PrimaryExpressionContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitGroupingOperation(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBasicStringLiteral()
    {
        // Setup
        final ImpalaSqlParser.BasicStringLiteralContext context = new ImpalaSqlParser.BasicStringLiteralContext(
                new ImpalaSqlParser.StringContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitBasicStringLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBooleanValue()
    {
        // Setup
        final ImpalaSqlParser.BooleanValueContext context = new ImpalaSqlParser.BooleanValueContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitBooleanValue(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitInterval()
    {
        // Setup
        final ImpalaSqlParser.IntervalContext context = new ImpalaSqlParser.IntervalContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitInterval(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitWhenClause()
    {
        // Setup
        final ImpalaSqlParser.WhenClauseContext context = new ImpalaSqlParser.WhenClauseContext(new ParserRuleContext(),
                0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitWhenClause(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitFilter()
    {
        // Setup
        final ImpalaSqlParser.FilterContext context = new ImpalaSqlParser.FilterContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitFilter(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitOver()
    {
        // Setup
        final ImpalaSqlParser.OverContext context = new ImpalaSqlParser.OverContext(new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitOver(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitWindowFrame()
    {
        // Setup
        final ImpalaSqlParser.WindowFrameContext context = new ImpalaSqlParser.WindowFrameContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitWindowFrame(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUnboundedFrame()
    {
        // Setup
        final ImpalaSqlParser.UnboundedFrameContext context = new ImpalaSqlParser.UnboundedFrameContext(
                new ImpalaSqlParser.FrameBoundContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitUnboundedFrame(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitCurrentRowBound()
    {
        // Setup
        final ImpalaSqlParser.CurrentRowBoundContext context = new ImpalaSqlParser.CurrentRowBoundContext(
                new ImpalaSqlParser.FrameBoundContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitCurrentRowBound(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBoundedFrame()
    {
        // Setup
        final ImpalaSqlParser.BoundedFrameContext context = new ImpalaSqlParser.BoundedFrameContext(
                new ImpalaSqlParser.FrameBoundContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitBoundedFrame(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQualifiedArgument()
    {
        // Setup
        final ImpalaSqlParser.QualifiedArgumentContext context = new ImpalaSqlParser.QualifiedArgumentContext(
                new ImpalaSqlParser.PathElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitQualifiedArgument(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUnqualifiedArgument()
    {
        // Setup
        final ImpalaSqlParser.UnqualifiedArgumentContext context = new ImpalaSqlParser.UnqualifiedArgumentContext(
                new ImpalaSqlParser.PathElementContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitUnqualifiedArgument(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitPathSpecification()
    {
        // Setup
        final ImpalaSqlParser.PathSpecificationContext context = new ImpalaSqlParser.PathSpecificationContext(
                new ParserRuleContext(), 0);
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitPathSpecification(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitUnquotedIdentifier()
    {
        // Setup
        final ImpalaSqlParser.UnquotedIdentifierContext context = new ImpalaSqlParser.UnquotedIdentifierContext(
                new ImpalaSqlParser.IdentifierContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitUnquotedIdentifier(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitQuotedIdentifier()
    {
        // Setup
        final ImpalaSqlParser.QuotedIdentifierContext context = new ImpalaSqlParser.QuotedIdentifierContext(
                new ImpalaSqlParser.IdentifierContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitQuotedIdentifier(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitBackQuotedIdentifier()
    {
        // Setup
        final ImpalaSqlParser.BackQuotedIdentifierContext context = new ImpalaSqlParser.BackQuotedIdentifierContext(
                new ImpalaSqlParser.IdentifierContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitBackQuotedIdentifier(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDecimalLiteral()
    {
        // Setup
        final ImpalaSqlParser.DecimalLiteralContext context = new ImpalaSqlParser.DecimalLiteralContext(
                new ImpalaSqlParser.NumberContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;
        when(mockParsingOptions.getDecimalLiteralTreatment())
                .thenReturn(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE);

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDecimalLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitDoubleLiteral()
    {
        // Setup
        final ImpalaSqlParser.DoubleLiteralContext context = new ImpalaSqlParser.DoubleLiteralContext(
                new ImpalaSqlParser.NumberContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitDoubleLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testVisitIntegerLiteral()
    {
        // Setup
        final ImpalaSqlParser.IntegerLiteralContext context = new ImpalaSqlParser.IntegerLiteralContext(
                new ImpalaSqlParser.NumberContext(new ParserRuleContext(), 0));
        final Node expectedResult = null;

        // Run the test
        final Node result = impalaAstBuilderUnderTest.visitIntegerLiteral(context);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
