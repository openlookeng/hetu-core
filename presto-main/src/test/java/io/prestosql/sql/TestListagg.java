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
package io.prestosql.sql;

import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.query.QueryAssertions;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Objects;

import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;

public class TestListagg
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testListaggQueryWithOneValue()
    {
        assertions.assertQuery(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a') t(value)",
                "VALUES (VARCHAR 'a')");
    }

    @Test
    public void testListaggQueryWithOneValueGrouping()
    {
        assertions.assertQuery(
                "SELECT id, listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES (1, 'a')) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id ",
                "VALUES (1, VARCHAR 'a')");
    }

    @Test
    public void testListaggQueryWithMultipleValues()
    {
        assertions.assertQuery(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) ",
                "VALUES (VARCHAR 'a,bb,ccc,dddd')");
    }

    @Test
    public void testListaggQueryWithImplicitSeparator()
    {
        assertions.assertQuery(
                "SELECT listagg(value) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'b', 'c') t(value) ",
                "VALUES (VARCHAR 'abc')");
    }

    @Test
    public void testListaggQueryWithImplicitSeparatorGrouping()
    {
        assertions.assertQuery(
                "SELECT id, listagg(value) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES " +
                        "         (1, 'c'), " +
                        "         (2, 'b'), " +
                        "         (1, 'a')," +
                        "         (2, 'd')" +
                        "     )  t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id ",
                "VALUES " +
                        "        (1, VARCHAR 'ac')," +
                        "        (2, VARCHAR 'bd')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesOrderedDescending()
    {
        assertions.assertQuery(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value DESC) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) ",
                "VALUES (VARCHAR 'dddd,ccc,bb,a')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesMultipleSortItems()
    {
        assertions.assertQuery(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY sortitem1, sortitem2) " +
                        "FROM (VALUES (2, 'C', 'ccc'), (2, 'B', 'bb'), (3, 'D', 'dddd'), (1, 'A', 'a')) t(sortitem1, sortitem2, value) ",
                "VALUES (VARCHAR 'a,bb,ccc,dddd')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesMultipleSortItemsGrouping()
    {
        assertions.assertQuery(
                "SELECT id, listagg(value, ',') WITHIN GROUP (ORDER BY weight, label) " +
                        "FROM (VALUES (1, 200, 'C', 'ccc'), (1, 200, 'B', 'bb'), (2, 300, 'D', 'dddd'), (1, 100, 'A', 'a')) t(id, weight, label, value) " +
                        "GROUP BY id " +
                        "ORDER BY id ",
                "VALUES (1, VARCHAR 'a,bb,ccc')," +
                        "        (2, VARCHAR 'dddd')");
    }

    @Test
    public void testListaggQueryWithFunctionExpression()
    {
        assertions.assertQuery(
                "SELECT listagg(upper(value), ' ') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'Trino', 'SQL', 'everything') t(value) ",
                "VALUES (VARCHAR 'SQL TRINO EVERYTHING')");
    }

    @Test
    public void testListaggQueryWithNullValues()
    {
        assertions.assertQuery(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', NULL, 'bb', NULL, 'ccc', NULL, 'dddd', NULL) t(value) ",
                "VALUES (VARCHAR 'a,bb,ccc,dddd')");
    }

    @Test
    public void testListaggQueryWithNullValuesGrouping()
    {
        assertions.assertQuery(
                "SELECT id, listagg(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES " +
                        "             (1, 'a'), " +
                        "             (2, NULL), " +
                        "             (3, 'bb'), " +
                        "             (1, NULL), " +
                        "             (1, 'ccc'), " +
                        "             (2, NULL), " +
                        "             (3, 'dddd'), " +
                        "             (2, NULL)" +
                        "     ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id ",
                "VALUES (1, VARCHAR 'a,ccc')," +
                        "        (2, NULL)," +
                        "        (3, VARCHAR 'bb,dddd')");
    }

    @Test
    public void testListaggQueryIncorrectSyntax()
    {
        // missing WITHIN GROUP (ORDER BY ...)
        assertions.assertFails(
                "SELECT listagg(value, ',') " +
                        "FROM (VALUES 'a') t(value)",
                "line 1:28: mismatched input 'FROM'. Expecting: 'WITHIN'");

        // missing WITHIN GROUP (ORDER BY ...)
        assertions.assertFails(
                "SELECT listagg(value) " +
                        "FROM (VALUES 'a') t(value)",
                "line 1:23: mismatched input 'FROM'. Expecting: 'WITHIN'");

        // too many arguments
        assertions.assertFails(
                "SELECT listagg(value, ',', '...') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 'a') t(value)",
                "line 1:26: mismatched input ','. Expecting: <expression>");

        // window frames are not supported
        String query = "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) OVER (PARTITION BY id) FROM (VALUES (1, 'a')) t(id, value)";
        try {
            assertions.execute(query);
            throw new RuntimeException("Expected failure for query: " + query);
        }
        catch (ParsingException exception) {
            String expectedMessage = "line 1:63: mismatched input '('. Expecting: ',', 'EXCEPT', 'FETCH', 'FROM', 'GROUP', 'HAVING', 'INTERSECT', 'LIMIT', 'OFFSET', 'ORDER', 'UNION', 'WHERE', <EOF>";
            if (!Objects.equals(exception.getMessage(), expectedMessage)) {
                throw new RuntimeException(String.format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), expectedMessage, query));
            }
        }

        // invalid argument for ON OVERFLOW clause
        assertions.assertFails(
                "SELECT listagg(value, ',' ON OVERFLOW COLLAPSE) WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 'a') t(value)",
                "line 1:39: mismatched input 'COLLAPSE'. Expecting: 'ERROR', 'TRUNCATE'");

        // invalid separator type (integer instead of varchar)
        assertions.assertFails(
                "SELECT LISTAGG(value, 123) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'Trino', 'SQL', 'everything') t(value) ",
                "line 1:23: mismatched input '123'. Expecting: <string>");

        // invalid truncation filler type (integer instead of varchar)
        assertions.assertFails(
                "SELECT LISTAGG(value, ',' ON OVERFLOW TRUNCATE 1234567890 WITHOUT COUNT) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'Trino', 'SQL', 'everything') t(value) ",
                "line 1:48: mismatched input '1234567890'. Expecting: 'WITH', 'WITHOUT', <string>");
    }

    @Test
    public void testListaggQueryIncorrectExpression()
    {
        // integer values
        assertions.assertFails(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 1, NULL, 2, 3, 4) t(value)",
                "line 1:8: Expected expression of varchar, but 'value' has integer type");

        // boolean values
        assertions.assertFails(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES TRUE, NULL, FALSE, FALSE, TRUE) t(value)",
                "line 1:8: Expected expression of varchar, but 'value' has boolean type");

        // array values
        assertions.assertFails(
                "SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES array['abc', 'def'], array['sql']) t(value)",
                "line 1:8: Expected expression of varchar, but 'value' has array\\(varchar\\(3\\)\\) type");
    }

    @Test
    public void testListaaggQueryIncorrectOrderByExpression()
    {
        assertions.assertFails(
                "SELECT listagg(label, ',') WITHIN GROUP (ORDER BY rgb) " +
                        "FROM (VALUES ('red', rgb(255, 0, 0)), ('green', rgb(0, 128, 0)), ('blue', rgb(0, 0, 255))) color(label, rgb) ",
                "line 1:8: ORDER BY can only be applied to orderable types \\(actual: color\\)");
    }

    @Test
    public void testListaggQueryWithExplicitlyCastedNumericValues()
    {
        assertions.assertQuery(
                "SELECT listagg(try_cast(value as varchar), ',') WITHIN GROUP (ORDER BY value)" +
                        "FROM (VALUES 1, NULL, 2, 3, 4) t(value)",
                "VALUES (VARCHAR '1,2,3,4')");
    }

    @Test
    public void testListaggQueryWithDistinct()
    {
        assertions.assertQuery(
                "SELECT listagg( DISTINCT value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES  'a', 'b', 'a', 'b', 'c', 'd', 'd', 'a', 'd', 'b', 'd') t(value)",
                "VALUES (VARCHAR 'a,b,c,d')");
    }

    @Test
    public void testListaggQueryWithDistinctGrouping()
    {
        assertions.assertQuery(
                "SELECT id, listagg( DISTINCT value, ',') WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES  " +
                        "          (1, 'a'), " +
                        "          (1, 'b'), " +
                        "          (1, 'a'), " +
                        "          (2, 'b'), " +
                        "          (1, 'c'), " +
                        "          (1, 'd'), " +
                        "          (2, 'd'), " +
                        "          (1, 'a'), " +
                        "          (2, 'd'), " +
                        "          (2, 'b'), " +
                        "          (1, 'd')" +
                        "    ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id ",
                "VALUES (1, VARCHAR 'a,b,c,d')," +
                        "        (2, VARCHAR 'b,d')");
    }

    @Test
    public void testListaggQueryWithMultipleValuesWithDefaultSeparator()
    {
        assertions.assertQuery(
                "SELECT listagg(value) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES 'a', 'bb', 'ccc', 'dddd') t(value) ",
                "VALUES (VARCHAR 'abbcccdddd')");
    }

    @Test
    public void testListaggQueryWithOrderingAndGrouping()
    {
        assertions.assertQuery("SELECT id, LISTAGG(value, ',') WITHIN GROUP (ORDER BY value) " +
                        "          FROM (VALUES " +
                        "                   (1, 'a'), " +
                        "                   (1, 'b'), " +
                        "                   (2, 'd'), " +
                        "                   (2, 'c') " +
                        "               ) t(id, value)" +
                        "          GROUP BY id" +
                        "          ORDER BY id",
                "VALUES     " +
                        "     (1, VARCHAR 'a,b')," +
                        "     (2, VARCHAR 'c,d')");
    }

    @Test
    public void testListaggQueryOverflowError()
    {
        String tooLargeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        assertions.assertFails(
                "SELECT LISTAGG(value, ',' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES '" + tooLargeValue + "','Trino') t(value) ",
                "Concatenated string has the length in bytes larger than the maximum output length 1048576");
    }

    @Test
    public void testListaggQueryOverflowErrorGrouping()
    {
        String tooLargeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        assertions.assertFails(
                "SELECT id, LISTAGG(value, ',' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES " +
                        "           (1, '" + tooLargeValue + "')," +
                        "           (1, 'Trino')," +
                        "           (2, 'SQL')" +
                        "     ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id ",
                "Concatenated string has the length in bytes larger than the maximum output length 1048576");
    }

    @Test
    public void testListaggQueryOverflowTruncateWithoutCountAndWithoutOverflowFiller()
    {
        String largeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 6);
        assertions.assertQuery(
                "SELECT LISTAGG(value, ',' ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES '" + largeValue + "', 'trino', 'rocks') t(value) ",
                "VALUES (VARCHAR '" + largeValue + ",rocks,...')");
    }

    @Test
    public void testListaggQueryOverflowTruncateWithCountAndWithOverflowFiller()
    {
        String largeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 12);
        assertions.assertQuery(
                "SELECT LISTAGG(value, ',' ON OVERFLOW TRUNCATE '.....' WITH COUNT) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES '" + largeValue + "', 'trino', 'sql', 'everything') t(value) ",
                "VALUES (VARCHAR '" + largeValue + ",everything,.....(2)')");
    }

    @Test
    public void testListaggQueryGroupingOverflowTruncateWithCountAndWithOverflowFiller()
    {
        String largeValue = StringUtils.repeat("a", DEFAULT_MAX_PAGE_SIZE_IN_BYTES - 12);
        assertions.assertQuery(
                "SELECT id, LISTAGG(value, ',' ON OVERFLOW TRUNCATE '.....' WITH COUNT) WITHIN GROUP (ORDER BY value) " +
                        "FROM (VALUES " +
                        "             (1, '" + largeValue + "'), " +
                        "             (1, 'trino'), " +
                        "             (1, 'sql'), " +
                        "             (1, 'everything'), " +
                        "             (2, 'listagg'), " +
                        "             (2, 'string joiner') " +
                        "     ) t(id, value) " +
                        "GROUP BY id " +
                        "ORDER BY id ",
                "VALUES " +
                        "   (1, VARCHAR '" + largeValue + ",everything,.....(2)')," +
                        "   (2, VARCHAR 'listagg,string joiner')");
    }
}
