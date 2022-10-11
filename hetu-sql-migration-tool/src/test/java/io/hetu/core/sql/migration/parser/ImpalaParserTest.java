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
import io.prestosql.sql.parser.SqlParserOptions;
import org.antlr.v4.runtime.ParserRuleContext;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.function.Function;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ImpalaParserTest
{
    @Mock
    private SqlParserOptions mockOptions;

    private ImpalaParser impalaParserUnderTest;

    @BeforeMethod
    public void setUp()
    {
        initMocks(this);
        impalaParserUnderTest = new ImpalaParser(mockOptions);
    }

    @Test
    public void testInvokeParser()
    {
        // Setup
        final Function<ImpalaSqlParser, ParserRuleContext> parseFunction = val -> {
            return new ParserRuleContext();
        };
        final ParsingOptions parsingOptions = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE);
        final JSONObject expectedResult = new JSONObject(false, Arrays.asList(), false, false);

        // Run the test
        final JSONObject result = impalaParserUnderTest.invokeParser("sql", parseFunction, parsingOptions);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
