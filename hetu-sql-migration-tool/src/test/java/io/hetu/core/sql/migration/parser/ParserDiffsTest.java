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

import org.codehaus.jettison.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

public class ParserDiffsTest
{
    private ParserDiffs parserDiffsUnderTest;

    @BeforeMethod
    public void setUp()
    {
        parserDiffsUnderTest = new ParserDiffs(DiffType.DELETED, Optional.of("value"), Optional.of("value"),
                Optional.of("value"), Optional.of("value"), Optional.of("value"));
    }

    @Test
    public void testToJsonObject()
    {
        // Setup
        final JSONObject expectedResult = new JSONObject(false, Arrays.asList(), false, false);

        // Run the test
        final JSONObject result = parserDiffsUnderTest.toJsonObject();
    }
}
