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
package io.prestosql.cli;

import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.CreateIndex;
import io.prestosql.sql.tree.Expression;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.cli.Console.extractPartitions;
import static org.testng.Assert.assertEquals;

public class TestParsingIndexCommands
{
    private final SqlParser parser = new SqlParser();

    @Test
    public void testParseIndexPartition()
    {
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t=123", "t=123");
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t='123'", "t=123");
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t='2020-01-01'", "t=2020-01-01");
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t=date'2020-01-01'", "t=2020-01-01");
    }

    @Test
    public void testParseIndexPartitionsMultiple()
    {
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t=123 OR t=1234", "t=123", "t=1234");
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t='123' OR t='1234'", "t=123", "t=1234");
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t='2020' OR t='2020-01' OR t='2020-01-01'",
                "t=2020", "t=2020-01", "t=2020-01-01");
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t=date'2020-01-01' OR t = date'2020-01-02'",
                "t=2020-01-01", "t=2020-01-02");
    }

    @Test(expectedExceptions = ParsingException.class)
    public void testParseIndexPartitionAnd()
    {
        testParseIndexPartitionsHelper("CREATE INDEX aaa USING bloom ON hive.test.test (test) WHERE t=123 AND t=1234");
    }

    private void testParseIndexPartitionsHelper(String query, String... expectedPartitions)
    {
        Expression expression = ((CreateIndex) parser.createStatement(query, new ParsingOptions())).getExpression().get();
        List<String> extracted = extractPartitions(expression);

        assertEquals(extracted.size(), expectedPartitions.length);
        for (int i = 0; i < expectedPartitions.length; i++) {
            assertEquals(extracted.get(i), expectedPartitions[i]);
        }
    }
}
