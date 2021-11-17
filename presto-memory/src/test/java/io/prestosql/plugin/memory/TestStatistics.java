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

package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static io.prestosql.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestStatistics
        extends AbstractTestQueryFramework
{
    public TestStatistics()
    {
        super(() -> MemoryQueryRunner.createQueryRunner(1, ImmutableMap.of(), ImmutableMap.of("memory.table-statistics-enabled", "true"), true));
    }

    @Test
    public void testUsingStatistics()
    {
        assertQuerySucceeds("CREATE TABLE test_orders AS SELECT * FROM tpch.tiny.orders");
        assertQuerySucceeds("CREATE TABLE test_nation AS SELECT * FROM tpch.tiny.nation");
        assertQuerySucceeds("CREATE TABLE test_customer AS SELECT * FROM tpch.tiny.customer");
        assertQuerySucceeds("CREATE TABLE test_lineitem AS SELECT * FROM tpch.tiny.lineitem");

        String query = "select count(*) from test_customer, test_lineitem, test_nation, test_orders where test_nation.nationkey=5";

        MaterializedResult result1 = computeActual(query);

        assertQuerySucceeds("ANALYZE test_orders");
        assertQuerySucceeds("ANALYZE test_nation");
        assertQuerySucceeds("ANALYZE test_customer");
        assertQuerySucceeds("ANALYZE test_customer");

        MaterializedResult result2 = computeActual(query);

        assertEquals(result1, result2);
    }
}
